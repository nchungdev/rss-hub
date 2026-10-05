import os
import json
import html
import asyncio
import logging
from typing import Optional
from datetime import datetime, timezone
import httpx
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from scrapers.feed_manager import (
    load_feeds, 
    save_feeds, 
    get_feed_posts, 
    sanitize_slug, 
    DATA_DIR
)
from scrapers.cookie_vault import (
    load_vault,
    save_vault,
    get_vault_summary,
    get_cookie_by_id,
    get_profile_by_id,
    resolve_effective_cookie,
    resolve_effective_auth
)
from formatters.rss import generate_rss_xml
from formatters.json_feed import generate_json_feed
from formatters.atom import generate_atom_xml
from formatters.rule_engine import (
    load_rules,
    save_rules,
    get_rule_by_id,
    save_rule,
    delete_rule,
    get_rules_summary,
    run_pipeline
)
from formatters.output_processor import (
    process_posts_for_output,
    load_feed_output,
    save_feed_output,
    process_and_save_feed_output,
    reprocess_all_feeds,
    get_raw_feed_posts
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rsshub.wrapper")

app = FastAPI(title="ClaraOS RSS Hub", description="Unified Multi-source RSS/JSON/Atom Feed Wrapper")

RSSHUB_UPSTREAM = os.getenv("RSSHUB_UPSTREAM", "http://127.0.0.1:1200")
BASE_URL = os.getenv("BASE_URL", "https://rss.data1box.win")
CLARAOS_URL = os.getenv("CLARAOS_URL", "https://data1box.win")
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")

def load_settings() -> dict:
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "scrape_interval_minutes": int(os.getenv("SCRAPE_INTERVAL_MINUTES", "30"))
    }

def save_settings(settings: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)

async def background_scheduler():
    await asyncio.sleep(5)
    while True:
        try:
            now = datetime.now(timezone.utc).timestamp()
            feeds = load_feeds()
            
            for slug, meta in feeds.items():
                feed_interval = int(meta.get("interval_minutes") or 30)
                last_scraped = float(meta.get("last_scraped_at") or 0)
                if now - last_scraped >= feed_interval * 60:
                    logger.info(f"Triggering scheduled scrape for feed [{slug}] (interval: {feed_interval}m)...")
                    posts = await asyncio.to_thread(get_feed_posts, slug, True)
                    out_res = await asyncio.to_thread(process_and_save_feed_output, slug, posts, meta)
                    meta["last_scraped_at"] = now
                    meta["raw_post_count"] = len(posts)
                    meta["output_post_count"] = out_res.get("output_count", len(posts))
                    feeds[slug] = meta
                    save_feeds(feeds)
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
        await asyncio.sleep(60)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_scheduler())
    # On startup, ensure all feeds have generated Task Output files from existing RAW data
    asyncio.create_task(asyncio.to_thread(reprocess_all_feeds))

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "claraos-rss-hub"}

@app.get("/api/profiles")
def api_get_profiles():
    feeds = load_feeds()
    return get_vault_summary(feeds)

@app.get("/api/profiles/{profile_id}")
def api_get_profile_detail(profile_id: str):
    prof = get_profile_by_id(profile_id)
    if prof:
        return prof
    raise HTTPException(status_code=404, detail="Không tìm thấy profile")

@app.post("/api/profiles")
async def api_create_or_update_profile(request: Request):
    data = await request.json()
    raw_id = data.get("id") or data.get("name", "")
    profile_id = sanitize_slug(raw_id)
    if not profile_id:
        raise HTTPException(status_code=400, detail="Tên hoặc ID profile không hợp lệ")
    
    name = data.get("name", "").strip() or profile_id
    domain = data.get("domain", "").strip().lower() or data.get("website", "").strip().lower() or "generic"
    auth_type = data.get("auth_type", "cookie").strip().lower()
    cookie = data.get("cookie", "").strip()
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()
    login_url = data.get("login_url", "").strip()
    api_key = data.get("api_key", "").strip()
    header_name = data.get("header_name", "Authorization").strip() or "Authorization"
    custom_headers = data.get("custom_headers", "").strip()
    description = data.get("description", "").strip()

    vault = load_vault()
    vault[profile_id] = {
        "id": profile_id,
        "name": name,
        "domain": domain,
        "website": domain,
        "auth_type": auth_type,
        "scraper_type": "web",
        "cookie": cookie,
        "username": username,
        "password": password,
        "login_url": login_url,
        "api_key": api_key,
        "header_name": header_name,
        "custom_headers": custom_headers,
        "description": description,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    save_vault(vault)
    return {"status": "ok", "id": profile_id}

@app.delete("/api/profiles/{profile_id}")
def api_delete_profile(profile_id: str):
    vault = load_vault()
    if profile_id in vault:
        del vault[profile_id]
        save_vault(vault)
        return {"status": "ok", "id": profile_id}
    raise HTTPException(status_code=404, detail="Không tìm thấy profile này")

# Cookie aliases for backward compatibility
@app.get("/api/cookies")
def api_get_cookies():
    return api_get_profiles()

@app.get("/api/cookies/{profile_id}")
def api_get_cookie_detail(profile_id: str):
    return api_get_profile_detail(profile_id)

@app.post("/api/cookies")
async def api_create_or_update_cookie(request: Request):
    return await api_create_or_update_profile(request)

@app.delete("/api/cookies/{profile_id}")
def api_delete_cookie(profile_id: str):
    return api_delete_profile(profile_id)

# ==========================================
# OUTPUT RULE ENGINE API ENDPOINTS
# ==========================================

@app.get("/api/rules")
def api_get_rules():
    feeds = load_feeds()
    return get_rules_summary(feeds)

@app.get("/api/rules/{rule_id}")
def api_get_rule_detail(rule_id: str):
    r = get_rule_by_id(rule_id)
    if r:
        return r
    raise HTTPException(status_code=404, detail="Không tìm thấy rule này")

@app.post("/api/rules")
async def api_create_or_update_rule(request: Request):
    data = await request.json()
    rule_id = save_rule(data)
    # Automatically reprocess all feeds with updated rules
    asyncio.create_task(asyncio.to_thread(reprocess_all_feeds))
    return {"status": "ok", "id": rule_id}

@app.delete("/api/rules/{rule_id}")
def api_delete_rule(rule_id: str):
    success = delete_rule(rule_id)
    if success:
        # Automatically reprocess all feeds after rule deletion
        asyncio.create_task(asyncio.to_thread(reprocess_all_feeds))
        return {"status": "ok", "id": rule_id}
    raise HTTPException(status_code=404, detail="Không tìm thấy rule này")

@app.get("/api/feeds")
def api_get_feeds():
    return load_feeds()

@app.post("/api/feeds")
async def api_create_or_update_feed(request: Request):
    data = await request.json()
    raw_slug = data.get("slug", "")
    slug = sanitize_slug(raw_slug)
    if not slug:
        raise HTTPException(status_code=400, detail="Định danh slug không hợp lệ (chỉ chấp nhận chữ cái, số, gạch ngang)")
    
    title = data.get("title", "").strip() or slug
    feed_type = data.get("type", "web")
    target = data.get("target", "").strip() or slug
    category = data.get("category", "Chung").strip() or "Chung"
    description = data.get("description", "").strip()
    
    cookie_mode = data.get("cookie_mode", "none")
    cookie_profile = data.get("cookie_profile", "").strip()
    cookie = data.get("cookie", "").strip()
    save_to_vault = bool(data.get("save_to_vault", False))
    vault_profile_name = data.get("vault_profile_name", "").strip()
    vault_website = data.get("vault_website", "").strip().lower() or "generic"

    # Save to cookie vault if requested
    if save_to_vault and cookie and vault_profile_name:
        vault = load_vault()
        p_id = sanitize_slug(vault_profile_name) or f"cookie_{int(datetime.now().timestamp())}"
        vault[p_id] = {
            "id": p_id,
            "name": vault_profile_name,
            "website": vault_website,
            "scraper_type": feed_type,
            "cookie": cookie,
            "description": f"Lưu từ kênh {title}",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        save_vault(vault)
        cookie_mode = "profile"
        cookie_profile = p_id

    use_flaresolverr = bool(data.get("use_flaresolverr", False))
    selectors = data.get("selectors", {})
    custom_output = data.get("custom_output") or {}

    original_slug = sanitize_slug(data.get("original_slug", ""))
    feeds = load_feeds()

    created_at = None
    if original_slug and original_slug in feeds:
        created_at = feeds[original_slug].get("created_at")
    elif slug in feeds:
        created_at = feeds[slug].get("created_at")
    if not created_at:
        created_at = datetime.now(timezone.utc).isoformat()

    # If slug was renamed, delete old key and move history cache & output files
    if original_slug and original_slug != slug:
        if original_slug in feeds:
            del feeds[original_slug]
            old_cache = os.path.join(DATA_DIR, f"history_{original_slug}.json")
            new_cache = os.path.join(DATA_DIR, f"history_{slug}.json")
            if os.path.exists(old_cache):
                try:
                    os.rename(old_cache, new_cache)
                except Exception:
                    pass
            old_out = os.path.join(DATA_DIR, f"output_{original_slug}.json")
            new_out = os.path.join(DATA_DIR, f"output_{slug}.json")
            if os.path.exists(old_out):
                try:
                    os.rename(old_out, new_out)
                except Exception:
                    pass

    feeds[slug] = {
        "slug": slug,
        "title": title,
        "type": feed_type,
        "target": target,
        "category": category,
        "description": description,
        "cookie_mode": cookie_mode,
        "cookie_profile": cookie_profile,
        "cookie": cookie,
        "use_flaresolverr": use_flaresolverr,
        "selectors": selectors,
        "custom_output": custom_output,
        "applied_rules": data.get("applied_rules") or [],
        "custom_rules": data.get("custom_rules") or [],
        "interval_minutes": int(data.get("interval_minutes", 30) or 30),
        "last_scraped_at": feeds.get(slug, {}).get("last_scraped_at") or (feeds.get(original_slug, {}).get("last_scraped_at") if original_slug else 0) or 0,
        "created_at": created_at,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    save_feeds(feeds)
    
    # Trigger initial scrape and auto-process output in background
    async def initial_scrape_and_process():
        try:
            p = await asyncio.to_thread(get_feed_posts, slug, True)
            await asyncio.to_thread(process_and_save_feed_output, slug, p, feeds.get(slug, {}))
        except Exception as e:
            logger.warning(f"Initial scrape/process failed for {slug}: {e}")

    asyncio.create_task(initial_scrape_and_process())
    return {"status": "ok", "slug": slug}

@app.get("/api/feeds/{slug}")
def api_get_feed_detail(slug: str):
    clean_slug = sanitize_slug(slug)
    feeds = load_feeds()
    if clean_slug in feeds:
        return feeds[clean_slug]
    raise HTTPException(status_code=404, detail="Kênh feed không tồn tại")

@app.post("/api/feeds/{slug}/interval")
async def api_update_feed_interval(slug: str, request: Request):
    clean_slug = sanitize_slug(slug.lstrip("#"))
    feeds = load_feeds()
    if clean_slug not in feeds:
        raise HTTPException(status_code=404, detail="Kênh feed không tồn tại")
    data = await request.json()
    try:
        val = int(data.get("interval_minutes", 30))
    except (ValueError, TypeError):
        val = 30
    if val < 1 or val > 1440:
        raise HTTPException(status_code=400, detail="Tần suất phải từ 1 đến 1440 phút")
    feeds[clean_slug]["interval_minutes"] = val
    save_feeds(feeds)
    return {"status": "ok", "slug": clean_slug, "interval_minutes": val}

@app.delete("/api/feeds/{slug}")
def api_delete_feed(slug: str):
    clean_slug = sanitize_slug(slug)
    feeds = load_feeds()
    if clean_slug in feeds:
        del feeds[clean_slug]
        save_feeds(feeds)
        cache_file = os.path.join(DATA_DIR, f"history_{clean_slug}.json")
        if os.path.exists(cache_file):
            try:
                os.remove(cache_file)
            except Exception:
                pass
        out_file = os.path.join(DATA_DIR, f"output_{clean_slug}.json")
        if os.path.exists(out_file):
            try:
                os.remove(out_file)
            except Exception:
                pass
        return {"status": "ok", "slug": clean_slug}
    raise HTTPException(status_code=404, detail="Kênh feed không tồn tại")

@app.get("/api/settings")
def api_get_settings():
    return load_settings()

@app.post("/api/settings")
async def api_update_settings(request: Request):
    data = await request.json()
    try:
        val = int(data.get("scrape_interval_minutes", 30))
    except (ValueError, TypeError):
        val = 30
    if val < 1 or val > 1440:
        raise HTTPException(status_code=400, detail="Tần suất phải từ 1 đến 1440 phút")
    settings = load_settings()
    settings["scrape_interval_minutes"] = val
    save_settings(settings)
    return {"status": "ok", "settings": settings}

@app.get("/api/refresh/{tag}")
async def refresh_feed(tag: str):
    clean_tag = sanitize_slug(tag.lstrip("#"))
    now = datetime.now(timezone.utc).timestamp()
    posts = await asyncio.to_thread(get_feed_posts, clean_tag, True)
    feeds = load_feeds()
    meta = feeds.get(clean_tag, {})
    out_res = await asyncio.to_thread(process_and_save_feed_output, clean_tag, posts, meta)
    if clean_tag in feeds:
        feeds[clean_tag]["last_scraped_at"] = now
        feeds[clean_tag]["last_status"] = "success" if posts else "empty"
        feeds[clean_tag]["last_post_count"] = len(posts)
        feeds[clean_tag]["total_posts"] = len(posts)
        feeds[clean_tag]["raw_post_count"] = len(posts)
        feeds[clean_tag]["output_post_count"] = out_res.get("output_count", len(posts))
        save_feeds(feeds)
    return {
        "status": "ok",
        "tag": clean_tag,
        "count": len(posts),
        "raw_count": len(posts),
        "output_count": out_res.get("output_count", len(posts)),
        "filtered_count": out_res.get("filtered_count", 0)
    }

@app.post("/api/feeds/{slug}/reprocess")
async def api_reprocess_feed(slug: str):
    clean_slug = sanitize_slug(slug.lstrip("#"))
    feeds = load_feeds()
    if clean_slug not in feeds:
        raise HTTPException(status_code=404, detail="Kênh feed không tồn tại")
    res = await asyncio.to_thread(process_and_save_feed_output, clean_slug)
    return {
        "status": "ok",
        "slug": clean_slug,
        "raw_count": res.get("raw_count", 0),
        "output_count": res.get("output_count", 0),
        "filtered_count": res.get("filtered_count", 0),
        "updated_at": res.get("updated_at")
    }

@app.post("/api/reprocess-all")
async def api_reprocess_all():
    results = await asyncio.to_thread(reprocess_all_feeds)
    return {"status": "ok", "feeds": results}

@app.get("/api/feeds/{slug}/output")
async def api_get_feed_output(slug: str):
    clean_slug = sanitize_slug(slug.lstrip("#"))
    feeds = load_feeds()
    if clean_slug not in feeds:
        raise HTTPException(status_code=404, detail="Kênh feed không tồn tại")
    meta = feeds[clean_slug]
    raw_posts = await asyncio.to_thread(get_raw_feed_posts, clean_slug)
    if not raw_posts:
        raw_posts = await asyncio.to_thread(get_feed_posts, clean_slug, False)
    if not isinstance(raw_posts, list):
        raw_posts = list(raw_posts.values()) if isinstance(raw_posts, dict) else []

    output_data = await asyncio.to_thread(load_feed_output, clean_slug)
    if not output_data or not output_data.get("posts"):
        output_data = await asyncio.to_thread(process_and_save_feed_output, clean_slug, raw_posts, meta)

    return {
        "status": "ok",
        "slug": clean_slug,
        "title": meta.get("title", clean_slug),
        "category": meta.get("category", "Chung"),
        "target": meta.get("target", clean_slug),
        "type": meta.get("type", "web"),
        "raw_count": len(raw_posts),
        "output_count": output_data.get("output_count", len(output_data.get("posts", []))),
        "filtered_count": max(0, len(raw_posts) - len(output_data.get("posts", []))),
        "rules_applied": output_data.get("rules_applied", []),
        "updated_at": output_data.get("updated_at"),
        "raw_posts": raw_posts,
        "output_posts": output_data.get("posts", [])
    }

@app.get("/api/dashboard/posts")
async def api_get_dashboard_posts(slug: str = "all"):
    feeds = load_feeds()
    clean_slug = sanitize_slug(slug.lstrip("#")) if slug != "all" else "all"
    target_slugs = [clean_slug] if clean_slug != "all" and clean_slug in feeds else list(feeds.keys())

    all_output_posts = []
    all_raw_posts = []

    for s in target_slugs:
        meta = feeds.get(s, {})
        title = meta.get("title", s)
        cat = meta.get("category", "Chung")
        feed_type = meta.get("type", "web")

        raw = await asyncio.to_thread(get_raw_feed_posts, s)
        if not raw:
            raw = await asyncio.to_thread(get_feed_posts, s, False)
        if not isinstance(raw, list):
            raw = list(raw.values()) if isinstance(raw, dict) else []

        for r in raw:
            r_copy = dict(r)
            r_copy["_feed_slug"] = s
            r_copy["_feed_title"] = title
            r_copy["_feed_category"] = cat
            r_copy["_feed_type"] = feed_type
            all_raw_posts.append(r_copy)

        out_data = await asyncio.to_thread(load_feed_output, s)
        if not out_data or not out_data.get("posts"):
            out_data = await asyncio.to_thread(process_and_save_feed_output, s, raw, meta)
        posts_out = out_data.get("posts", []) if out_data else []
        for p in posts_out:
            p_copy = dict(p)
            p_copy["_feed_slug"] = s
            p_copy["_feed_title"] = title
            p_copy["_feed_category"] = cat
            p_copy["_feed_type"] = feed_type
            p_copy["_applied_rules"] = out_data.get("rules_applied", [])
            all_output_posts.append(p_copy)

    return {
        "status": "ok",
        "slug": clean_slug,
        "feeds_summary": [{"slug": s, "title": feeds.get(s, {}).get("title", s), "category": feeds.get(s, {}).get("category", "Chung")} for s in feeds.keys()],
        "output_count": len(all_output_posts),
        "raw_count": len(all_raw_posts),
        "output_posts": all_output_posts,
        "raw_posts": all_raw_posts
    }

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    feeds = load_feeds()
    settings = load_settings()
    global_interval = int(settings.get("scrape_interval_minutes", 30))
    feed_cards = ""
    total_posts = 0
    total_raw_posts = 0
    total_output_posts = 0
    categories = set()

    for slug, meta in feeds.items():
        cat = meta.get("category", "Chung")
        categories.add(cat)
        posts = await asyncio.to_thread(get_feed_posts, slug, False)
        if not isinstance(posts, list):
            posts = list(posts.values()) if isinstance(posts, dict) else []
        count = len(posts)
        total_posts += count
        
        feed_type = meta.get("type", "web")
        type_badge = "WEB SCRAPER"
        icon_svg = '<circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>'
        if feed_type == "web":
            type_badge = "WEB SCRAPER"
            icon_svg = '<circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>'
        elif feed_type == "threads":
            type_badge = "THREADS"
            icon_svg = '<path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/>'
        elif feed_type == "rsshub":
            type_badge = "RSSHUB"
            icon_svg = '<path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/>'
        elif feed_type == "custom_rss":
            type_badge = "EXTERNAL RSS"
            icon_svg = '<path d="M4 11a9 9 0 0 1 9 9"/><path d="M4 4a16 16 0 0 1 16 16"/><circle cx="5" cy="19" r="1"/>'

        # Cookie Mode Badge
        cookie_mode = meta.get("cookie_mode", "none")
        cookie_profile = meta.get("cookie_profile", "")
        cookie_val = meta.get("cookie", "")
        
        cookie_badge = ""
        if cookie_mode == "profile" and cookie_profile:
            cookie_badge = f'<span class="badge green" style="padding: 2px 7px; font-size: 0.65rem;" title="Dùng Cookie theo trang web [{cookie_profile}]"><span class="dot" style="background:#10b981;"></span> Cookie: {cookie_profile}</span>'
        elif cookie_mode == "custom" and cookie_val:
            cookie_badge = '<span class="badge" style="padding: 2px 7px; font-size: 0.65rem; background:rgba(245, 158, 11, 0.12); color:#fbbf24; border-color:rgba(245, 158, 11, 0.3);" title="Dùng Cookie riêng biệt của scraper này"><span class="dot" style="background:#f59e0b;"></span> Custom Cookie</span>'
        else:
            cookie_badge = '<span class="badge" style="padding: 2px 7px; font-size: 0.65rem; opacity:0.65;" title="Chế độ Guest / Không dùng cookie">Guest</span>'

        # Per-Feed Interval Badge (Interactive on each card)
        feed_interval = int(meta.get("interval_minutes") or 30)
        safe_title = meta.get("title", slug).replace("'", "\\'").replace('"', '&quot;')
        interval_badge = f'''<button type="button" class="badge" onclick="openFeedIntervalModal('{slug}', '{safe_title}', {feed_interval})" style="cursor: pointer; padding: 2px 7px; font-size: 0.65rem; background:rgba(168, 85, 247, 0.14); color:#c084fc; border:1px solid rgba(168, 85, 247, 0.35); border-radius: 5px; display: inline-flex; align-items: center; gap: 4px; transition: all 0.15s ease;" title="Tần suất cào riêng của kênh này: {feed_interval} phút (Bấm để đổi nhanh)"><span class="dot" style="background:#a855f7;"></span> ⏱️ {feed_interval}m <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 10px; height: 10px; opacity: 0.7;"><path d="M12 20h9M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"/></svg></button>'''

        custom_out = meta.get("custom_output") or {}
        custom_badges = []
        if custom_out.get("filter_include"):
            inc_preview = custom_out["filter_include"].split(",")[0][:10]
            custom_badges.append(f'<span class="badge" style="padding: 2px 6px; font-size: 0.65rem; background: rgba(56, 189, 248, 0.1); color: #38bdf8; border-color: rgba(56, 189, 248, 0.25);" title="Bộ lọc Include: {custom_out["filter_include"]}">🔍 +{inc_preview}</span>')
        if custom_out.get("filter_exclude"):
            exc_preview = custom_out["filter_exclude"].split(",")[0][:10]
            custom_badges.append(f'<span class="badge" style="padding: 2px 6px; font-size: 0.65rem; background: rgba(244, 63, 94, 0.1); color: #fb7185; border-color: rgba(244, 63, 94, 0.25);" title="Bộ lọc Exclude: {custom_out["filter_exclude"]}">🚫 -{exc_preview}</span>')
        if custom_out.get("title_template"):
            custom_badges.append('<span class="badge" style="padding: 2px 6px; font-size: 0.65rem; opacity: 0.8;" title="Mẫu tiêu đề tùy chỉnh">🏷️ Template</span>')
        custom_badges_html = " ".join(custom_badges)

        out_data = await asyncio.to_thread(load_feed_output, slug)
        if not out_data or not out_data.get("posts"):
            out_data = await asyncio.to_thread(process_and_save_feed_output, slug, posts, meta)
        out_posts = out_data.get("posts", [])
        out_count = len(out_posts)
        filtered_count = max(0, count - out_count)
        total_raw_posts += count
        total_output_posts += out_count

        # Recent items preview (sleek single row per article from task output)
        preview_items_html = ""
        preview_src = out_posts[:3] if out_posts else posts[:3]
        for p in preview_src:
            user = p.get("_formatted_creator") or p.get("username", "web")
            first_line = p.get("_formatted_title") or p.get("preview_title") or (p.get("text", "").split("\n")[0][:110] if p.get("text") else "Bài viết không có tiêu đề")
            gdrive = p.get("gdrive_links", [])
            badge = '<span class="badge cyan" style="padding: 2px 7px; font-size: 0.65rem;"><span class="dot"></span> Ebook Drive</span>' if gdrive else ""
            item_url = p.get("url", "#")
            
            preview_items_html += f"""
            <div style="background: rgba(15, 23, 42, 0.65); border: 1px solid rgba(255, 255, 255, 0.06); border-radius: 8px; padding: 6px 10px; display: flex; align-items: center; justify-content: space-between; gap: 8px;">
                <div style="min-width: 0; flex: 1;">
                    <a href="{item_url}" target="_blank" rel="noopener noreferrer" style="font-weight: 600; font-size: 0.75rem; color: #38bdf8; text-decoration: none; display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">🌐 {first_line}</a>
                    <div style="color: var(--text-dim); font-size: 0.68rem; margin-top: 1px;">Nguồn / Tác giả: {user}</div>
                </div>
                {badge}
            </div>
            """

        feed_cards += f"""
        <div class="task-card feed-item" data-category="{cat}" data-slug="{slug}" id="feedCard_{slug}">
            <div style="display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap;">
                <!-- Left: Feed Icon & Meta Info -->
                <div style="display: flex; align-items: center; gap: 12px; min-width: 260px; flex: 1;">
                    <div class="brand-icon" style="width: 36px; height: 36px; min-width: 36px; border-radius: 10px; background: linear-gradient(135deg, rgba(14, 165, 233, 0.2), rgba(99, 102, 241, 0.2)); border: 1px solid rgba(14, 165, 233, 0.3);">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 18px; height: 18px; color: #38bdf8;">
                            {icon_svg}
                        </svg>
                    </div>
                    <div style="min-width: 0;">
                        <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
                            <span class="task-title" style="font-size: 0.96rem; font-weight: 700; color: #ffffff;">{meta['title']}</span>
                            <span style="font-size: 0.65rem; font-weight: 700; padding: 2px 6px; border-radius: 5px; background: rgba(14, 165, 233, 0.12); color: #38bdf8; border: 1px solid rgba(14, 165, 233, 0.3); text-transform: uppercase;">{cat}</span>
                            <span style="font-size: 0.65rem; font-weight: 700; padding: 2px 6px; border-radius: 5px; background: rgba(255,255,255,0.06); color: var(--text-dim); border: 1px solid var(--card-border);">{type_badge}</span>
                            {cookie_badge}
                            {interval_badge}
                            {custom_badges_html}
                        </div>
                        <div style="display: flex; align-items: center; gap: 8px; margin-top: 3px; font-size: 0.73rem; color: var(--text-dim); flex-wrap: wrap;">
                            <span style="font-family: var(--mono); color: var(--text-muted); max-width: 320px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="{meta.get('target', slug)}">🎯 {meta.get('target', slug)}</span>
                            <span>•</span>
                            <span style="color: #34d399; font-weight: 600;">{out_count} output / {count} raw bài</span>
                            {f'<span>•</span><span style="max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text-dim);" title="{meta.get("description", "")}">{meta.get("description", "")}</span>' if meta.get("description") else ''}
                        </div>
                    </div>
                </div>

                <!-- Right: Format Endpoints & Scraper Actions -->
                <div style="display: flex; align-items: center; gap: 6px; flex-wrap: wrap;">
                    <!-- Format Buttons Group (XML, JSON, ATOM) -->
                    <div class="format-btn-group" style="margin: 0; gap: 4px;">
                        <a href="{BASE_URL}/{slug}.xml" target="_blank" rel="noopener noreferrer" class="format-btn xml" style="height: 30px; padding: 0 9px; font-size: 0.72rem;" title="Mở RSS 2.0 (XML): {BASE_URL}/{slug}.xml">
                            <span>XML</span>
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 11px; height: 11px; opacity: 0.7;"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                        </a>
                        <a href="{BASE_URL}/{slug}.json" target="_blank" rel="noopener noreferrer" class="format-btn json" style="height: 30px; padding: 0 9px; font-size: 0.72rem;" title="Mở JSON Feed: {BASE_URL}/{slug}.json">
                            <span>JSON</span>
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 11px; height: 11px; opacity: 0.7;"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                        </a>
                        <a href="{BASE_URL}/{slug}.atom" target="_blank" rel="noopener noreferrer" class="format-btn atom" style="height: 30px; padding: 0 9px; font-size: 0.72rem;" title="Mở Atom Feed: {BASE_URL}/{slug}.atom">
                            <span>ATOM</span>
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 11px; height: 11px; opacity: 0.7;"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                        </a>
                    </div>

                    <div style="width: 1px; height: 18px; background: var(--card-border); margin: 0 2px;"></div>

                    <!-- Open Output Preview & Compare Modal -->
                    <button class="btn" onclick="openCompareModal('{slug}')" id="btnPreview_{slug}" title="Xem và so sánh Task Output sau Rule & RAW Data" style="height: 30px; padding: 0 9px; font-size: 0.74rem; color: #38bdf8; border-color: rgba(56,189,248,0.3);">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                        <span>Xem Output</span>
                    </button>

                    <!-- Reprocess Rules -->
                    <button class="btn" onclick="reprocessFeed('{slug}', this)" title="Chạy lại bộ Rules trên RAW Data đã cào" style="height: 30px; padding: 0 8px; font-size: 0.74rem; color: #a855f7; border-color: rgba(168,85,247,0.3);">
                        <span>⚡ Rule</span>
                    </button>

                    <!-- Edit Scraper Config -->
                    <button class="btn" onclick="editFeedModal('{slug}')" title="Cấu hình Scraper, Tần suất & Cookie" style="height: 30px; padding: 0 9px; font-size: 0.74rem;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><path d="M12 20h9M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"/></svg>
                        <span>Cấu hình</span>
                    </button>

                    <!-- Refresh / Scrape Now -->
                    <button class="btn primary" onclick="refreshFeed('{slug}', this)" title="Cào mới dữ liệu ngay" style="height: 30px; padding: 0 10px; font-size: 0.74rem;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
                        <span>Cào</span>
                    </button>

                    <!-- Delete -->
                    <button class="btn danger" onclick="deleteFeed('{slug}')" title="Xóa kênh Feed này" style="height: 30px; padding: 0 8px;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                    </button>
                </div>
            </div>

            <!-- Collapsible Preview Drawer -->
            <div id="drawerPreview_{slug}" style="display: none; margin-top: 6px; padding-top: 8px; border-top: 1px solid rgba(255, 255, 255, 0.06);">
                <div style="font-size: 0.68rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-dim); margin-bottom: 6px; display: flex; align-items: center; gap: 6px;">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 12px; height: 12px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                    <span>Bài viết output sau khi qua bộ rule</span>
                </div>
                <div style="display: grid; grid-template-columns: 1fr; gap: 6px;">
                    {preview_items_html if preview_items_html else '<div style="color:var(--text-dim); font-size:0.75rem; font-style:italic; padding:4px 0;">Chưa có dữ liệu bài viết (bấm nút Cào để tải).</div>'}
                </div>
            </div>
        </div>
        """

    # Category pills
    cat_pills = f'<button class="pill active" onclick="filterFeeds(\'all\', this)"><span>Tất cả Feeds</span><span class="pill-count">{len(feeds)}</span></button>'
    for c in sorted(categories):
        count_c = sum(1 for m in feeds.values() if m.get("category", "Chung") == c)
        cat_pills += f'<button class="pill" onclick="filterFeeds(\'{c}\', this)"><span>{c}</span><span class="pill-count">{count_c}</span></button>'

    # Time & Profiles calculation
    now = datetime.now(timezone.utc).timestamp()
    profiles = get_vault_summary(feeds)

    # 1. Generate Dashboard Table Rows
    dashboard_table_rows = ""
    for slug, meta in feeds.items():
        title = meta.get("title", slug)
        cat = meta.get("category", "Chung")
        target = meta.get("target", slug)
        feed_type = meta.get("type", "web")
        interval = int(meta.get("interval_minutes") or 30)
        last_scraped = float(meta.get("last_scraped_at") or 0)
        
        posts_for_feed = await asyncio.to_thread(get_feed_posts, slug, False)
        if not isinstance(posts_for_feed, list):
            posts_for_feed = list(posts_for_feed.values()) if isinstance(posts_for_feed, dict) else []
        p_count = len(posts_for_feed)

        out_data = await asyncio.to_thread(load_feed_output, slug)
        if not out_data or not out_data.get("posts"):
            out_data = await asyncio.to_thread(process_and_save_feed_output, slug, posts_for_feed, meta)
        out_count = len(out_data.get("posts", [])) if out_data else p_count
        filtered_count = max(0, p_count - out_count)

        # Status
        if last_scraped == 0 and p_count == 0:
            status_html = '<div style="font-weight:600; font-size:0.78rem; color:var(--text-dim); display:flex; align-items:center; gap:5px;"><span class="dot" style="background:#64748b;"></span> Chưa cào</div>'
            sub_status = '<div style="font-size:0.68rem; color:var(--text-dim); margin-top:2px;">Sẵn sàng cào</div>'
        else:
            diff = max(0, int(now - last_scraped))
            if diff < 60: time_ago = "Vừa xong"
            elif diff < 3600: time_ago = f"{diff // 60}m trước"
            elif diff < 86400: time_ago = f"{diff // 3600}h trước"
            else: time_ago = f"{diff // 86400}d trước"

            if p_count > 0:
                filter_sub = f' <span style="color:#fbbf24; font-size:0.68rem; font-weight:700;" title="Đã lọc loại bỏ {filtered_count} bài">(-{filtered_count})</span>' if filtered_count > 0 else ""
                status_html = f'<div style="font-weight:600; font-size:0.78rem; color:#10b981; display:flex; align-items:center; gap:5px;"><span class="dot" style="background:#10b981;"></span> {out_count}/{p_count} bài{filter_sub}</div>'
            else:
                status_html = '<div style="font-weight:600; font-size:0.78rem; color:#f59e0b; display:flex; align-items:center; gap:5px;"><span class="dot" style="background:#f59e0b;"></span> 0 bài</div>'
            
            sub_status = f'<div style="font-size:0.68rem; color:var(--text-dim); margin-top:2px;">Cào {time_ago} • <button type="button" onclick="openFeedIntervalModal(\'{slug}\', \'{safe_title}\', {interval})" style="background:none; border:none; padding:0; color:#c084fc; cursor:pointer; text-decoration:underline;" title="Bấm để sửa chu kỳ">⏱️ {interval}m</button></div>'

        # Engine label
        if feed_type == "web":
            eng_label = '<span style="color:#38bdf8; font-size:0.72rem; display:inline-flex; align-items:center; gap:4px; font-weight:600;">🌐 Web</span>'
        elif feed_type == "threads":
            eng_label = '<span style="color:#c084fc; font-size:0.72rem; display:inline-flex; align-items:center; gap:4px; font-weight:600;">🧵 Threads</span>'
        elif feed_type == "rsshub":
            eng_label = '<span style="color:#22d3ee; font-size:0.72rem; display:inline-flex; align-items:center; gap:4px; font-weight:600;">🚀 RSSHub</span>'
        else:
            eng_label = '<span style="color:#fbbf24; font-size:0.72rem; display:inline-flex; align-items:center; gap:4px; font-weight:600;">📡 RSS</span>'

        # Profile label
        c_mode = meta.get("cookie_mode", "none")
        c_prof = meta.get("cookie_profile", "")
        if c_mode == "profile" and c_prof:
            prof_label = f'<span style="font-size:0.68rem; color:#38bdf8; font-family:var(--mono);" title="Profile: {c_prof}">📂 {c_prof}</span>'
        elif c_mode == "custom":
            prof_label = '<span style="font-size:0.68rem; color:#fbbf24;">Custom Cookie</span>'
        else:
            prof_label = '<span style="font-size:0.68rem; color:var(--text-dim);">Guest</span>'

        safe_title = title.replace("'", "\\'").replace('"', '&quot;')
        truncated_target = target if len(target) <= 42 else target[:39] + "..."

        dashboard_table_rows += f"""
        <tr class="dashboard-row" data-slug="{slug}" data-category="{cat}">
            <td style="vertical-align: middle;">
                <div style="font-weight: 600; color: #f8fafc; font-size: 0.86rem; display: flex; align-items: center; gap: 6px; flex-wrap: wrap;">
                    <a href="javascript:void(0)" onclick="viewFeedPostsOnDashboard('{slug}')" style="color: #f8fafc; text-decoration: none;" title="Xem các bài viết của kênh này dưới Dashboard">{title}</a>
                    <span class="badge gray" style="font-size: 0.62rem; padding: 1px 6px;">{cat}</span>
                </div>
                <div style="display: flex; align-items: center; gap: 8px; margin-top: 4px; font-size: 0.7rem; color: var(--text-dim); flex-wrap: wrap;">
                    <span style="font-family: var(--mono); color: var(--text-muted);">/{slug}</span>
                    <span>•</span>
                    <span style="max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="{target}">{truncated_target}</span>
                    <span>•</span>
                    <span style="display: inline-flex; gap: 4px;">
                        <a href="{BASE_URL}/{slug}.xml" target="_blank" style="color: #f59e0b; text-decoration: none; font-size: 0.65rem; font-family: var(--mono); font-weight:600;">XML</a>
                        <span style="color: rgba(255,255,255,0.2);">|</span>
                        <a href="{BASE_URL}/{slug}.json" target="_blank" style="color: #38bdf8; text-decoration: none; font-size: 0.65rem; font-family: var(--mono); font-weight:600;">JSON</a>
                        <span style="color: rgba(255,255,255,0.2);">|</span>
                        <a href="{BASE_URL}/{slug}.atom" target="_blank" style="color: #a855f7; text-decoration: none; font-size: 0.65rem; font-family: var(--mono); font-weight:600;">ATOM</a>
                    </span>
                </div>
            </td>
            <td style="vertical-align: middle;">
                <div>{eng_label}</div>
                <div style="margin-top: 3px;">{prof_label}</div>
            </td>
            <td style="vertical-align: middle;">
                {status_html}
                {sub_status}
            </td>
            <td style="text-align: right; vertical-align: middle;">
                <div style="display: inline-flex; align-items: center; gap: 5px; justify-content: flex-end;">
                    <button class="btn primary" onclick="refreshFeed('{slug}', this)" title="Cào mới dữ liệu ngay" style="height: 27px; padding: 0 8px; font-size: 0.72rem;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 12px; height: 12px;"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
                        <span>Cào</span>
                    </button>
                    <button class="btn" onclick="viewFeedPostsOnDashboard('{slug}')" title="Xem danh sách bài viết dưới Dashboard" style="height: 27px; padding: 0 8px; font-size: 0.72rem; color: #10b981; border-color: rgba(16,185,129,0.3);">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 12px; height: 12px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                        <span>Xem</span>
                    </button>
                    <button class="btn" onclick="openCompareModal('{slug}')" id="btnDashPreview_{slug}" title="So sánh chi tiết Task Output vs RAW Data gốc" style="height: 27px; padding: 0 7px; font-size: 0.72rem; color: #38bdf8; border-color: rgba(56, 189, 248, 0.3);">
                        <span>So sánh</span>
                    </button>
                    <button class="btn" onclick="reprocessFeed('{slug}', this)" title="Chạy lại Rules trên RAW data không cần cào lại web" style="height: 27px; padding: 0 7px; font-size: 0.72rem; color: #a855f7; border-color: rgba(168, 85, 247, 0.3);">
                        <span>⚡</span>
                    </button>
                    <button class="btn" onclick="editFeedModal('{slug}')" title="Cấu hình scraper này" style="height: 27px; padding: 0 7px; font-size: 0.72rem;">
                        <span>⚙️</span>
                    </button>
                </div>
            </td>
        </tr>
        """

    # 2. Domain groups for Profiles Tab
    domain_groups = {}
    for p in profiles:
        dom = p.get("domain", "generic")
        domain_groups.setdefault(dom, []).append(p)

    domain_pills = f'<button class="pill active" onclick="filterProfiles(\'all\', this)"><span>Tất cả Domain</span><span class="pill-count">{len(profiles)}</span></button>'
    for dom in sorted(domain_groups.keys()):
        p_list = domain_groups[dom]
        domain_pills += f'<button class="pill" onclick="filterProfiles(\'{dom}\', this)"><span>{dom}</span><span class="pill-count">{len(p_list)}</span></button>'

    profiles_html = ""
    if not profiles:
        profiles_html = '''<div style="text-align: center; padding: 48px 20px; background: var(--card-bg); border: 1px dashed var(--card-border); border-radius: 16px;">
            <div style="font-size: 2.2rem; margin-bottom: 10px;">🔐</div>
            <div style="font-weight: 700; font-size: 1rem; color: #fff; margin-bottom: 6px;">Chưa có Profile xác thực nào</div>
            <div style="font-size: 0.78rem; color: var(--text-dim); max-width: 440px; margin: 0 auto 16px;">Tạo profile theo domain để quản lý cookie, tài khoản login hoặc API key dùng chung cho các scraper.</div>
            <button class="btn primary" onclick="openAddProfileModal()">+ Thêm Profile Đầu Tiên</button>
        </div>'''
    else:
        for dom in sorted(domain_groups.keys()):
            p_list = domain_groups[dom]
            profiles_html += f"""
            <div class="domain-group-section" data-domain="{dom}" style="margin-bottom: 24px;">
                <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; padding-bottom: 6px; border-bottom: 1px solid rgba(255,255,255,0.06);">
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <span style="font-size: 0.88rem; font-weight: 700; color: #38bdf8;">🌐 {dom}</span>
                        <span class="badge gray" style="font-size: 0.68rem;">{len(p_list)} profile</span>
                    </div>
                    <button class="btn" onclick="openAddProfileForDomain('{dom}')" style="height: 26px; padding: 0 8px; font-size: 0.7rem;">+ Thêm vào domain này</button>
                </div>
                <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 12px;">
            """
            for p in p_list:
                p_id = p["id"]
                p_name = p["name"]
                auth_type = p.get("auth_type", "cookie")
                if auth_type == "cookie":
                    auth_badge = '<span class="badge amber" style="font-size: 0.68rem;">🍪 Session Cookie</span>'
                elif auth_type == "login":
                    auth_badge = '<span class="badge blue" style="font-size: 0.68rem;">👤 Login (User/Pass)</span>'
                elif auth_type == "api_key":
                    auth_badge = '<span class="badge green" style="font-size: 0.68rem;">🔑 API Key</span>'
                else:
                    auth_badge = '<span class="badge gray" style="font-size: 0.68rem;">⚙️ Custom Headers</span>'

                used_feeds = p.get("used_by", [])
                if used_feeds:
                    used_html = '<span style="color:#34d399; font-size:0.7rem;">Áp dụng: <strong>' + ", ".join(used_feeds) + '</strong></span>'
                else:
                    used_html = '<span style="color:var(--text-dim); font-size:0.7rem;">Chưa gắn scraper nào</span>'

                desc_html = f'<div style="font-size: 0.72rem; color: var(--text-dim); margin-top: 6px; line-height: 1.4;">{p.get("description")}</div>' if p.get("description") else ""

                profiles_html += f"""
                    <div class="task-card profile-item" data-domain="{dom}" style="padding: 14px; margin: 0; display: flex; flex-direction: column; justify-content: space-between;">
                        <div>
                            <div style="display: flex; align-items: flex-start; justify-content: space-between; gap: 8px; margin-bottom: 8px;">
                                <div>
                                    <div style="font-weight: 700; font-size: 0.88rem; color: #fff;">{p_name}</div>
                                    <div style="font-size: 0.68rem; color: var(--text-dim); font-family: var(--mono);">{p_id}</div>
                                </div>
                                {auth_badge}
                            </div>
                            <div style="background: rgba(0,0,0,0.3); border: 1px solid rgba(255,255,255,0.06); border-radius: 8px; padding: 6px 10px; font-family: var(--mono); font-size: 0.72rem; color: #cbd5e1; word-break: break-all; margin-bottom: 8px;">
                                <code>{p["masked"]}</code>
                            </div>
                            <div style="margin-bottom: 6px;">
                                {used_html}
                            </div>
                            {desc_html}
                        </div>
                        <div style="display: flex; align-items: center; justify-content: flex-end; gap: 6px; margin-top: 12px; padding-top: 8px; border-top: 1px solid rgba(255,255,255,0.06);">
                            <button class="btn" onclick="copyProfileCredential('{p_id}')" title="Sao chép thông tin xác thực" style="height: 28px; padding: 0 8px; font-size: 0.72rem;">
                                <span>📋 Copy</span>
                            </button>
                            <button class="btn" onclick="openEditProfileModal('{p_id}')" title="Sửa profile này" style="height: 28px; padding: 0 8px; font-size: 0.72rem;">
                                <span>Sửa</span>
                            </button>
                            <button class="btn danger" onclick="deleteProfileModal('{p_id}')" title="Xóa profile này" style="height: 28px; padding: 0 8px; font-size: 0.72rem;">
                                <span>Xóa</span>
                            </button>
                        </div>
                    </div>
                """
            profiles_html += '</div></div>'

    # Rules calculation
    rules_list = get_rules_summary(feeds)

    count_replace = sum(1 for r in rules_list if r.get("rule_type") == "replace")
    count_filter = sum(1 for r in rules_list if r.get("rule_type") == "filter")
    count_format = sum(1 for r in rules_list if r.get("rule_type") == "format")
    count_extract = sum(1 for r in rules_list if r.get("rule_type") == "extract_links")

    rule_pills = f'<button class="pill active" onclick="filterRules(\'all\', this)"><span>Tất cả Rule</span><span class="pill-count">{len(rules_list)}</span></button>'
    rule_pills += f'<button class="pill" onclick="filterRules(\'replace\', this)"><span>🔄 Thay thế</span><span class="pill-count">{count_replace}</span></button>'
    rule_pills += f'<button class="pill" onclick="filterRules(\'filter\', this)"><span>⛔ Lọc bài</span><span class="pill-count">{count_filter}</span></button>'
    rule_pills += f'<button class="pill" onclick="filterRules(\'format\', this)"><span>✨ Làm sạch</span><span class="pill-count">{count_format}</span></button>'
    rule_pills += f'<button class="pill" onclick="filterRules(\'extract_links\', this)"><span>🔗 Bóc link</span><span class="pill-count">{count_extract}</span></button>'

    rules_html = ""
    if not rules_list:
        rules_html = '''<div style="text-align: center; padding: 48px 20px; background: var(--card-bg); border: 1px dashed var(--card-border); border-radius: 16px;">
            <div style="font-size: 2.2rem; margin-bottom: 10px;">⚡</div>
            <div style="font-weight: 700; font-size: 1rem; color: #fff; margin-bottom: 6px;">Chưa có Rule xử lý output nào</div>
            <div style="font-size: 0.78rem; color: var(--text-dim); max-width: 440px; margin: 0 auto 16px;">Tạo rule để tự động xóa quảng cáo, lọc bài viết theo regex, làm sạch HTML hoặc bóc tách link Google Drive.</div>
            <button class="btn primary" onclick="openAddRuleModal()">+ Thêm Rule Đầu Tiên</button>
        </div>'''
    else:
        rules_html = '<div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); gap: 14px;">'
        for r in rules_list:
            r_id = r.get("id", "")
            r_name = r.get("name", r_id)
            r_type = r.get("rule_type", "replace")
            r_desc = r.get("description", "")
            r_builtin = r.get("builtin", False)
            used_by = r.get("used_by", [])

            if r_type == "replace":
                type_badge = '<span class="badge blue" style="font-size:0.68rem;">🔄 Thay thế</span>'
                tgt = r.get("target_field", "both")
                tgt_label = "Tiêu đề & Nội dung" if tgt == "both" else ("Tiêu đề" if tgt == "title" else "Nội dung")
                is_rx = "Regex" if r.get("is_regex") else "Text"
                pat_preview = r.get("pattern", "")
                if len(pat_preview) > 55: pat_preview = pat_preview[:52] + "..."
                preview_box = f'''<div style="background:rgba(0,0,0,0.3); border:1px solid rgba(255,255,255,0.06); border-radius:8px; padding:8px 10px; font-family:var(--mono); font-size:0.72rem; color:#cbd5e1; word-break:break-all;">
                    <div style="color:#38bdf8;">🔍 Tìm ({is_rx} - {tgt_label}): <code>{html.escape(pat_preview)}</code></div>
                    <div style="color:#34d399; margin-top:2px;">✏️ Đổi thành: <code>"{html.escape(r.get('replacement', ''))}"</code></div>
                </div>'''
            elif r_type == "filter":
                type_badge = '<span class="badge red" style="background:rgba(239,68,68,0.15); color:#f87171; border:1px solid rgba(239,68,68,0.25); font-size:0.68rem;">⛔ Lọc bài</span>'
                cond = "Bỏ qua nếu khớp (Exclude)" if r.get("condition") == "exclude" else "Chỉ giữ nếu khớp (Include)"
                pat_preview = r.get("pattern", "")
                if len(pat_preview) > 55: pat_preview = pat_preview[:52] + "..."
                preview_box = f'''<div style="background:rgba(0,0,0,0.3); border:1px solid rgba(255,255,255,0.06); border-radius:8px; padding:8px 10px; font-family:var(--mono); font-size:0.72rem; color:#cbd5e1; word-break:break-all;">
                    <div style="color:#f87171;">⚠️ {cond}</div>
                    <div style="color:#cbd5e1; margin-top:2px;">Mẫu: <code>{html.escape(pat_preview)}</code></div>
                </div>'''
            elif r_type == "format":
                type_badge = '<span class="badge green" style="background:rgba(16,185,129,0.15); color:#34d399; border:1px solid rgba(16,185,129,0.25); font-size:0.68rem;">✨ Làm sạch</span>'
                clean_txt = "Làm sạch HTML rác & tracking" if r.get("clean_html") else "Định dạng text"
                preview_box = f'''<div style="background:rgba(0,0,0,0.3); border:1px solid rgba(255,255,255,0.06); border-radius:8px; padding:8px 10px; font-family:var(--mono); font-size:0.72rem; color:#cbd5e1; word-break:break-all;">
                    <div style="color:#34d399;">🛡️ {clean_txt}</div>
                    <div style="color:#94a3b8; margin-top:2px;">Xóa tags: <code>{html.escape(r.get("strip_tags", "script,style,iframe"))}</code></div>
                </div>'''
            else:
                type_badge = '<span class="badge purple" style="background:rgba(168,85,247,0.15); color:#c084fc; border:1px solid rgba(168,85,247,0.25); font-size:0.68rem;">🔗 Bóc link</span>'
                links_str = ", ".join(r.get("link_types") or ["gdrive"])
                tag_str = f'| Tag: {r.get("auto_tag")}' if r.get("auto_tag") else ''
                preview_box = f'''<div style="background:rgba(0,0,0,0.3); border:1px solid rgba(255,255,255,0.06); border-radius:8px; padding:8px 10px; font-family:var(--mono); font-size:0.72rem; color:#cbd5e1; word-break:break-all;">
                    <div style="color:#c084fc;">📥 Quét link: {links_str}</div>
                    <div style="color:#94a3b8; margin-top:2px;">Action: Hộp tải Enclosure {tag_str}</div>
                </div>'''

            builtin_badge = '<span class="badge gray" style="font-size:0.65rem;">Mặc định</span>' if r_builtin else '<span class="badge cyan" style="font-size:0.65rem;">Tùy chỉnh</span>'

            if used_by:
                used_html = '<span style="color:#34d399; font-size:0.7rem;">Áp dụng cho: <strong>' + ", ".join(used_by) + '</strong></span>'
            else:
                used_html = '<span style="color:var(--text-dim); font-size:0.7rem;">Chưa gắn scraper nào</span>'

            desc_html = f'<div style="font-size: 0.72rem; color: var(--text-dim); margin-top: 6px; line-height: 1.4;">{html.escape(r_desc)}</div>' if r_desc else ''

            del_btn = f'<button class="btn danger" onclick="deleteRuleModal(\'{r_id}\')" title="Xóa rule này" style="height: 28px; padding: 0 8px; font-size: 0.72rem;"><span>Xóa</span></button>' if not r_builtin else '<button class="btn" disabled title="Rule mặc định của hệ thống" style="height: 28px; padding: 0 8px; font-size: 0.72rem; opacity:0.4; cursor:not-allowed;"><span>Khóa</span></button>'

            rules_html += f"""
            <div class="task-card rule-item" data-type="{r_type}" style="padding: 14px; margin: 0; display: flex; flex-direction: column; justify-content: space-between;">
                <div>
                    <div style="display: flex; align-items: flex-start; justify-content: space-between; gap: 8px; margin-bottom: 8px;">
                        <div>
                            <div style="font-weight: 700; font-size: 0.88rem; color: #fff;">{r_name}</div>
                            <div style="font-size: 0.68rem; color: var(--text-dim); font-family: var(--mono);">{r_id}</div>
                        </div>
                        <div style="display: flex; align-items: center; gap: 4px;">
                            {builtin_badge}
                            {type_badge}
                        </div>
                    </div>
                    {preview_box}
                    <div style="margin-top: 8px;">
                        {used_html}
                    </div>
                    {desc_html}
                </div>
                <div style="display: flex; align-items: center; justify-content: flex-end; gap: 6px; margin-top: 12px; padding-top: 8px; border-top: 1px solid rgba(255,255,255,0.06);">
                    <button class="btn" onclick="duplicateRule('{r_id}')" title="Nhân bản rule này" style="height: 28px; padding: 0 8px; font-size: 0.72rem;">
                        <span>📋 Nhân bản</span>
                    </button>
                    <button class="btn" onclick="openEditRuleModal('{r_id}')" title="Sửa rule này" style="height: 28px; padding: 0 8px; font-size: 0.72rem;">
                        <span>Sửa</span>
                    </button>
                    {del_btn}
                </div>
            </div>
            """
        rules_html += '</div>'

    html_content = f"""<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>RSS Hub | ClaraOS Feed Engine</title>
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Cdefs%3E%3ClinearGradient id='g' x1='0%25' y1='0%25' x2='100%25' y2='100%25'%3E%3Cstop offset='0%25' stop-color='%230284c7'/%3E%3Cstop offset='100%25' stop-color='%2338bdf8'/%3E%3C/linearGradient%3E%3C/defs%3E%3Crect width='32' height='32' rx='8' fill='url(%23g)'/%3E%3Ccircle cx='8' cy='24' r='3' fill='%23ffffff'/%3E%3Cpath d='M8 14a10 10 0 0 1 10 10h-3a7 7 0 0 0-7-7v-3z' fill='%23ffffff'/%3E%3Cpath d='M8 8a16 16 0 0 1 16 16h-3A13 13 0 0 0 8 11V8z' fill='%23ffffff'/%3E%3C/svg%3E">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg: #070c18;
      --card-bg: rgba(15, 23, 42, 0.75);
      --card-border: rgba(255, 255, 255, 0.08);
      --card-hover: rgba(56, 189, 248, 0.2);
      --primary: #0ea5e9;
      --primary-glow: rgba(14, 165, 233, 0.35);
      --emerald: #10b981;
      --emerald-glow: rgba(16, 185, 129, 0.3);
      --amber: #f59e0b;
      --rose: #f43f5e;
      --violet: #8b5cf6;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --text-dim: #64748b;
      --sidebar-w: 260px;
      --font: "Plus Jakarta Sans", system-ui, -apple-system, sans-serif;
      --mono: "JetBrains Mono", monospace;
    }}

    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: var(--font);
      background-color: var(--bg);
      color: var(--text);
      min-height: 100vh;
      line-height: 1.5;
      background-image: 
        radial-gradient(circle at 10% 15%, rgba(14, 165, 233, 0.12), transparent 40%),
        radial-gradient(circle at 90% 20%, rgba(139, 92, 246, 0.1), transparent 45%),
        radial-gradient(circle at 50% 95%, rgba(16, 185, 129, 0.08), transparent 50%);
      background-attachment: fixed;
    }}

    ::-webkit-scrollbar {{ width: 8px; height: 8px; }}
    ::-webkit-scrollbar-track {{ background: rgba(0,0,0,0.2); }}
    ::-webkit-scrollbar-thumb {{ background: rgba(255,255,255,0.15); border-radius: 4px; }}
    ::-webkit-scrollbar-thumb:hover {{ background: rgba(255,255,255,0.3); }}

    /* Sidebar */
    .torbox-sidebar {{
      width: var(--sidebar-w);
      min-width: var(--sidebar-w);
      background: rgba(15, 23, 42, 0.95);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      border-right: 1px solid var(--card-border);
      display: flex;
      flex-direction: column;
      z-index: 100;
      transition: transform 0.25s cubic-bezier(0.16, 1, 0.3, 1);
      flex-shrink: 0;
      height: 100vh;
    }}
    .sidebar-brand {{
      height: 64px;
      padding: 0 18px;
      display: flex;
      align-items: center;
      gap: 12px;
      border-bottom: 1px solid var(--card-border);
    }}
    .brand-icon {{
      width: 42px;
      height: 42px;
      border-radius: 12px;
      background: linear-gradient(135deg, #0ea5e9, #6366f1);
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 4px 16px var(--primary-glow);
      flex-shrink: 0;
    }}
    .brand-icon svg {{ width: 22px; height: 22px; fill: white; }}
    .brand-title {{
      font-size: 1.12rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      background: linear-gradient(to right, #ffffff, #cbd5e1);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }}
    .brand-sub {{
      font-size: 0.72rem;
      color: var(--text-dim);
      font-weight: 500;
    }}

    .sidebar-nav {{
      flex: 1;
      padding: 16px 12px;
      display: flex;
      flex-direction: column;
      gap: 4px;
      overflow-y: auto;
    }}
    .nav-section-title {{
      font-size: 0.65rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      color: var(--text-dim);
      padding: 6px 10px;
      text-transform: uppercase;
    }}
    .nav-item {{
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 10px 14px;
      border-radius: 12px;
      color: var(--text-muted);
      font-size: 0.82rem;
      font-weight: 500;
      text-decoration: none;
      transition: all 0.15s ease;
      cursor: pointer;
      background: transparent;
      border: none;
      text-align: left;
      width: 100%;
    }}
    .nav-item:hover {{
      background: rgba(255, 255, 255, 0.06);
      color: #fff;
    }}
    .nav-item.active {{
      background: rgba(14, 165, 233, 0.15);
      color: #38bdf8;
      font-weight: 600;
      border-left: 3px solid #0ea5e9;
    }}
    .nav-item svg {{ width: 18px; height: 18px; flex-shrink: 0; }}

    .sidebar-telemetry {{
      padding: 14px;
      border-top: 1px solid var(--card-border);
      display: flex;
      flex-direction: row;
      gap: 8px;
    }}

    /* Viewport & Header */
    .torbox-main-viewport {{
      flex: 1;
      display: flex;
      flex-direction: column;
      min-width: 0;
      overflow-y: auto;
      height: 100vh;
    }}
    .topbar-header {{
      height: 64px;
      background: rgba(7, 12, 24, 0.85);
      backdrop-filter: blur(16px);
      border-bottom: 1px solid var(--card-border);
      padding: 0 32px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      position: sticky;
      top: 0;
      z-index: 40;
      flex-shrink: 0;
    }}
    .page-title {{
      font-size: 1.15rem;
      font-weight: 700;
      letter-spacing: -0.01em;
      color: #fff;
      margin: 0;
      white-space: nowrap;
    }}

    main {{
      flex: 1;
      width: 100%;
      padding: 18px 32px 32px 32px;
      box-sizing: border-box;
    }}

    /* Badges */
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 5px 12px;
      border-radius: 9999px;
      font-size: 0.75rem;
      font-weight: 600;
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--card-border);
      color: var(--text-muted);
      white-space: nowrap;
    }}
    .badge.green {{
      background: rgba(16, 185, 129, 0.12);
      border-color: rgba(16, 185, 129, 0.3);
      color: #34d399;
    }}
    .badge.cyan {{
      background: rgba(14, 165, 233, 0.12);
      border-color: rgba(14, 165, 233, 0.3);
      color: #38bdf8;
    }}
    .dot {{
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--text-dim);
    }}
    .badge.green .dot {{ background: #10b981; box-shadow: 0 0 8px #10b981; }}
    .badge.cyan .dot {{ background: #0ea5e9; box-shadow: 0 0 8px #0ea5e9; }}

    /* Buttons */
    .btn {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      height: 36px;
      padding: 0 14px;
      border-radius: 10px;
      font-size: 0.8rem;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s ease;
      border: 1px solid var(--card-border);
      background: rgba(255, 255, 255, 0.06);
      color: var(--text);
      font-family: var(--font);
      text-decoration: none;
      box-sizing: border-box;
      white-space: nowrap;
    }}
    .btn:hover {{
      background: rgba(255, 255, 255, 0.12);
      border-color: rgba(255, 255, 255, 0.2);
      transform: translateY(-1px);
    }}
    .btn.primary {{
      background: linear-gradient(135deg, #0284c7, #0369a1);
      border-color: rgba(14, 165, 233, 0.4);
      color: white;
      box-shadow: 0 4px 14px var(--primary-glow);
    }}
    .btn.primary:hover {{
      background: linear-gradient(135deg, #38bdf8, #0ea5e9);
      box-shadow: 0 6px 20px rgba(14, 165, 233, 0.45);
    }}
    .btn.danger {{
      background: rgba(244, 63, 94, 0.12);
      border-color: rgba(244, 63, 94, 0.3);
      color: #fb7185;
    }}
    .btn.danger:hover {{
      background: rgba(244, 63, 94, 0.25);
      border-color: rgba(244, 63, 94, 0.6);
      color: #fff;
    }}

    /* Format Endpoint Buttons (XML, JSON, ATOM) */
    .format-btn-group {{
      display: flex;
      align-items: center;
      gap: 8px;
      margin: 8px 0 12px 0;
    }}
    .format-btn {{
      flex: 1;
      min-width: 0;
      height: 36px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 7px;
      padding: 0 10px;
      border-radius: 10px;
      font-size: 0.78rem;
      font-weight: 700;
      font-family: var(--mono);
      text-decoration: none;
      cursor: pointer;
      transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1);
      box-sizing: border-box;
      white-space: nowrap;
    }}
    .format-btn:hover {{
      transform: translateY(-1.5px);
    }}
    .format-btn.xml {{
      background: rgba(245, 158, 11, 0.1);
      border: 1px solid rgba(245, 158, 11, 0.3);
      color: #fbbf24;
    }}
    .format-btn.xml:hover {{
      background: rgba(245, 158, 11, 0.22);
      border-color: rgba(245, 158, 11, 0.6);
      box-shadow: 0 4px 14px rgba(245, 158, 11, 0.25);
      color: #fef3c7;
    }}
    .format-btn.json {{
      background: rgba(14, 165, 233, 0.1);
      border: 1px solid rgba(14, 165, 233, 0.3);
      color: #38bdf8;
    }}
    .format-btn.json:hover {{
      background: rgba(14, 165, 233, 0.22);
      border-color: rgba(14, 165, 233, 0.6);
      box-shadow: 0 4px 14px rgba(14, 165, 233, 0.25);
      color: #e0f2fe;
    }}
    .format-btn.atom {{
      background: rgba(139, 92, 246, 0.1);
      border: 1px solid rgba(139, 92, 246, 0.3);
      color: #c084fc;
    }}
    .format-btn.atom:hover {{
      background: rgba(139, 92, 246, 0.22);
      border-color: rgba(139, 92, 246, 0.6);
      box-shadow: 0 4px 14px rgba(139, 92, 246, 0.25);
      color: #f3e8ff;
    }}

    /* Stats Grid */
    .stats-grid {{
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 16px;
      margin-bottom: 24px;
    }}
    .stat-card {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 16px;
      padding: 18px;
      backdrop-filter: blur(12px);
      display: flex;
      align-items: center;
      gap: 14px;
      transition: all 0.2s ease;
    }}
    .stat-card:hover {{
      border-color: var(--card-hover);
      transform: translateY(-2px);
    }}
    .stat-card-icon {{
      width: 48px;
      height: 48px;
      border-radius: 12px;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
    }}
    .stat-card-icon svg {{ width: 24px; height: 24px; }}
    .stat-blue {{ background: rgba(14, 165, 233, 0.15); color: #38bdf8; }}
    .stat-emerald {{ background: rgba(16, 185, 129, 0.15); color: #34d399; }}
    .stat-violet {{ background: rgba(139, 92, 246, 0.15); color: #c084fc; }}
    .stat-amber {{ background: rgba(245, 158, 11, 0.15); color: #fbbf24; }}

    .stat-info .label {{
      font-size: 0.75rem;
      font-weight: 600;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }}
    .stat-info .val {{
      font-size: 1.5rem;
      font-weight: 800;
      color: var(--text);
      line-height: 1.2;
      margin-top: 2px;
    }}
    .stat-info .desc {{
      font-size: 0.72rem;
      color: var(--text-dim);
    }}

    /* Controls Bar & Filter Pills */
    .controls-bar {{
      background: rgba(15, 23, 42, 0.65);
      border: 1px solid var(--card-border);
      border-radius: 16px;
      padding: 10px 16px;
      margin-bottom: 20px;
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      min-height: 56px;
    }}
    .filter-pills {{
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .pill {{
      height: 36px;
      padding: 0 14px;
      border-radius: 10px;
      font-size: 0.8rem;
      font-weight: 600;
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid transparent;
      color: var(--text-muted, #94a3b8);
      cursor: pointer;
      transition: all 0.2s ease;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      white-space: nowrap;
    }}
    .pill:hover {{
      background: rgba(255, 255, 255, 0.08);
      color: var(--text, #f8fafc);
    }}
    .pill.active {{
      background: rgba(14, 165, 233, 0.15);
      color: #38bdf8;
      border: 1px solid rgba(14, 165, 233, 0.4);
      box-shadow: 0 0 12px rgba(14, 165, 233, 0.15);
    }}
    .pill-count {{
      background: rgba(255, 255, 255, 0.08);
      padding: 1px 7px;
      border-radius: 9999px;
      font-size: 0.72rem;
      font-family: var(--mono);
      color: inherit;
    }}
    .pill.active .pill-count {{
      background: rgba(14, 165, 233, 0.25);
      color: #38bdf8;
    }}

    /* Button Toggle Group */
    .btn-toggle {{
      background: transparent;
      border: none;
      color: var(--text-dim);
      transition: all 0.15s ease;
      outline: none;
    }}
    .btn-toggle:hover {{
      color: #fff;
      background: rgba(255, 255, 255, 0.06);
    }}
    .btn-toggle.active {{
      background: rgba(255, 255, 255, 0.12) !important;
      color: #fff !important;
      font-weight: 600;
      box-shadow: 0 1px 3px rgba(0,0,0,0.3);
    }}
    @media (max-width: 992px) {{
      #dashPostsGrid {{
        grid-template-columns: 1fr !important;
      }}
    }}

    /* Task / Feed Cards */
    .task-list {{
      display: flex;
      flex-direction: column;
      gap: 10px;
    }}
    .task-card {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 14px;
      padding: 12px 18px;
      backdrop-filter: blur(12px);
      display: flex;
      flex-direction: column;
      gap: 6px;
      transition: all 0.2s ease;
    }}
    .task-card:hover {{
      border-color: rgba(255, 255, 255, 0.16);
      transform: translateY(-1px);
    }}
    .task-card-head {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
    }}
    .task-title {{
      font-size: 1.1rem;
      font-weight: 700;
      color: #ffffff;
    }}
    .task-card-footer {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .task-actions-row {{
      display: flex;
      align-items: center;
      gap: 8px;
    }}

    /* Modal Form */
    .modal-overlay {{
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.75);
      backdrop-filter: blur(8px);
      z-index: 100;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      opacity: 0;
      pointer-events: none;
      transition: opacity 0.2s ease;
    }}
    .modal-overlay.open {{ opacity: 1; pointer-events: auto; }}
    .modal-card {{
      background: #0f172a;
      border: 1px solid var(--card-border);
      border-radius: 20px;
      width: min(620px, 100%);
      max-height: 90vh;
      overflow-y: auto;
      padding: 26px;
      box-shadow: 0 20px 50px rgba(0,0,0,0.6);
    }}
    .modal-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 20px;
      padding-bottom: 12px;
      border-bottom: 1px solid var(--card-border);
    }}
    .modal-title {{ font-size: 1.18rem; font-weight: 700; color: white; }}
    .modal-close {{
      background: transparent;
      border: none;
      color: var(--text-muted);
      cursor: pointer;
      font-size: 1.3rem;
      padding: 4px;
    }}
    .form-group {{ margin-bottom: 16px; }}
    .form-label {{
      display: block;
      font-size: 0.8rem;
      font-weight: 600;
      color: var(--text-muted);
      margin-bottom: 6px;
    }}
    .form-input, .form-select, .form-textarea {{
      width: 100%;
      padding: 10px 14px;
      border-radius: 10px;
      background: rgba(7, 12, 24, 0.85);
      border: 1px solid var(--card-border);
      color: #fff;
      font-family: var(--font);
      font-size: 0.85rem;
      outline: none;
      transition: border-color 0.2s ease;
      box-sizing: border-box;
    }}
    .form-input:focus, .form-select:focus, .form-textarea:focus {{
      border-color: #0ea5e9;
      box-shadow: 0 0 10px rgba(14, 165, 233, 0.25);
    }}
    .form-hint {{
      font-size: 0.72rem;
      color: var(--text-dim);
      margin-top: 5px;
      line-height: 1.4;
    }}

    /* Toast Notification */
    .toast {{
      position: fixed;
      bottom: 24px;
      right: 24px;
      z-index: 200;
      background: #1e293b;
      border: 1px solid var(--card-border);
      color: var(--text);
      padding: 12px 20px;
      border-radius: 12px;
      font-size: 0.85rem;
      font-weight: 500;
      box-shadow: 0 10px 30px rgba(0,0,0,0.5);
      transform: translateY(100px);
      opacity: 0;
      transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
      display: flex;
      align-items: center;
      gap: 10px;
    }}
    .toast.show {{ transform: translateY(0); opacity: 1; }}
    .toast.error {{ border-color: rgba(244, 63, 94, 0.5); color: #fda4af; }}

    /* Mobile Responsive */
    .hamburger-btn {{
      display: none;
      background: rgba(255, 255, 255, 0.08);
      border: 1px solid var(--card-border);
      color: #fff;
      font-size: 1.1rem;
      width: 36px;
      height: 36px;
      border-radius: 10px;
      cursor: pointer;
      align-items: center;
      justify-content: center;
    }}
    .mobile-close-btn {{
      display: none;
      margin-left: auto;
      background: transparent;
      border: none;
      color: var(--text-dim);
      font-size: 1.2rem;
      cursor: pointer;
      padding: 4px;
    }}

    @media (max-width: 992px) {{
      .stats-grid {{ grid-template-columns: repeat(2, 1fr); }}
    }}
    @media (max-width: 768px) {{
      :root {{
        --sidebar-w: 0px;
      }}
      .torbox-sidebar {{
        position: fixed;
        inset: 0 auto 0 0;
        transform: translateX(-100%);
      }}
      .torbox-sidebar.open {{
        transform: translateX(0);
      }}
      .hamburger-btn {{
        display: inline-flex;
      }}
      .mobile-close-btn {{
        display: block;
      }}
      .topbar-header {{
        padding: 0 16px;
      }}
      main {{
        padding: 16px 12px 24px;
      }}
      .stats-grid {{
        grid-template-columns: 1fr !important;
        gap: 10px !important;
      }}
      .preset-interval-btn {{
        padding: 9px 12px;
        font-size: 0.8rem;
        font-weight: 600;
        border-radius: 8px;
        border: 1px solid var(--card-border);
        background: rgba(255, 255, 255, 0.04);
        color: var(--text-muted);
        cursor: pointer;
        transition: all 0.15s ease;
        text-align: center;
        display: flex;
        align-items: center;
        justify-content: center;
      }}
      .preset-interval-btn:hover {{
        border-color: #38bdf8;
        color: #fff;
        background: rgba(14, 165, 233, 0.12);
      }}
      .stat-card.clickable {{
        cursor: pointer;
        transition: transform 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease;
      }}
      .stat-card.clickable:hover {{
        transform: translateY(-2px);
        border-color: rgba(168, 85, 247, 0.5);
        box-shadow: 0 8px 24px rgba(168, 85, 247, 0.15);
      }}
    }}

    /* Main Tabs Styling */
    .main-tab-content {{
      animation: tabFadeIn 0.2s cubic-bezier(0.16, 1, 0.3, 1);
    }}
    @keyframes tabFadeIn {{
      from {{ opacity: 0; transform: translateY(5px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}

    /* Table Styles */
    .table-card {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 16px;
      overflow: hidden;
      backdrop-filter: blur(12px);
    }}
    .table-responsive {{
      overflow-x: auto;
      width: 100%;
    }}
    .data-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.8rem;
      text-align: left;
    }}
    .data-table th {{
      padding: 12px 16px;
      background: rgba(255, 255, 255, 0.03);
      border-bottom: 1px solid var(--card-border);
      color: var(--text-dim);
      font-size: 0.72rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      white-space: nowrap;
    }}
    .data-table td {{
      padding: 12px 16px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
      color: var(--text);
      vertical-align: middle;
    }}
    .data-table tr:hover td {{
      background: rgba(255, 255, 255, 0.02);
    }}
    .data-table tr:last-child td {{
      border-bottom: none;
    }}

    /* Quick KPI Bar */
    .kpi-strip {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(185px, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }}
    .kpi-box {{
      background: rgba(15, 23, 42, 0.65);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 10px 14px;
      display: flex;
      align-items: center;
      gap: 10px;
    }}
    .kpi-icon {{
      width: 34px;
      height: 34px;
      border-radius: 9px;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
    }}
    .kpi-icon svg {{ width: 17px; height: 17px; }}
    .kpi-info .kpi-label {{ font-size: 0.68rem; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.04em; }}
    .kpi-info .kpi-val {{ font-size: 1.1rem; font-weight: 700; color: #fff; line-height: 1.2; }}

    @media (max-width: 900px) {{
      .kpi-strip {{ grid-template-columns: repeat(2, 1fr); }}
    }}
    @media (max-width: 600px) {{
      .kpi-strip {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body style="display: flex; flex-direction: row; min-height: 100vh; overflow: hidden; background-color: var(--bg);">

  <!-- Mobile Overlay Backdrop -->
  <div id="sidebarBackdrop" onclick="toggleSidebar(false)" style="display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.7); backdrop-filter: blur(4px); z-index: 90;"></div>

  <!-- CLARAOS SUITE SIDEBAR -->
  <aside id="appSidebar" class="torbox-sidebar">
    <div class="sidebar-brand">
      <div class="brand-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="5" cy="19" r="1.5" fill="currentColor"/>
          <path d="M4 4a16 16 0 0 1 16 16"/>
          <path d="M4 11a9 9 0 0 1 9 9"/>
        </svg>
      </div>
      <div style="min-width: 0;">
        <div class="brand-title">RSS Hub</div>
        <div class="brand-sub">ClaraOS Feed Engine</div>
      </div>
      <button class="mobile-close-btn" onclick="toggleSidebar(false)">✕</button>
    </div>

    <!-- Navigation items (TABS) -->
    <nav class="sidebar-nav">
      <div class="nav-section-title">HỆ THỐNG</div>
      <button class="nav-item active" id="navItem_dashboard" onclick="switchMainTab('dashboard')">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect width="7" height="9" x="3" y="3" rx="1"/><rect width="7" height="5" x="14" y="3" rx="1"/><rect width="7" height="9" x="14" y="12" rx="1"/><rect width="7" height="5" x="3" y="16" rx="1"/></svg>
        <span>Dashboard</span>
        <span class="badge blue" style="margin-left: auto; font-size: 0.65rem; padding: 1px 6px;">Live</span>
      </button>

      <button class="nav-item" id="navItem_scrapers" onclick="switchMainTab('scrapers')">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="5" cy="19" r="1.5"/><path d="M4 4a16 16 0 0 1 16 16"/><path d="M4 11a9 9 0 0 1 9 9"/></svg>
        <span>Quản lý Scraper</span>
        <span class="badge gray" style="margin-left: auto; font-size: 0.65rem; padding: 1px 6px;">{len(feeds)}</span>
      </button>

      <button class="nav-item" id="navItem_profiles" onclick="switchMainTab('profiles')">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
        <span>Quản lý Profile</span>
        <span class="badge violet" style="margin-left: auto; font-size: 0.65rem; padding: 1px 6px;">{len(profiles)}</span>
      </button>

      <button class="nav-item" id="navItem_rules" onclick="switchMainTab('rules')">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
        <span>Quản lý Rule</span>
        <span class="badge cyan" style="margin-left: auto; font-size: 0.65rem; padding: 1px 6px;">{len(rules_list)}</span>
      </button>
    </nav>

    <!-- Sidebar Telemetry Badges -->
    <div class="sidebar-telemetry">
      <div class="badge green" style="flex: 1; justify-content: center;" title="Gateway: Online">
        <span class="dot"></span>
        <span>Gateway</span>
      </div>
      <div class="badge cyan" style="flex: 1; justify-content: center;" title="Port: 8098">
        <span class="dot"></span>
        <span>Port 8098</span>
      </div>
    </div>
  </aside>

  <!-- MAIN SCROLLABLE VIEWPORT -->
  <div class="torbox-main-viewport">
    <header class="topbar-header">
      <div style="display: flex; align-items: center; gap: 12px; min-width: 0; flex-wrap: wrap;">
        <button class="hamburger-btn" onclick="toggleSidebar(true)">☰</button>
        <h1 class="page-title" id="mainPageTitle" style="margin: 0; white-space: nowrap;">Dashboard &amp; Giám sát Lượt cào</h1>
        <div style="display: flex; align-items: center; gap: 6px;">
          <span style="background: rgba(14, 165, 233, 0.12); color: #38bdf8; border: 1px solid rgba(14, 165, 233, 0.25); border-radius: 999px; font-size: 0.72rem; font-weight: 600; padding: 2px 8px; display: inline-flex; align-items: center; gap: 4px;" title="Tổng số kênh Feed">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 11px; height: 11px;"><circle cx="5" cy="19" r="1.5"/><path d="M4 4a16 16 0 0 1 16 16"/><path d="M4 11a9 9 0 0 1 9 9"/></svg>
            <span>{len(feeds)} Scrapers</span>
          </span>
          <span style="background: rgba(16, 185, 129, 0.12); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.25); border-radius: 999px; font-size: 0.72rem; font-weight: 600; padding: 2px 8px; display: inline-flex; align-items: center; gap: 4px;" title="Tổng số bài viết đã cào">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 11px; height: 11px;"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg>
            <span>{total_posts} Bài</span>
          </span>
          <span style="background: rgba(16, 185, 129, 0.1); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.25); border-radius: 999px; font-size: 0.72rem; font-weight: 600; padding: 2px 8px; display: inline-flex; align-items: center; gap: 5px;" title="Gateway Online: rss.data1box.win">
            <span style="width: 6px; height: 6px; border-radius: 50%; background: #10b981; box-shadow: 0 0 6px #10b981;"></span>
            <span>Online</span>
          </span>
        </div>
      </div>

      <!-- Action buttons dynamically shown per tab -->
      <div style="display: flex; align-items: center; gap: 8px; flex-shrink: 0;">
        <!-- Dashboard Actions -->
        <div id="topbarActions_dashboard" style="display: flex; align-items: center; gap: 8px;">
          <button class="btn" onclick="reprocessAllFeeds(this)" title="Chạy lại toàn bộ Rules trên dữ liệu RAW của tất cả scraper" style="height: 32px; font-size: 0.76rem; color: #a855f7; border-color: rgba(168, 85, 247, 0.35);">
            <span>⚡ Chạy lại tất cả Rule</span>
          </button>
          <button class="btn" onclick="openAllIntervalsModal()" title="Xem lịch cào riêng của các Scraper" style="height: 32px; font-size: 0.76rem;">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:14px; height:14px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
            <span>Lịch cào</span>
          </button>
          <button class="btn primary" onclick="refreshAllFeeds()" title="Cào mới tất cả các scraper ngay" style="height: 32px; font-size: 0.76rem;">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
            <span>Cào mới tất cả</span>
          </button>
        </div>

        <!-- Scrapers Actions -->
        <div id="topbarActions_scrapers" style="display: none; align-items: center; gap: 8px;">
          <button class="btn" onclick="openAllIntervalsModal()" title="Xem lịch cào riêng của các Scraper" style="height: 32px; font-size: 0.76rem;">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:14px; height:14px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
            <span>Tần suất cào</span>
          </button>
          <button class="btn primary" onclick="openAddFeedModal()" style="height: 32px; font-size: 0.76rem;">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
            <span>+ Thêm kênh Feed</span>
          </button>
        </div>

        <!-- Profiles Actions -->
        <div id="topbarActions_profiles" style="display: none; align-items: center; gap: 8px;">
          <button class="btn primary" onclick="openAddProfileModal()" style="height: 32px; font-size: 0.76rem;">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
            <span>+ Thêm Profile mới</span>
          </button>
        </div>

        <!-- Rules Actions -->
        <div id="topbarActions_rules" style="display: none; align-items: center; gap: 8px;">
          <button class="btn" onclick="reprocessAllFeeds(this)" style="height: 32px; font-size: 0.76rem; color: #a855f7; border-color: rgba(168, 85, 247, 0.35);" title="Chạy lại Rules trên RAW data của tất cả scraper">
            <span>⚡ Re-process toàn bộ Feed</span>
          </button>
          <button class="btn primary" onclick="openAddRuleModal()" style="height: 32px; font-size: 0.76rem;">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
            <span>+ Thêm Rule mới</span>
          </button>
        </div>
      </div>
    </header>

    <main>
      <!-- TAB 1: DASHBOARD (Table kết quả các lượt cào của các scraper đã cấu hình) -->
      <div id="view_dashboard" class="main-tab-content">
        <!-- Quick KPI Strip -->
        <div class="kpi-strip" style="grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));">
          <div class="kpi-box">
            <div class="kpi-icon" style="background: rgba(14, 165, 233, 0.15); color: #38bdf8;">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 18px; height: 18px;"><circle cx="5" cy="19" r="1.5"/><path d="M4 4a16 16 0 0 1 16 16"/><path d="M4 11a9 9 0 0 1 9 9"/></svg>
            </div>
            <div class="kpi-info">
              <div class="kpi-label">Scrapers Hoạt Động</div>
              <div class="kpi-val">{len(feeds)} Kênh</div>
            </div>
          </div>
          <div class="kpi-box">
            <div class="kpi-icon" style="background: rgba(16, 185, 129, 0.15); color: #10b981;">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 18px; height: 18px;"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>
            </div>
            <div class="kpi-info">
              <div class="kpi-label">Task Output Sạch</div>
              <div class="kpi-val">{total_output_posts} Bài</div>
            </div>
          </div>
          <div class="kpi-box">
            <div class="kpi-icon" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8;">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 18px; height: 18px;"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
            </div>
            <div class="kpi-info">
              <div class="kpi-label">Dữ Liệu Scrape RAW</div>
              <div class="kpi-val">{total_raw_posts} Bài</div>
            </div>
          </div>
          <div class="kpi-box clickable" onclick="switchMainTab('rules')" style="cursor: pointer;" title="Bấm để xem kho Rule và Profile">
            <div class="kpi-icon" style="background: rgba(168, 85, 247, 0.15); color: #c084fc;">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 18px; height: 18px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
            </div>
            <div class="kpi-info">
              <div class="kpi-label">Kho Rule &amp; Profile</div>
              <div class="kpi-val">{len(rules_list)} Rules • {len(profiles)} Profiles</div>
            </div>
          </div>
        </div>

        <!-- Scraper Execution Table Card -->
        <div class="table-card">
          <div style="display: flex; align-items: center; justify-content: space-between; padding: 12px 16px; border-bottom: 1px solid var(--card-border); gap: 10px; flex-wrap: wrap;">
            <div style="display: flex; align-items: center; gap: 8px;">
              <span style="font-size: 0.88rem; font-weight: 700; color: #fff;">📊 Bảng Kết quả Lượt cào của các Scraper</span>
              <span class="badge blue" style="font-size: 0.68rem;">{len(feeds)} Scrapers</span>
            </div>
            <div style="display: flex; align-items: center; gap: 8px;">
              <input type="text" class="form-input" placeholder="🔍 Lọc theo tên, slug, domain..." style="height: 30px; font-size: 0.74rem; width: 190px;" oninput="filterDashboardTable(this.value)">
              <button class="btn primary" onclick="refreshAllFeeds()" style="height: 30px; font-size: 0.74rem;" title="Cào mới tất cả các scraper ngay">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 12px; height: 12px;"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
                <span>Cào tất cả</span>
              </button>
            </div>
          </div>

          <div class="table-responsive">
            <table class="data-table" id="dashboardTable">
              <thead>
                <tr>
                  <th style="width: 40%;">Scraper &amp; Mục tiêu</th>
                  <th style="width: 18%;">Engine &amp; Profile</th>
                  <th style="width: 22%;">Trạng thái &amp; Tần suất</th>
                  <th style="width: 20%; text-align: right;">Thao tác</th>
                </tr>
              </thead>
              <tbody>
                {dashboard_table_rows if dashboard_table_rows else '<tr><td colspan="4" style="text-align:center; padding:32px; color:var(--text-dim);">Chưa có Scraper nào được cấu hình.</td></tr>'}
              </tbody>
            </table>
          </div>
        </div>

        <!-- DASHBOARD POSTS EXPLORER: TASK OUTPUT & SCRAPED LIST -->
        <div id="dashboardExplorer" class="dashboard-explorer-section" style="margin-top: 24px;">
          <!-- Explorer Header & Action Toolbar -->
          <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 14px; gap: 12px; flex-wrap: wrap;">
            <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap;">
              <div style="display: flex; align-items: center; gap: 8px;">
                <span style="font-size: 0.95rem; font-weight: 700; color: #fff;">📑 Dòng chảy Bài viết &amp; Output</span>
                <span class="badge blue" id="dashTotalStatsBadge" style="font-size: 0.68rem;">Đang tải...</span>
              </div>
              
              <!-- View Mode Switcher -->
              <div style="display: inline-flex; background: rgba(255,255,255,0.04); padding: 3px; border-radius: 8px; border: 1px solid var(--card-border); gap: 2px;">
                <button type="button" class="btn-toggle active" id="btnDashViewSplit" onclick="setDashboardViewMode('split')" style="height: 26px; padding: 0 9px; font-size: 0.72rem; border-radius: 5px; cursor: pointer;" title="Xem song song 2 cột: Task Output &amp; RAW Scraped">
                  <span>👥 Song song (2 Cột)</span>
                </button>
                <button type="button" class="btn-toggle" id="btnDashViewOutput" onclick="setDashboardViewMode('output')" style="height: 26px; padding: 0 9px; font-size: 0.72rem; border-radius: 5px; cursor: pointer;" title="Chỉ hiển thị Task Output sau Rule">
                  <span>🎯 Task Output</span>
                  <span class="pill-count" id="dashOutputBadge" style="margin-left: 3px; font-size: 0.65rem;">0</span>
                </button>
                <button type="button" class="btn-toggle" id="btnDashViewScraped" onclick="setDashboardViewMode('scraped')" style="height: 26px; padding: 0 9px; font-size: 0.72rem; border-radius: 5px; cursor: pointer;" title="Chỉ hiển thị Task Chứa List Scrape Được">
                  <span>📥 List Scrape Được (RAW)</span>
                  <span class="pill-count" id="dashScrapedBadge" style="margin-left: 3px; font-size: 0.65rem;">0</span>
                </button>
              </div>
            </div>

            <!-- Explorer Filters -->
            <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
              <select id="dashFeedFilter" class="form-input" style="height: 30px; font-size: 0.74rem; width: 180px;" onchange="onDashFeedFilterChange(this.value)">
                <option value="all">🔍 Tất cả Kênh / Scraper</option>
              </select>
              <input type="text" id="dashPostSearch" class="form-input" placeholder="🔍 Lọc tiêu đề / nội dung..." style="height: 30px; font-size: 0.74rem; width: 180px;" oninput="onDashPostSearch(this.value)">
              <button type="button" class="btn" onclick="reloadDashboardPosts()" style="height: 30px; padding: 0 10px; font-size: 0.72rem;" title="Làm mới lại dữ liệu bài viết">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 12px; height: 12px;"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
                <span>Làm mới</span>
              </button>
            </div>
          </div>

          <!-- Dual Tasks Posts Container -->
          <div id="dashPostsGrid" style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px;">
            
            <!-- TASK 1: List Task Output sau khi qua Rule -->
            <div class="task-card" id="dashCardOutput" style="display: flex; flex-direction: column; max-height: 800px; padding: 14px; margin: 0; background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 12px;">
              <div style="display: flex; align-items: center; justify-content: space-between; padding-bottom: 10px; border-bottom: 1px solid var(--card-border); gap: 8px;">
                <div style="display: flex; align-items: center; gap: 8px;">
                  <span style="font-size: 0.88rem; font-weight: 700; color: #10b981; display: flex; align-items: center; gap: 6px;">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 16px; height: 16px;"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>
                    <span>🎯 Task Output (Sau Rule Engine)</span>
                  </span>
                  <span class="badge green" id="dashCardOutputHeaderCount" style="font-size: 0.68rem;">0 bài</span>
                </div>
                <div style="font-size: 0.7rem; color: var(--text-dim);" id="dashOutputFilterStatus">Tất cả kênh</div>
              </div>
              <div id="dashOutputList" style="flex: 1; overflow-y: auto; padding: 10px 0; display: flex; flex-direction: column; gap: 10px; min-height: 200px;">
                <div style="text-align: center; padding: 30px; color: var(--text-dim);">Đang tải dữ liệu Task Output...</div>
              </div>
            </div>

            <!-- TASK 2: Task Chứa List Scrape Được (RAW) -->
            <div class="task-card" id="dashCardScraped" style="display: flex; flex-direction: column; max-height: 800px; padding: 14px; margin: 0; background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 12px;">
              <div style="display: flex; align-items: center; justify-content: space-between; padding-bottom: 10px; border-bottom: 1px solid var(--card-border); gap: 8px;">
                <div style="display: flex; align-items: center; gap: 8px;">
                  <span style="font-size: 0.88rem; font-weight: 700; color: #38bdf8; display: flex; align-items: center; gap: 6px;">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 16px; height: 16px;"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                    <span>📥 Task Chứa List Scrape Được (RAW Data)</span>
                  </span>
                  <span class="badge blue" id="dashCardScrapedHeaderCount" style="font-size: 0.68rem;">0 bài</span>
                </div>
                <div style="font-size: 0.7rem; color: var(--text-dim);" id="dashScrapedFilterStatus">Dữ liệu thô từ nguồn</div>
              </div>
              <div id="dashScrapedList" style="flex: 1; overflow-y: auto; padding: 10px 0; display: flex; flex-direction: column; gap: 10px; min-height: 200px;">
                <div style="text-align: center; padding: 30px; color: var(--text-dim);">Đang tải dữ liệu scrape được...</div>
              </div>
            </div>

          </div>
        </div>
      </div>

      <!-- TAB 2: QUẢN LÝ SCRAPER (Xem, edit, xoá, add) -->
      <div id="view_scrapers" class="main-tab-content" style="display: none;">
        <!-- Controls Bar & Filter Pills -->
        <div class="controls-bar" style="margin-bottom: 14px; margin-top: 4px;">
          <div class="filter-pills">
            {cat_pills}
          </div>

          <div style="display: flex; align-items: center; gap: 8px;">
            <input type="text" class="form-input" placeholder="🔍 Lọc scraper..." style="height: 30px; font-size: 0.74rem; width: 170px;" oninput="filterScrapersBySearch(this.value)">
            <button class="btn primary" onclick="openAddFeedModal()" title="Thêm nguồn RSS mới" style="height: 30px; font-size: 0.74rem;">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
              <span>+ Thêm Scraper</span>
            </button>
          </div>
        </div>

        <!-- Task / Feeds List -->
        <div id="viewFeeds" class="task-list">
          {feed_cards if feed_cards else '<div style="text-align:center; padding:40px; color:var(--text-dim);">Chưa có kênh feed nào. Bấm "+ Thêm Scraper" để bắt đầu!</div>'}
        </div>
      </div>

      <!-- TAB 3: QUẢN LÝ PROFILE (Domain, username/password, api key, session cookie) -->
      <div id="view_profiles" class="main-tab-content" style="display: none;">
        <!-- Controls Bar & Domain Pills -->
        <div class="controls-bar" style="margin-bottom: 14px; margin-top: 4px;">
          <div class="filter-pills" id="domainFilterPills">
            {domain_pills}
          </div>

          <div style="display: flex; align-items: center; gap: 8px;">
            <input type="text" class="form-input" placeholder="🔍 Lọc domain, profile..." style="height: 30px; font-size: 0.74rem; width: 170px;" oninput="filterProfilesBySearch(this.value)">
            <button class="btn primary" onclick="openAddProfileModal()" title="Thêm profile xác thực mới" style="height: 30px; font-size: 0.74rem;">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
              <span>+ Thêm Profile</span>
            </button>
          </div>
        </div>

        <!-- Profiles List Grouped by Domain -->
        <div id="viewProfilesList">
          {profiles_html}
        </div>
      </div>

      <!-- TAB 4: QUẢN LÝ RULE (Kho Rule xử lý output: Replace, Filter, Format, Extract Links) -->
      <div id="view_rules" class="main-tab-content" style="display: none;">
        <!-- Controls Bar & Rule Pills -->
        <div class="controls-bar" style="margin-bottom: 14px; margin-top: 4px;">
          <div class="filter-pills" id="ruleFilterPills">
            {rule_pills}
          </div>

          <div style="display: flex; align-items: center; gap: 8px;">
            <input type="text" class="form-input" placeholder="🔍 Tìm rule, từ khóa..." style="height: 30px; font-size: 0.74rem; width: 170px;" oninput="filterRulesBySearch(this.value)">
            <button class="btn primary" onclick="openAddRuleModal()" title="Thêm Rule xử lý mới" style="height: 30px; font-size: 0.74rem;">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
              <span>+ Thêm Rule</span>
            </button>
          </div>
        </div>

        <!-- Rule Cards Grid -->
        <div id="viewRulesList">
          {rules_html}
        </div>
      </div>
    </main>
  </div>

  <!-- MODAL: ADD / CONFIGURE FEED -->
  <div id="modalAdd" class="modal-overlay" onclick="handleModalClick(event, 'modalAdd')">
    <div class="modal-card">
      <div class="modal-head">
        <h2 class="modal-title" id="modalAddTitle">Tạo Kênh RSS Mới (Universal Feed Generator)</h2>
        <button class="modal-close" onclick="closeModal('modalAdd')">✕</button>
      </div>

      <form id="formAddFeed" onsubmit="handleSaveFeed(event)">
        <input type="hidden" id="feedOriginalSlug" value="">
        <!-- 1. CHỌN LOẠI NGUỒN CÀO (SCRAPER ENGINE) -->
        <div class="form-group">
          <label class="form-label">Loại nguồn cào (Scraper Engine) *</label>
          <select id="feedType" class="form-select" onchange="handleTypeChange(this.value)" style="font-weight: 600; font-size: 0.85rem;">
            <option value="web" selected>🌐 Cào Trang Web Bất Kỳ (Generic Web Scraper - Tin tức, Diễn đàn, Blog...)</option>
            <option value="threads">🧵 Mạng xã hội Threads (Hashtag, Từ khóa hoặc Profile)</option>
            <option value="rsshub">🚀 Tuyến đường RSSHub Upstream (1000+ routes có sẵn)</option>
            <option value="custom_rss">📡 Đồng bộ URL RSS / Atom ngoài (External Feed)</option>
          </select>
          <div class="form-hint" id="hintEngine">Biến bất kỳ website, diễn đàn (Voz, Tinhte...), báo chí hay blog nào thành RSS chuẩn hóa.</div>
        </div>

        <!-- 2. MỤC TIÊU NGUỒN (TARGET URL / PARAM) -->
        <div class="form-group">
          <label class="form-label" id="lblTarget">Địa chỉ URL Trang web cần cào *</label>
          <input type="text" id="feedTarget" class="form-input" required placeholder="vd: https://tuoitre.vn/cong-nghe.htm, https://voz.vn/f/chuyen-tro-linh-tinh.17/" oninput="autoGuessWebsite(this.value)">
          <div class="form-hint" id="hintTarget">Nhập URL trang web. Hệ thống tự động phân tích HTML và bóc tách các bài viết mới thành RSS.</div>
        </div>

        <!-- 2.1 TÙY CHỌN NÂNG CAO CHO WEB SCRAPER -->
        <div id="sectionWebAdvanced" style="margin-bottom: 16px; padding: 12px 14px; background: rgba(7, 12, 24, 0.7); border: 1px solid var(--card-border); border-radius: 12px;">
          <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
            <span style="font-size: 0.76rem; font-weight: 700; color: #38bdf8;">⚙️ Tùy chỉnh CSS Selectors (Không bắt buộc)</span>
            <span style="font-size: 0.68rem; color: var(--text-dim);">Để trống để dùng Smart Auto-Detect</span>
          </div>
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px;">
            <div>
              <label style="font-size: 0.68rem; color: var(--text-muted); display: block; margin-bottom: 2px;">Item Container Selector</label>
              <input type="text" id="selItem" class="form-input" placeholder="vd: .structItem--thread, article" style="font-size: 0.75rem; height: 32px; font-family: var(--mono);">
            </div>
            <div>
              <label style="font-size: 0.68rem; color: var(--text-muted); display: block; margin-bottom: 2px;">Title Selector</label>
              <input type="text" id="selTitle" class="form-input" placeholder="vd: .title, h2, a" style="font-size: 0.75rem; height: 32px; font-family: var(--mono);">
            </div>
            <div>
              <label style="font-size: 0.68rem; color: var(--text-muted); display: block; margin-bottom: 2px;">Link Selector</label>
              <input type="text" id="selLink" class="form-input" placeholder="vd: a, h2 a" style="font-size: 0.75rem; height: 32px; font-family: var(--mono);">
            </div>
            <div>
              <label style="font-size: 0.68rem; color: var(--text-muted); display: block; margin-bottom: 2px;">Summary/Desc Selector</label>
              <input type="text" id="selDesc" class="form-input" placeholder="vd: .summary, p" style="font-size: 0.75rem; height: 32px; font-family: var(--mono);">
            </div>
          </div>
          <div style="margin-top: 10px;">
            <label style="display: flex; align-items: center; gap: 8px; font-size: 0.76rem; color: var(--text-muted); cursor: pointer;">
              <input type="checkbox" id="chkUseFlareSolverr">
              <span>🛡️ Sử dụng FlareSolverr (Vượt Cloudflare Turnstile / Bot Protection nếu trang web bị chặn)</span>
            </label>
          </div>
        </div>

        <!-- 3. THÔNG TIN HIỂN THỊ CỦA FEED -->
        <div class="form-group">
          <label class="form-label">Tên hiển thị kênh (Title) *</label>
          <input type="text" id="feedTitle" class="form-input" required placeholder="Ví dụ: Công nghệ Tuổi Trẻ, Voz Trò Chuyện Linh Tinh, ..." oninput="autoGenerateSlug(this.value)">
        </div>

        <div class="form-group">
          <label class="form-label">Định danh URL (Slug) *</label>
          <div style="display:flex; align-items:center; gap:8px;">
            <span style="font-size:0.75rem; color:var(--text-dim); font-family:var(--mono);">{BASE_URL}/</span>
            <input type="text" id="feedSlug" class="form-input" required placeholder="cong-nghe" style="font-family:var(--mono);">
            <span style="font-size:0.75rem; color:var(--text-dim); font-family:var(--mono);">.xml / .json</span>
          </div>
          <div class="form-hint">Chỉ gồm chữ cái viết thường không dấu, số và gạch ngang (a-z, 0-9, -).</div>
        </div>

        <div class="form-group">
          <label class="form-label">Thể loại / Chuyên mục (Category)</label>
          <input type="text" id="feedCategory" class="form-input" placeholder="vd: Công nghệ, Tin tức, Diễn đàn, Sách &amp; Ebooks, Giải trí">
        </div>

        <div class="form-group">
          <label class="form-label">Mô tả ngắn</label>
          <textarea id="feedDesc" class="form-textarea" rows="2" placeholder="Mô tả nội dung kênh feed này..."></textarea>
        </div>

        <!-- 4. CẤU HÌNH TẦN SUẤT CÀO DỮ LIỆU (SCRAPE INTERVAL) -->
        <div style="background: rgba(7, 12, 24, 0.7); border: 1px solid var(--card-border); border-radius: 14px; padding: 14px; margin-bottom: 16px;">
          <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px;">
            <div style="display: flex; align-items: center; gap: 8px;">
              <span style="font-size: 0.82rem; font-weight: 700; color: #c084fc;">⏱️ Cấu hình Tần suất Cào (Scrape Interval) *</span>
            </div>
            <span style="font-size: 0.68rem; color: var(--text-dim);">Chu kỳ quét ngầm độc lập</span>
          </div>

          <div style="margin-bottom: 10px;">
            <label class="form-label" style="margin-bottom: 6px;">Chọn nhanh chu kỳ cào định kỳ:</label>
            <div style="display: grid; grid-template-columns: repeat(5, 1fr); gap: 6px;">
              <button type="button" class="preset-interval-btn" onclick="selectFormPresetInterval(5)" id="btnFormPreset5" style="padding: 7px 4px; font-size: 0.75rem;">⚡ 5m</button>
              <button type="button" class="preset-interval-btn" onclick="selectFormPresetInterval(10)" id="btnFormPreset10" style="padding: 7px 4px; font-size: 0.75rem;">⚡ 10m</button>
              <button type="button" class="preset-interval-btn" onclick="selectFormPresetInterval(15)" id="btnFormPreset15" style="padding: 7px 4px; font-size: 0.75rem;">15m</button>
              <button type="button" class="preset-interval-btn" onclick="selectFormPresetInterval(30)" id="btnFormPreset30" style="padding: 7px 4px; font-size: 0.75rem;">30m</button>
              <button type="button" class="preset-interval-btn" onclick="selectFormPresetInterval(60)" id="btnFormPreset60" style="padding: 7px 4px; font-size: 0.75rem;">1h (60m)</button>
              <button type="button" class="preset-interval-btn" onclick="selectFormPresetInterval(120)" id="btnFormPreset120" style="padding: 7px 4px; font-size: 0.75rem;">2h</button>
              <button type="button" class="preset-interval-btn" onclick="selectFormPresetInterval(360)" id="btnFormPreset360" style="padding: 7px 4px; font-size: 0.75rem;">6h</button>
              <button type="button" class="preset-interval-btn" onclick="selectFormPresetInterval(720)" id="btnFormPreset720" style="padding: 7px 4px; font-size: 0.75rem;">12h</button>
              <button type="button" class="preset-interval-btn" onclick="selectFormPresetInterval(1440)" id="btnFormPreset1440" style="padding: 7px 4px; font-size: 0.75rem;">24h (1 ngày)</button>
            </div>
          </div>

          <div>
            <label class="form-label" style="font-size: 0.75rem;">Hoặc nhập số phút tùy chỉnh:</label>
            <div style="display: flex; align-items: center; gap: 8px;">
              <input type="number" id="feedInterval" class="form-input" min="1" max="1440" required value="30" style="font-weight: 700; font-size: 0.95rem; width: 120px; font-family: var(--mono);" oninput="updateFormPresetHighlight(this.value)">
              <span style="color: var(--text-muted); font-size: 0.8rem; font-weight: 600;">phút / lần cào</span>
            </div>
            <div class="form-hint" style="margin-top: 6px;">Mỗi Scraper hoạt động với chu kỳ cào riêng độc lập, hệ thống sẽ tự động cào bài mới theo đúng số phút đã đặt.</div>
          </div>
        </div>

        <!-- 5. CẤU HÌNH COOKIE XÁC THỰC CHO SCRAPER -->
        <div style="background: rgba(7, 12, 24, 0.7); border: 1px solid var(--card-border); border-radius: 14px; padding: 14px; margin-bottom: 16px;">
          <div class="form-group" style="margin-bottom: 10px;">
            <label class="form-label" style="display:flex; align-items:center; justify-content:space-between;">
              <span style="font-weight: 700; color: #38bdf8;">🔑 5. Cấu hình Cookie Xác thực cho Scraper</span>
              <a href="javascript:void(0)" onclick="openCookieVaultModal()" style="color:#38bdf8; font-size:0.72rem; text-decoration:none;">Quản lý Kho Cookie &rarr;</a>
            </label>
            <select id="feedCookieMode" class="form-select" onchange="handleCookieModeChange(this.value)">
              <option value="none">🌐 Không dùng Cookie (Truy cập công khai / Guest)</option>
              <option value="profile">📂 Chọn Cookie theo trang web từ Kho Cookie...</option>
              <option value="custom">✏️ Nhập Cookie riêng cho Scraper này</option>
            </select>
            <div class="form-hint" id="hintCookieMode">Scraper sẽ truy cập công khai mà không gửi cookie xác thực.</div>
          </div>

          <div id="groupCookieProfile" class="form-group" style="display: none; margin-bottom: 10px;">
            <label class="form-label">Chọn Bộ Cookie theo Trang web *</label>
            <select id="feedCookieProfile" class="form-select"></select>
            <div class="form-hint">Chọn bộ cookie tương ứng với trang web bạn muốn cào.</div>
          </div>

          <div id="groupCookieCustom" class="form-group" style="display: none; margin-bottom: 0;">
            <label class="form-label">Dán Cookie riêng cho Scraper này *</label>
            <textarea id="feedCookieCustom" class="form-textarea" rows="2" placeholder="vd: sessionid=xyz...; token=abc...; hoặc dán JSON từ Cookie-Editor" style="font-family: var(--mono); font-size: 0.76rem;"></textarea>
            
            <div style="margin-top: 8px;">
              <label style="display: flex; align-items: center; gap: 8px; font-size: 0.78rem; color: var(--text-dim); cursor: pointer;">
                <input type="checkbox" id="chkSaveToVault" onchange="document.getElementById('wrapProfileName').style.display = this.checked ? 'flex' : 'none';">
                <span style="color: var(--text-muted); font-weight: 500;">Lưu cookie này vào Kho để các Scraper khác của trang web này tái sử dụng</span>
              </label>
              <div id="wrapProfileName" style="display: none; margin-top: 8px; gap: 8px;">
                <input type="text" id="vaultProfileName" class="form-input" placeholder="Đặt tên bộ cookie (vd: Voz VIP, Tuổi Trẻ Member)" style="flex:1;">
                <input type="text" id="vaultWebsite" class="form-input" placeholder="Trang web / Domain (vd: voz.vn, tuoitre.vn)" style="flex:1;">
              </div>
            </div>
          </div>
        </div>

        <!-- 6. TÙY BIẾN ĐỊNH DẠNG ĐẦU RA (CUSTOM OUTPUT & FILTERS) -->
        <div style="background: rgba(7, 12, 24, 0.7); border: 1px solid var(--card-border); border-radius: 14px; padding: 14px; margin-bottom: 16px;">
          <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px;">
            <div style="display: flex; align-items: center; gap: 8px;">
              <span style="font-size: 0.8rem; font-weight: 700; color: #38bdf8;">⚙️ 6. Cấu hình Định dạng Đầu ra (Custom Output &amp; Filters)</span>
            </div>
            <span style="font-size: 0.68rem; color: var(--text-dim);">Tùy chỉnh RSS / JSON / Atom</span>
          </div>

          <!-- Title Template -->
          <div class="form-group" style="margin-bottom: 12px;">
            <label class="form-label" style="display: flex; justify-content: space-between;">
              <span>Mẫu Tiêu đề bài viết (Title Template)</span>
              <span style="font-size: 0.7rem; color: var(--text-dim); font-weight: normal;">Hỗ trợ: {{title}}, {{author}}, {{category}}, {{date}}</span>
            </label>
            <input type="text" id="outTitleTemplate" class="form-input" placeholder="Để trống để dùng mặc định, hoặc vd: [{{category}}] {{title}}" style="font-family: var(--mono); font-size: 0.78rem;">
            <div class="form-hint">Tùy biến cấu trúc tiêu đề xuất hiện trong ứng dụng đọc RSS (Feedly, Inoreader, NetNewsWire...).</div>
          </div>

          <!-- Content & Media Settings -->
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 12px;">
            <div>
              <label class="form-label">Chế độ Thân bài (Content Mode)</label>
              <select id="outContentMode" class="form-select" style="font-size: 0.78rem;">
                <option value="full" selected>📄 Toàn văn đầy đủ (Full Text &amp; Media)</option>
                <option value="summary">✂️ Chỉ trích đoạn ngắn (Summary snippet)</option>
              </select>
            </div>
            <div>
              <label class="form-label">Giới hạn số bài xuất ra (Max items)</label>
              <input type="number" id="outLimit" class="form-input" placeholder="Để trống hoặc vd: 25" min="1" max="200" style="font-size: 0.78rem;">
            </div>
          </div>

          <!-- Checkboxes for Media & Links -->
          <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px; margin-bottom: 12px; padding: 10px; background: rgba(15, 23, 42, 0.5); border-radius: 10px; border: 1px solid rgba(255, 255, 255, 0.05);">
            <label style="display: flex; align-items: center; gap: 8px; font-size: 0.75rem; color: var(--text-muted); cursor: pointer;">
              <input type="checkbox" id="outIncludeImages" checked>
              <span>🖼️ Nhúng hình ảnh</span>
            </label>
            <label style="display: flex; align-items: center; gap: 8px; font-size: 0.75rem; color: var(--text-muted); cursor: pointer;">
              <input type="checkbox" id="outIncludeSourceLink" checked>
              <span>🔗 Link xem bài gốc</span>
            </label>
            <label style="display: flex; align-items: center; gap: 8px; font-size: 0.75rem; color: var(--text-muted); cursor: pointer;">
              <input type="checkbox" id="outIncludeEnclosures" checked>
              <span>📥 Box file / Drive</span>
            </label>
          </div>

          <!-- Keyword Filters -->
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
            <div>
              <label class="form-label">Chỉ lấy bài có từ khóa (Include)</label>
              <input type="text" id="outFilterInclude" class="form-input" placeholder="vd: ebook, drive, AI, công nghệ" style="font-size: 0.78rem;">
              <div class="form-hint">Phân tách dấu phẩy. Bài viết phải chứa ít nhất 1 từ.</div>
            </div>
            <div>
              <label class="form-label">Bỏ qua bài có từ khóa (Exclude)</label>
              <input type="text" id="outFilterExclude" class="form-input" placeholder="vd: quảng cáo, tuyển dụng, giveaway" style="font-size: 0.78rem;">
              <div class="form-hint">Phân tách dấu phẩy. Bỏ qua nếu chứa từ này.</div>
            </div>
          </div>

          <!-- Output Rules Pipeline -->
          <div style="margin-top: 14px; padding-top: 12px; border-top: 1px dashed rgba(255, 255, 255, 0.08);">
            <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
              <label class="form-label" style="margin: 0; font-weight: 600; color: #38bdf8; display: flex; align-items: center; gap: 6px;">
                <span>⚡ Pipeline Xử lý Output (Rule Engine)</span>
              </label>
              <a href="javascript:void(0)" onclick="closeModal('modalAdd'); switchMainTab('rules');" style="font-size: 0.7rem; color: var(--accent); text-decoration: none;">+ Quản lý kho Rule &rarr;</a>
            </div>
            <div class="form-hint" style="margin-bottom: 8px;">Chọn các rule áp dụng cho luồng bài viết của scraper này (xóa quảng cáo, làm sạch HTML, bóc link Drive, lọc nâng cao):</div>
            <div id="feedAppliedRulesContainer" style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; max-height: 160px; overflow-y: auto; padding: 8px; background: rgba(15, 23, 42, 0.5); border-radius: 8px; border: 1px solid rgba(255, 255, 255, 0.05);">
              <div style="color: var(--text-dim); font-size: 0.75rem; grid-column: span 2;">Đang tải danh sách rule...</div>
            </div>
          </div>
        </div>

        <div style="display:flex; align-items:center; justify-content:flex-end; gap:10px; margin-top:20px; padding-top:16px; border-top:1px solid var(--card-border);">
          <button type="button" class="btn" onclick="closeModal('modalAdd')">Hủy bỏ</button>
          <button type="submit" id="btnSubmitFeed" class="btn primary">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:15px; height:15px;"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
            <span id="btnSubmitFeedText">Lưu &amp; Kích hoạt Feed</span>
          </button>
        </div>
      </form>
    </div>
  </div>

  <!-- MODAL: ADD / EDIT PROFILE (DOMAIN & CREDENTIALS) -->
  <div id="modalProfileEdit" class="modal-overlay" onclick="handleModalClick(event, 'modalProfileEdit')">
    <div class="modal-card" style="width: min(620px, 100%);">
      <div class="modal-head">
        <div>
          <h2 class="modal-title" id="lblProfileModalTitle">Thêm Profile Xác thực Mới</h2>
          <div style="font-size:0.75rem; color:var(--text-dim); margin-top:2px;">Quản lý xác thực theo từng Domain (Session Cookie, Tài khoản login, hoặc API Key).</div>
        </div>
        <button class="modal-close" onclick="closeModal('modalProfileEdit')">✕</button>
      </div>

      <form id="formProfileEdit" onsubmit="handleSaveProfile(event)">
        <input type="hidden" id="editProfileId" value="">

        <!-- Domain & Name -->
        <div style="display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-bottom:12px;">
          <div>
            <label class="form-label">Domain / Trang web *</label>
            <input type="text" id="profDomain" class="form-input" required placeholder="vd: threads.net, voz.vn, tuoitre.vn">
            <div class="form-hint">vd: threads.net, voz.vn, reddit.com</div>
          </div>
          <div>
            <label class="form-label">Tên Profile định danh *</label>
            <input type="text" id="profName" class="form-input" required placeholder="vd: Voz VIP Member, Acc Threads 1">
            <div class="form-hint">Tên gợi nhớ cho bộ profile này</div>
          </div>
        </div>

        <!-- Auth Method Selector -->
        <div class="form-group" style="margin-bottom:12px;">
          <label class="form-label">Phương thức Xác thực (Auth Method) *</label>
          <select id="profAuthType" class="form-select" onchange="handleAuthTypeChange(this.value)" style="font-weight:600; font-size:0.85rem;">
            <option value="cookie" selected>🍪 Session Cookie (Khuyên dùng cho Web Scraper / Threads)</option>
            <option value="login">👤 Đăng nhập bằng Tài khoản &amp; Mật khẩu (Username / Password)</option>
            <option value="api_key">🔑 API Key / Bearer Token / Secret Header</option>
            <option value="custom_header">⚙️ Custom HTTP Headers (Định dạng JSON)</option>
          </select>
        </div>

        <!-- Method 1: Cookie -->
        <div id="groupAuthCookie" class="form-group" style="margin-bottom:12px;">
          <label class="form-label">Chuỗi Cookie xác thực *</label>
          <textarea id="profCookie" class="form-textarea" rows="3" placeholder="vd: sessionid=xyz...; token=abc...; hoặc dán mảng JSON từ extension Cookie-Editor" style="font-family:var(--mono); font-size:0.75rem;"></textarea>
          <div class="form-hint">Có thể dán chuỗi cookie thô dạng `name=val; name2=val2` hoặc JSON export từ trình duyệt.</div>
        </div>

        <!-- Method 2: Login Username/Password -->
        <div id="groupAuthLogin" style="display:none; background:rgba(7, 12, 24, 0.7); border:1px solid var(--card-border); border-radius:12px; padding:14px; margin-bottom:14px;">
          <div style="display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-bottom:10px;">
            <div>
              <label class="form-label">Tên đăng nhập / Email *</label>
              <input type="text" id="profUsername" class="form-input" placeholder="user@example.com hoặc username">
            </div>
            <div>
              <label class="form-label">Mật khẩu (Password) *</label>
              <input type="password" id="profPassword" class="form-input" placeholder="••••••••••••">
            </div>
          </div>
          <div class="form-group" style="margin-bottom:0;">
            <label class="form-label">URL Trang đăng nhập (Tùy chọn)</label>
            <input type="text" id="profLoginUrl" class="form-input" placeholder="vd: https://voz.vn/login/">
          </div>
        </div>

        <!-- Method 3: API Key -->
        <div id="groupAuthApiKey" style="display:none; background:rgba(7, 12, 24, 0.7); border:1px solid var(--card-border); border-radius:12px; padding:14px; margin-bottom:14px;">
          <div style="display:grid; grid-template-columns: 1fr 2fr; gap:10px; margin-bottom:0;">
            <div>
              <label class="form-label">Tên Header</label>
              <input type="text" id="profHeaderName" class="form-input" value="Authorization" placeholder="vd: Authorization, X-Api-Key">
            </div>
            <div>
              <label class="form-label">Giá trị API Key / Token *</label>
              <input type="text" id="profApiKey" class="form-input" placeholder="vd: Bearer sk-..." style="font-family:var(--mono); font-size:0.78rem;">
            </div>
          </div>
        </div>

        <!-- Method 4: Custom Headers -->
        <div id="groupAuthHeaders" class="form-group" style="display:none; margin-bottom:12px;">
          <label class="form-label">Custom HTTP Headers (JSON format)</label>
          <textarea id="profCustomHeaders" class="form-textarea" rows="3" placeholder='{{"User-Agent": "...", "X-Custom-Auth": "..."}}' style="font-family:var(--mono); font-size:0.75rem;"></textarea>
        </div>

        <!-- Description -->
        <div class="form-group" style="margin-bottom:16px;">
          <label class="form-label">Ghi chú / Mô tả (Tùy chọn)</label>
          <input type="text" id="profDesc" class="form-input" placeholder="Ghi chú về tài khoản, thời hạn cookie, mục đích sử dụng...">
        </div>

        <div style="display:flex; justify-content:flex-end; gap:8px; padding-top:14px; border-top:1px solid var(--card-border);">
          <button type="button" class="btn" onclick="closeModal('modalProfileEdit')" style="height:32px; font-size:0.76rem;">Hủy</button>
          <button type="submit" class="btn primary" id="btnSubmitProfile" style="height:32px; font-size:0.76rem;">Lưu Profile</button>
        </div>
      </form>
    </div>
  </div>

  <!-- MODAL: QUICK PER-FEED INTERVAL -->
  <div id="modalFeedInterval" class="modal-overlay" onclick="handleModalClick(event, 'modalFeedInterval')">
    <div class="modal-card" style="width: min(520px, 100%);">
      <div class="modal-head">
        <div>
          <h2 class="modal-title" id="lblFeedIntervalTitle">Tần suất cào cho kênh</h2>
          <div style="font-size:0.75rem; color:var(--text-dim); margin-top:2px;" id="lblFeedIntervalSub">Thiết lập chu kỳ tự động cào bài viết mới cho riêng scraper này.</div>
        </div>
        <button class="modal-close" onclick="closeModal('modalFeedInterval')">✕</button>
      </div>

      <form onsubmit="handleSaveFeedInterval(event)">
        <input type="hidden" id="targetFeedSlug" value="">
        <div style="margin-bottom: 16px;">
          <label class="form-label">Chọn chu kỳ quét cho riêng kênh này:</label>
          <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-top: 8px;">
            <button type="button" class="preset-interval-btn" onclick="selectFeedPresetInterval(5)" id="btnFeedPreset5">⚡ 5 Phút</button>
            <button type="button" class="preset-interval-btn" onclick="selectFeedPresetInterval(10)" id="btnFeedPreset10">⚡ 10 Phút</button>
            <button type="button" class="preset-interval-btn" onclick="selectFeedPresetInterval(15)" id="btnFeedPreset15">15 Phút</button>
            <button type="button" class="preset-interval-btn" onclick="selectFeedPresetInterval(30)" id="btnFeedPreset30">30 Phút</button>
            <button type="button" class="preset-interval-btn" onclick="selectFeedPresetInterval(60)" id="btnFeedPreset60">1 Giờ (60m)</button>
            <button type="button" class="preset-interval-btn" onclick="selectFeedPresetInterval(120)" id="btnFeedPreset120">2 Giờ</button>
            <button type="button" class="preset-interval-btn" onclick="selectFeedPresetInterval(360)" id="btnFeedPreset360">6 Giờ</button>
            <button type="button" class="preset-interval-btn" onclick="selectFeedPresetInterval(720)" id="btnFeedPreset720">12 Giờ</button>
            <button type="button" class="preset-interval-btn" onclick="selectFeedPresetInterval(1440)" id="btnFeedPreset1440">24 Giờ</button>
          </div>
        </div>

        <div class="form-group" style="margin-bottom: 16px;">
          <label class="form-label">Hoặc nhập số phút tùy chỉnh:</label>
          <div style="display: flex; align-items: center; gap: 10px;">
            <input type="number" id="inputFeedIntervalVal" class="form-input" min="1" max="1440" required value="30" style="font-weight: 700; font-size: 1rem; width: 130px;" oninput="updateFeedPresetHighlight(this.value)">
            <span style="color: var(--text-muted); font-size: 0.85rem; font-weight: 600;">phút / lần cào</span>
          </div>
          <div class="form-hint" style="margin-top: 6px;">Hệ thống chạy background daemon kiểm tra và chỉ cào mới cho kênh này sau mỗi chu kỳ trên.</div>
        </div>

        <div style="display:flex; justify-content:flex-end; gap:8px; padding-top:14px; border-top:1px solid var(--card-border);">
          <button type="button" class="btn" onclick="closeModal('modalFeedInterval')">Hủy bỏ</button>
          <button type="submit" id="btnSaveFeedInterval" class="btn primary">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:14px; height:14px;"><polyline points="20 6 9 17 4 12"/></svg>
            <span>Lưu tần suất kênh này</span>
          </button>
        </div>
      </form>
    </div>
  </div>

  <!-- MODAL: ALL SCRAPERS INTERVALS OVERVIEW -->
  <div id="modalAllIntervals" class="modal-overlay" onclick="handleModalClick(event, 'modalAllIntervals')">
    <div class="modal-card" style="width: min(650px, 100%);">
      <div class="modal-head">
        <div>
          <h2 class="modal-title">Lịch Cào Theo Từng Scraper</h2>
          <div style="font-size:0.75rem; color:var(--text-dim); margin-top:2px;">Mỗi kênh hoạt động theo tần suất riêng biệt. Bấm vào nút tần suất của từng kênh để đổi nhanh.</div>
        </div>
        <button class="modal-close" onclick="closeModal('modalAllIntervals')">✕</button>
      </div>

      <div id="allIntervalsList" style="display:flex; flex-direction:column; gap:8px; margin-bottom:16px;">
        <!-- dynamic rows rendered from feeds -->
      </div>

      <div style="display:flex; justify-content:flex-end; padding-top:14px; border-top:1px solid var(--card-border);">
        <button type="button" class="btn" onclick="closeModal('modalAllIntervals')">Đóng</button>
      </div>
    </div>
  </div>

  <!-- MODAL: ADD / EDIT RULE -->
  <div id="modalRuleEdit" class="modal-overlay" onclick="handleModalClick(event, 'modalRuleEdit')">
    <div class="modal-card" style="width: min(680px, 100%);">
      <div class="modal-head">
        <div>
          <h2 class="modal-title" id="lblModalRuleTitle">Thêm Rule Xử Lý Output</h2>
          <div style="font-size:0.75rem; color:var(--text-dim); margin-top:2px;">Tùy biến bộ lọc, định dạng text hoặc bóc link đính kèm cho bài viết.</div>
        </div>
        <button class="modal-close" onclick="closeModal('modalRuleEdit')">✕</button>
      </div>

      <form id="formRuleEdit" onsubmit="handleSaveRule(event)">
        <input type="hidden" id="ruleIsEdit" value="false">

        <!-- Name & ID -->
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 14px;">
          <div>
            <label class="form-label required">Tên Rule</label>
            <input type="text" id="ruleName" class="form-input" required placeholder="vd: Xóa quảng cáo Telegram" oninput="autoGenerateRuleId(this.value)">
          </div>
          <div>
            <label class="form-label required">Mã định danh (ID Slug)</label>
            <input type="text" id="ruleId" class="form-input" required placeholder="vd: clean_tg_ads" style="font-family: var(--mono); font-size: 0.8rem;">
          </div>
        </div>

        <!-- Rule Type -->
        <div class="form-group" style="margin-bottom: 14px;">
          <label class="form-label required">Loại Rule (Action Type)</label>
          <select id="ruleType" class="form-select" onchange="handleRuleTypeChange(this.value)" style="font-weight: 600;">
            <option value="replace">🔄 Tìm kiếm &amp; Thay thế (Text / Regex Replace)</option>
            <option value="filter">⛔ Lọc bài viết (Conditional Filter: Include / Exclude)</option>
            <option value="format">✨ Định dạng &amp; Làm sạch (Sanitize HTML / Prefix / Truncate)</option>
            <option value="extract_links">🔗 Bóc tách link đính kèm (Google Drive / Fshare / Mega)</option>
          </select>
        </div>

        <!-- DYNAMIC GROUP 1: REPLACE -->
        <div id="groupRuleReplace" class="rule-type-group" style="padding: 12px; background: rgba(15, 23, 42, 0.5); border: 1px solid rgba(255,255,255,0.06); border-radius: 10px; margin-bottom: 14px;">
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px;">
            <div>
              <label class="form-label">Phạm vi áp dụng (Target Field)</label>
              <select id="ruleReplaceTarget" class="form-select" style="font-size: 0.8rem;">
                <option value="both">Cả Tiêu đề &amp; Nội dung</option>
                <option value="title">Chỉ Tiêu đề</option>
                <option value="content">Chỉ Nội dung</option>
              </select>
            </div>
            <div style="display: flex; align-items: flex-end; padding-bottom: 6px;">
              <label style="display: flex; align-items: center; gap: 8px; font-size: 0.8rem; color: var(--text-muted); cursor: pointer;">
                <input type="checkbox" id="ruleReplaceIsRegex">
                <span>Biểu thức chính quy (Regex Pattern)</span>
              </label>
            </div>
          </div>
          <div class="form-group" style="margin-bottom: 10px;">
            <label class="form-label required">Chuỗi hoặc Mẫu tìm kiếm (Pattern)</label>
            <input type="text" id="ruleReplacePattern" class="form-input" placeholder="vd: https?://t\.me/\S+|\[Quảng cáo\]" style="font-family: var(--mono); font-size: 0.8rem;">
            <div class="form-hint">Nhập từ khóa đơn giản hoặc biểu thức Regex muốn tìm và thay thế.</div>
          </div>
          <div class="form-group" style="margin-bottom: 0;">
            <label class="form-label">Thay thế bằng (Replacement)</label>
            <input type="text" id="ruleReplaceReplacement" class="form-input" placeholder="Để trống nếu muốn XÓA HOÀN TOÀN chuỗi tìm được" style="font-family: var(--mono); font-size: 0.8rem;">
          </div>
        </div>

        <!-- DYNAMIC GROUP 2: FILTER -->
        <div id="groupRuleFilter" class="rule-type-group" style="display: none; padding: 12px; background: rgba(15, 23, 42, 0.5); border: 1px solid rgba(255,255,255,0.06); border-radius: 10px; margin-bottom: 14px;">
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px;">
            <div>
              <label class="form-label">Điều kiện lọc (Condition)</label>
              <select id="ruleFilterCondition" class="form-select" style="font-size: 0.8rem;">
                <option value="exclude">⛔ Bỏ qua nếu khớp (Exclude / Drop post)</option>
                <option value="include">✅ Chỉ giữ lại nếu khớp (Include only)</option>
              </select>
            </div>
            <div>
              <label class="form-label">Kiểm tra trong (Target)</label>
              <select id="ruleFilterTarget" class="form-select" style="font-size: 0.8rem;">
                <option value="both">Cả Tiêu đề &amp; Nội dung</option>
                <option value="title">Chỉ Tiêu đề</option>
                <option value="content">Chỉ Nội dung</option>
                <option value="author">Tên Tác giả</option>
              </select>
            </div>
          </div>
          <div class="form-group" style="margin-bottom: 0;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
              <label class="form-label required" style="margin:0;">Mẫu từ khóa hoặc Regex (Pattern)</label>
              <label style="display: flex; align-items: center; gap: 6px; font-size: 0.75rem; color: var(--text-muted); cursor: pointer;">
                <input type="checkbox" id="ruleFilterIsRegex" checked>
                <span>Sử dụng Regex</span>
              </label>
            </div>
            <input type="text" id="ruleFilterPattern" class="form-input" placeholder="vd: giveaway|tuyển dụng|khuyến mãi|shopee\.vn" style="font-family: var(--mono); font-size: 0.8rem;">
            <div class="form-hint">Nếu chọn Exclude, bài viết chứa từ khóa này sẽ bị lọc bỏ khỏi luồng RSS.</div>
          </div>
        </div>

        <!-- DYNAMIC GROUP 3: FORMAT -->
        <div id="groupRuleFormat" class="rule-type-group" style="display: none; padding: 12px; background: rgba(15, 23, 42, 0.5); border: 1px solid rgba(255,255,255,0.06); border-radius: 10px; margin-bottom: 14px;">
          <div style="margin-bottom: 10px;">
            <label style="display: flex; align-items: center; gap: 8px; font-size: 0.8rem; color: var(--text-muted); cursor: pointer;">
              <input type="checkbox" id="ruleFormatCleanHtml" checked>
              <span>🛡️ Tự động làm sạch mã HTML độc hại &amp; rác tracking</span>
            </label>
          </div>
          <div class="form-group" style="margin-bottom: 10px;">
            <label class="form-label">Thẻ HTML cần loại bỏ triệt để (Strip Tags)</label>
            <input type="text" id="ruleFormatStripTags" class="form-input" value="script,style,iframe,object,embed,form" style="font-family: var(--mono); font-size: 0.8rem;">
            <div class="form-hint">Phân tách dấu phẩy. Các thẻ này và nội dung bên trong sẽ bị loại bỏ.</div>
          </div>
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px;">
            <div>
              <label class="form-label">Tiền tố tiêu đề (Title Prefix)</label>
              <input type="text" id="ruleFormatTitlePrefix" class="form-input" placeholder="vd: [Tin mới] " style="font-size: 0.8rem;">
            </div>
            <div>
              <label class="form-label">Hậu tố tiêu đề (Title Suffix)</label>
              <input type="text" id="ruleFormatTitleSuffix" class="form-input" placeholder="vd:  - ClaraOS" style="font-size: 0.8rem;">
            </div>
          </div>
          <div>
            <label class="form-label">Cắt ngắn nội dung tối đa (Truncate chars)</label>
            <input type="number" id="ruleFormatTruncate" class="form-input" placeholder="Để trống nếu không muốn cắt (vd: 500)" min="50" max="50000" style="font-size: 0.8rem;">
          </div>
        </div>

        <!-- DYNAMIC GROUP 4: EXTRACT LINKS -->
        <div id="groupRuleExtract" class="rule-type-group" style="display: none; padding: 12px; background: rgba(15, 23, 42, 0.5); border: 1px solid rgba(255,255,255,0.06); border-radius: 10px; margin-bottom: 14px;">
          <label class="form-label">Dịch vụ lưu trữ cần quét link tải:</label>
          <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-bottom: 10px;">
            <label style="display: flex; align-items: center; gap: 6px; font-size: 0.76rem; color: var(--text-muted); cursor: pointer;">
              <input type="checkbox" id="chkExtractGdrive" checked>
              <span>Google Drive</span>
            </label>
            <label style="display: flex; align-items: center; gap: 6px; font-size: 0.76rem; color: var(--text-muted); cursor: pointer;">
              <input type="checkbox" id="chkExtractFshare" checked>
              <span>Fshare.vn</span>
            </label>
            <label style="display: flex; align-items: center; gap: 6px; font-size: 0.76rem; color: var(--text-muted); cursor: pointer;">
              <input type="checkbox" id="chkExtractMega" checked>
              <span>Mega.nz</span>
            </label>
            <label style="display: flex; align-items: center; gap: 6px; font-size: 0.76rem; color: var(--text-muted); cursor: pointer;">
              <input type="checkbox" id="chkExtractMediafire" checked>
              <span>Mediafire</span>
            </label>
          </div>
          <div class="form-group" style="margin-bottom: 10px;">
            <label class="form-label">Regex quét link tùy biến (Custom Regex)</label>
            <input type="text" id="ruleExtractCustomRegex" class="form-input" placeholder="Để trống hoặc vd: https?://(?:www\.)?example\.com/download/\w+" style="font-family: var(--mono); font-size: 0.8rem;">
          </div>
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
            <div>
              <label class="form-label">Tự động gắn Tag vào đầu Tiêu đề</label>
              <input type="text" id="ruleExtractAutoTag" class="form-input" placeholder="vd: [Ebook] hoặc [Download]" style="font-size: 0.8rem;">
            </div>
            <div style="display: flex; align-items: flex-end; padding-bottom: 6px;">
              <label style="display: flex; align-items: center; gap: 6px; font-size: 0.78rem; color: var(--text-muted); cursor: pointer;">
                <input type="checkbox" id="ruleExtractEnclosure" checked>
                <span>Gắn Download Box &amp; Enclosure</span>
              </label>
            </div>
          </div>
        </div>

        <!-- Description -->
        <div class="form-group" style="margin-bottom: 16px;">
          <label class="form-label">Mô tả tóm tắt công dụng</label>
          <input type="text" id="ruleDesc" class="form-input" placeholder="vd: Tự động bóc tách link Google Drive từ bài viết và thêm tag [Ebook]">
        </div>

        <div style="display:flex; justify-content:flex-end; gap:8px; padding-top:14px; border-top:1px solid var(--card-border);">
          <button type="button" class="btn" onclick="closeModal('modalRuleEdit')" style="height:32px; font-size:0.76rem;">Hủy</button>
          <button type="submit" class="btn primary" id="btnSubmitRule" style="height:32px; font-size:0.76rem;">
            <span id="btnSubmitRuleText">Lưu Rule</span>
          </button>
        </div>
      </form>
    </div>
  </div>

  <!-- MODAL: COMPARE RAW VS TASK OUTPUT -->
  <div id="modalCompareOutput" class="modal-overlay" onclick="handleModalClick(event, 'modalCompareOutput')">
    <div class="modal-card" style="width: min(920px, 96vw); max-height: 90vh; display: flex; flex-direction: column;">
      <!-- Modal Head -->
      <div class="modal-head" style="padding-bottom: 12px; border-bottom: 1px solid var(--card-border);">
        <div>
          <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
            <h2 class="modal-title" id="cmpModalTitle" style="font-size: 1.1rem;">So sánh Task Output &amp; RAW Data</h2>
            <span id="cmpModalCatBadge" class="badge gray">Chung</span>
            <span id="cmpModalSlug" style="font-family: var(--mono); font-size: 0.75rem; color: var(--text-dim);">/slug</span>
          </div>
          <div style="display: flex; align-items: center; gap: 12px; margin-top: 6px; font-size: 0.76rem; color: var(--text-dim); flex-wrap: wrap;">
            <span id="cmpStatsBar">RAW: <strong>0</strong> ➔ Output: <strong style="color:#10b981;">0</strong> bài</span>
            <span>•</span>
            <span id="cmpRulesAppliedSummary" style="color: #38bdf8;">0 rules áp dụng</span>
            <span>•</span>
            <span id="cmpUpdatedAt">Vừa xong</span>
          </div>
        </div>
        <button class="modal-close" onclick="closeModal('modalCompareOutput')">✕</button>
      </div>

      <!-- Sub Navigation Tabs -->
      <div style="display: flex; align-items: center; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,0.06); gap: 10px; flex-wrap: wrap;">
        <div style="display: flex; gap: 6px;">
          <button type="button" class="pill active" id="tabBtnOutput" onclick="switchCompareTab('output')">
            <span>🎯 Task Output sau Rule</span>
            <span class="pill-count" id="cmpOutputBadge">0</span>
          </button>
          <button type="button" class="pill" id="tabBtnRaw" onclick="switchCompareTab('raw')">
            <span>📄 RAW Data gốc</span>
            <span class="pill-count" id="cmpRawBadge">0</span>
          </button>
        </div>
        <div id="cmpActiveRulesPills" style="display: flex; gap: 4px; flex-wrap: wrap; align-items: center;"></div>
      </div>

      <!-- Modal Body -->
      <div style="flex: 1; overflow-y: auto; padding: 14px 0; min-height: 280px;" id="cmpModalBody">
        <!-- Tab 1: Task Output -->
        <div id="cmpTabOutputPanel">
          <div id="cmpOutputList" style="display: flex; flex-direction: column; gap: 10px;">
            <div style="text-align:center; padding:30px; color:var(--text-dim);">Đang tải dữ liệu...</div>
          </div>
        </div>

        <!-- Tab 2: RAW Data -->
        <div id="cmpTabRawPanel" style="display: none;">
          <div id="cmpRawList" style="display: flex; flex-direction: column; gap: 10px;">
            <div style="text-align:center; padding:30px; color:var(--text-dim);">Đang tải dữ liệu...</div>
          </div>
        </div>
      </div>

      <!-- Modal Footer -->
      <div class="modal-foot" style="padding-top: 12px; border-top: 1px solid var(--card-border); display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap;">
        <div style="display: flex; align-items: center; gap: 8px;">
          <button type="button" class="btn primary" id="btnCmpReprocess" onclick="reprocessModalFeed()" style="height: 32px; padding: 0 12px; font-size: 0.75rem;" title="Chạy lại bộ Rules trên RAW Data không cần cào lại">
            <span>⚡ Chạy lại Rule</span>
          </button>
          <button type="button" class="btn" id="btnCmpScrape" onclick="scrapeModalFeed()" style="height: 32px; padding: 0 12px; font-size: 0.75rem;" title="Cào mới dữ liệu trực tiếp từ Website">
            <span>🌐 Cào mới từ Web</span>
          </button>
        </div>
        <div style="display: flex; align-items: center; gap: 8px;">
          <a id="cmpLinkXml" href="#" target="_blank" class="format-btn xml" style="height: 28px; padding: 0 8px; font-size: 0.72rem;">XML</a>
          <a id="cmpLinkJson" href="#" target="_blank" class="format-btn json" style="height: 28px; padding: 0 8px; font-size: 0.72rem;">JSON</a>
          <a id="cmpLinkAtom" href="#" target="_blank" class="format-btn atom" style="height: 28px; padding: 0 8px; font-size: 0.72rem;">ATOM</a>
          <button type="button" class="btn" onclick="closeModal('modalCompareOutput')" style="height: 32px; padding: 0 14px;">Đóng</button>
        </div>
      </div>
    </div>
  </div>

  <!-- Toast Notification -->
  <div id="toast" class="toast">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 18px; height: 18px; color: #34d399;"><polyline points="20 6 9 17 4 12"/></svg>
    <span id="toastMsg">Đã sao chép liên kết vào bộ nhớ tạm!</span>
  </div>

  <script>
    function showToast(msg, isError = false) {{
      const t = document.getElementById('toast');
      const m = document.getElementById('toastMsg');
      m.innerText = msg;
      if (isError) {{
        t.classList.add('error');
      }} else {{
        t.classList.remove('error');
      }}
      t.classList.add('show');
      setTimeout(() => t.classList.remove('show'), 2500);
    }}

    function copyLink(url, btn) {{
      navigator.clipboard.writeText(url).then(() => {{
        showToast('Đã chép: ' + url);
        if (btn) {{
          const orig = btn.innerHTML;
          btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px; color: #34d399;"><polyline points="20 6 9 17 4 12"/></svg><span>Đã chép</span>';
          setTimeout(() => btn.innerHTML = orig, 1800);
        }}
      }}).catch(() => {{
        showToast('Không thể sao chép!', true);
      }});
    }}

    function refreshFeed(slug, btn) {{
      if (btn) {{
        btn.dataset.origHtml = btn.innerHTML;
        btn.innerHTML = '<svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px; animation: spin 1s linear infinite;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><span>Đang cào...</span>';
        btn.disabled = true;
      }}
      fetch('/api/refresh/' + slug)
        .then(r => r.json())
        .then(d => {{
          const msg = (d.output_count !== undefined)
            ? 'Đã cào mới: ' + d.raw_count + ' ➔ ' + d.output_count + ' bài'
            : 'Đã cào mới thành công: ' + d.count + ' bài viết';
          showToast(msg);
          setTimeout(() => location.reload(), 1000);
        }})
        .catch(e => {{
          showToast('Lỗi khi cào: ' + e, true);
          if (btn && btn.dataset.origHtml) {{
            btn.innerHTML = btn.dataset.origHtml;
            btn.disabled = false;
          }}
        }});
    }}

    function refreshAllFeeds() {{
      const btn = document.getElementById('btnRefreshTop');
      if (btn) btn.innerHTML = '<svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 15px; height: 15px; animation: spin 1s linear infinite;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><span>Đang làm mới...</span>';
      fetch('/api/feeds')
        .then(r => r.json())
        .then(async feeds => {{
          const keys = Object.keys(feeds);
          for (let k of keys) {{
            await fetch('/api/refresh/' + k);
          }}
          showToast('Đã hoàn tất làm mới tất cả các kênh');
          setTimeout(() => location.reload(), 1000);
        }})
        .catch(e => showToast('Lỗi: ' + e, true));
    }}

    function deleteFeed(slug) {{
      if (!confirm('Bạn có chắc chắn muốn xóa kênh feed [' + slug + '] không?')) return;
      fetch('/api/feeds/' + slug, {{ method: 'DELETE' }})
        .then(r => r.json())
        .then(d => {{
          showToast('Đã xóa thành công kênh feed [' + slug + ']');
          setTimeout(() => location.reload(), 800);
        }})
        .catch(e => showToast('Lỗi khi xóa: ' + e, true));
    }}

    // MODAL COMPARE & TASK OUTPUT JAVASCRIPT HANDLERS
    let currentCmpSlug = null;

    function openCompareModal(slug) {{
      currentCmpSlug = slug;
      const modal = document.getElementById('modalCompareOutput');
      if (!modal) return;

      const titleEl = document.getElementById('cmpModalTitle');
      if (titleEl) titleEl.innerText = 'So sánh: ' + slug;
      const slugEl = document.getElementById('cmpModalSlug');
      if (slugEl) slugEl.innerText = '/' + slug;
      const catEl = document.getElementById('cmpModalCatBadge');
      if (catEl) catEl.innerText = 'Đang tải...';

      const lXml = document.getElementById('cmpLinkXml');
      if (lXml) lXml.href = '/' + slug + '.xml';
      const lJson = document.getElementById('cmpLinkJson');
      if (lJson) lJson.href = '/' + slug + '.json';
      const lAtom = document.getElementById('cmpLinkAtom');
      if (lAtom) lAtom.href = '/' + slug + '.atom';

      const outList = document.getElementById('cmpOutputList');
      if (outList) outList.innerHTML = '<div style="text-align:center; padding:30px; color:var(--text-dim);"><svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 20px; height: 20px; animation: spin 1s linear infinite; display: inline-block; margin-bottom: 8px;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><br>Đang tải Task Output sau Rule...</div>';
      const rawList = document.getElementById('cmpRawList');
      if (rawList) rawList.innerHTML = '<div style="text-align:center; padding:30px; color:var(--text-dim);"><svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 20px; height: 20px; animation: spin 1s linear infinite; display: inline-block; margin-bottom: 8px;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><br>Đang tải RAW Data gốc...</div>';

      switchCompareTab('output');
      openModal('modalCompareOutput');
      loadCompareModalData(slug);
    }}

    function switchCompareTab(tab) {{
      const tabBtnOut = document.getElementById('tabBtnOutput');
      const tabBtnRaw = document.getElementById('tabBtnRaw');
      const panelOut = document.getElementById('cmpTabOutputPanel');
      const panelRaw = document.getElementById('cmpTabRawPanel');

      if (tab === 'output') {{
        if (tabBtnOut) tabBtnOut.classList.add('active');
        if (tabBtnRaw) tabBtnRaw.classList.remove('active');
        if (panelOut) panelOut.style.display = 'block';
        if (panelRaw) panelRaw.style.display = 'none';
      }} else {{
        if (tabBtnOut) tabBtnOut.classList.remove('active');
        if (tabBtnRaw) tabBtnRaw.classList.add('active');
        if (panelOut) panelOut.style.display = 'none';
        if (panelRaw) panelRaw.style.display = 'block';
      }}
    }}

    function loadCompareModalData(slug) {{
      fetch('/api/feeds/' + slug + '/output')
        .then(r => {{
          if (!r.ok) throw new Error('Không thể tải dữ liệu output cho ' + slug);
          return r.json();
        }})
        .then(d => {{
          const titleEl = document.getElementById('cmpModalTitle');
          if (titleEl) titleEl.innerText = 'So sánh: ' + (d.title || d.slug);
          const slugEl = document.getElementById('cmpModalSlug');
          if (slugEl) slugEl.innerText = '/' + d.slug;
          const catEl = document.getElementById('cmpModalCatBadge');
          if (catEl) catEl.innerText = d.category || 'Chung';

          const outCount = d.output_count || 0;
          const rawCount = d.raw_count || 0;
          const filtCount = d.filtered_count || 0;

          const statsBar = document.getElementById('cmpStatsBar');
          if (statsBar) {{
            statsBar.innerHTML = 'RAW: <strong style="color:var(--text);">' + rawCount + '</strong> ➔ Output: <strong style="color:#10b981;">' + outCount + '</strong> bài' + (filtCount > 0 ? ' <span style="color:#f87171; font-weight:600;">(Lọc -' + filtCount + ')</span>' : '');
          }}

          const rulesEl = document.getElementById('cmpRulesAppliedSummary');
          const ruleList = d.rules_applied || [];
          if (rulesEl) {{
            rulesEl.innerText = ruleList.length + ' rules áp dụng';
          }}

          const updatedEl = document.getElementById('cmpUpdatedAt');
          if (updatedEl) {{
            if (d.updated_at) {{
              try {{
                const dt = new Date(d.updated_at);
                updatedEl.innerText = 'Xử lý: ' + dt.toLocaleTimeString() + ' ' + dt.toLocaleDateString();
              }} catch(e) {{
                updatedEl.innerText = 'Xử lý: ' + d.updated_at;
              }}
            }} else {{
              updatedEl.innerText = 'Vừa xử lý';
            }}
          }}

          const badgeOut = document.getElementById('cmpOutputBadge');
          if (badgeOut) badgeOut.innerText = outCount;
          const badgeRaw = document.getElementById('cmpRawBadge');
          if (badgeRaw) badgeRaw.innerText = rawCount;

          const pillsCont = document.getElementById('cmpActiveRulesPills');
          if (pillsCont) {{
            if (!ruleList || ruleList.length === 0) {{
              pillsCont.innerHTML = '<span style="font-size:0.68rem; color:var(--text-dim);">(Không áp dụng rule nào)</span>';
            }} else {{
              pillsCont.innerHTML = ruleList.map(rId => '<span style="font-size: 0.65rem; padding: 2px 7px; background: rgba(56,189,248,0.12); color: #38bdf8; border: 1px solid rgba(56,189,248,0.25); border-radius: 4px; font-family: var(--mono);">' + rId + '</span>').join('');
            }}
          }}

          const outList = document.getElementById('cmpOutputList');
          if (outList) {{
            const posts = d.output_posts || [];
            if (posts.length === 0) {{
              outList.innerHTML = '<div style="text-align:center; padding:36px; color:var(--text-dim); background:rgba(255,255,255,0.02); border-radius:8px; border:1px dashed var(--card-border);">Không có bài viết nào trong Task Output sau khi chạy Rule.</div>';
            }} else {{
              outList.innerHTML = posts.map((p, idx) => renderOutputPostItem(p, idx)).join('');
            }}
          }}

          const rawList = document.getElementById('cmpRawList');
          if (rawList) {{
            const posts = d.raw_posts || [];
            if (posts.length === 0) {{
              rawList.innerHTML = '<div style="text-align:center; padding:36px; color:var(--text-dim); background:rgba(255,255,255,0.02); border-radius:8px; border:1px dashed var(--card-border);">Không có dữ liệu RAW. Hãy bấm "Cào mới từ Web" để lấy bài viết.</div>';
            }} else {{
              rawList.innerHTML = posts.map((p, idx) => renderRawPostItem(p, idx)).join('');
            }}
          }}
        }})
        .catch(err => {{
          showToast('Lỗi tải dữ liệu output: ' + err, true);
          const outList = document.getElementById('cmpOutputList');
          if (outList) outList.innerHTML = '<div style="text-align:center; padding:20px; color:#f87171;">' + err + '</div>';
        }});
    }}

    function renderOutputPostItem(item, idx) {{
      const title = item._formatted_title || item.preview_title || item.title || '(Không có tiêu đề)';
      const link = item.url || item.link || '#';
      const dateStr = item._formatted_date || item.pubDate || item.published || (item.taken_at ? new Date(item.taken_at * 1000).toLocaleString() : '') || item.created_at || '';
      const content = item._formatted_html || item.content || item.description || item.text || item.summary || '';
      const jsonStr = encodeURIComponent(JSON.stringify(item, null, 2));

      // Extract Drive / Ebook / Enclosure info if present
      let ebookBox = '';
      const driveList = item.gdrive_links || item._extracted_links || [];
      if (item.enclosure && item.enclosure.url) {{
        ebookBox = `
          <div style="margin-top: 8px; padding: 8px 10px; background: rgba(16,185,129,0.08); border: 1px solid rgba(16,185,129,0.25); border-radius: 6px; display: flex; align-items: center; justify-content: space-between; gap: 8px; flex-wrap: wrap;">
            <div style="display: flex; align-items: center; gap: 6px; font-size: 0.73rem; color: #10b981; font-weight: 500;">
              <span>📥 Enclosure / Ebook:</span>
              <a href="${{item.enclosure.url}}" target="_blank" rel="noopener" style="color: #34d399; text-decoration: underline; word-break: break-all;">${{item.enclosure.url}}</a>
            </div>
            <a href="${{item.enclosure.url}}" target="_blank" class="btn" style="height: 24px; padding: 0 8px; font-size: 0.68rem; background: rgba(16,185,129,0.18); color: #34d399; border-color: rgba(16,185,129,0.4);">Mở Link</a>
          </div>
        `;
      }} else if (driveList.length > 0) {{
        const linksHtml = driveList.map(l => `<a href="${{l}}" target="_blank" rel="noopener" style="color: #10b981; text-decoration: underline; margin-right: 8px; font-size: 0.72rem; word-break: break-all;">📚 ${{l}}</a>`).join('');
        ebookBox = `
          <div style="margin-top: 8px; padding: 8px 10px; background: rgba(16,185,129,0.08); border: 1px solid rgba(16,185,129,0.25); border-radius: 6px; font-size: 0.73rem;">
            <div style="font-weight: 600; color: #10b981; margin-bottom: 4px;">📥 Kho Ebook / Tài liệu bóc tách:</div>
            <div>${{linksHtml}}</div>
          </div>
        `;
      }}

      const tempDiv = document.createElement('div');
      tempDiv.innerHTML = content;
      const cleanSnippet = tempDiv.textContent || tempDiv.innerText || '';
      const truncatedSnippet = cleanSnippet.length > 200 ? cleanSnippet.slice(0, 200) + '...' : cleanSnippet;

      return `
        <div style="padding: 12px; background: rgba(255,255,255,0.025); border: 1px solid var(--card-border); border-radius: 8px; transition: border-color 0.15s;">
          <div style="display: flex; align-items: flex-start; justify-content: space-between; gap: 8px;">
            <div style="flex: 1; min-width: 0;">
              <a href="${{link}}" target="_blank" rel="noopener" style="font-size: 0.85rem; font-weight: 600; color: #f8fafc; text-decoration: none; line-height: 1.4; display: inline-block;">
                ${{title}}
              </a>
              <div style="display: flex; align-items: center; gap: 8px; margin-top: 4px; font-size: 0.7rem; color: var(--text-dim); flex-wrap: wrap;">
                <span>#${{idx + 1}}</span>
                ${{dateStr ? '<span>•</span><span>' + dateStr + '</span>' : ''}}
                ${{item._applied_rules && item._applied_rules.length ? '<span>•</span><span style="color:#a855f7;">Rule: ' + item._applied_rules.join(', ') + '</span>' : ''}}
              </div>
            </div>
            <button type="button" class="btn" onclick="toggleItemJson('out_json_${{idx}}')" style="height: 24px; padding: 0 8px; font-size: 0.68rem; color: var(--text-dim);" title="Xem cấu trúc JSON của bài viết">
              JSON
            </button>
          </div>
          ${{truncatedSnippet ? '<div style="margin-top: 8px; font-size: 0.76rem; color: var(--text-muted); line-height: 1.45; word-break: break-word;">' + truncatedSnippet + '</div>' : ''}}
          ${{ebookBox}}
          <pre id="out_json_${{idx}}" style="display: none; margin-top: 10px; padding: 10px; background: #0b0f19; border: 1px solid rgba(255,255,255,0.08); border-radius: 6px; font-family: var(--mono); font-size: 0.68rem; color: #94a3b8; max-height: 220px; overflow: auto; white-space: pre-wrap; word-break: break-all;">${{decodeURIComponent(jsonStr)}}</pre>
        </div>
      `;
    }}

    function renderRawPostItem(item, idx) {{
      const title = item.preview_title || item._formatted_title || item.title || (item.text ? item.text.slice(0, 80) : '') || '(Không có tiêu đề gốc)';
      const link = item.url || item.link || '#';
      const dateStr = item._formatted_date || item.pubDate || item.published || (item.taken_at ? new Date(item.taken_at * 1000).toLocaleString() : '') || item.created_at || '';
      const content = item.text || item.description || item.content || item.summary || item._formatted_html || '';
      const jsonStr = encodeURIComponent(JSON.stringify(item, null, 2));

      const tempDiv = document.createElement('div');
      tempDiv.innerHTML = content;
      const cleanSnippet = tempDiv.textContent || tempDiv.innerText || '';
      const truncatedSnippet = cleanSnippet.length > 200 ? cleanSnippet.slice(0, 200) + '...' : cleanSnippet;

      return `
        <div style="padding: 12px; background: rgba(255,255,255,0.015); border: 1px solid rgba(255,255,255,0.05); border-radius: 8px;">
          <div style="display: flex; align-items: flex-start; justify-content: space-between; gap: 8px;">
            <div style="flex: 1; min-width: 0;">
              <a href="${{link}}" target="_blank" rel="noopener" style="font-size: 0.84rem; font-weight: 600; color: #cbd5e1; text-decoration: none; line-height: 1.4; display: inline-block;">
                ${{title}}
              </a>
              <div style="display: flex; align-items: center; gap: 8px; margin-top: 4px; font-size: 0.7rem; color: var(--text-dim); flex-wrap: wrap;">
                <span>#${{idx + 1}} (RAW)</span>
                ${{dateStr ? '<span>•</span><span>' + dateStr + '</span>' : ''}}
                <a href="${{link}}" target="_blank" rel="noopener" style="color: var(--text-dim); text-decoration: underline; max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${{link}}</a>
              </div>
            </div>
            <button type="button" class="btn" onclick="toggleItemJson('raw_json_${{idx}}')" style="height: 24px; padding: 0 8px; font-size: 0.68rem; color: var(--text-dim);" title="Xem cấu trúc JSON gốc">
              JSON
            </button>
          </div>
          ${{truncatedSnippet ? '<div style="margin-top: 8px; font-size: 0.75rem; color: var(--text-dim); line-height: 1.45; word-break: break-word;">' + truncatedSnippet + '</div>' : ''}}
          <pre id="raw_json_${{idx}}" style="display: none; margin-top: 10px; padding: 10px; background: #0b0f19; border: 1px solid rgba(255,255,255,0.08); border-radius: 6px; font-family: var(--mono); font-size: 0.68rem; color: #94a3b8; max-height: 220px; overflow: auto; white-space: pre-wrap; word-break: break-all;">${{decodeURIComponent(jsonStr)}}</pre>
        </div>
      `;
    }}

    function toggleItemJson(id) {{
      const el = document.getElementById(id);
      if (!el) return;
      el.style.display = (el.style.display === 'none' || el.style.display === '') ? 'block' : 'none';
    }}

    function reprocessFeed(slug, btn) {{
      if (btn) {{
        btn.dataset.origHtml = btn.innerHTML;
        btn.innerHTML = '<svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px; animation: spin 1s linear infinite;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><span>Đang chạy...</span>';
        btn.disabled = true;
      }}
      fetch('/api/feeds/' + slug + '/reprocess', {{ method: 'POST' }})
        .then(r => {{
          if (!r.ok) throw new Error('Không thể chạy lại rule cho ' + slug);
          return r.json();
        }})
        .then(d => {{
          showToast('⚡ Đã chạy lại Rule: ' + d.raw_count + ' ➔ ' + d.output_count + ' bài');
          if (btn && btn.dataset.origHtml) {{
            btn.innerHTML = btn.dataset.origHtml;
            btn.disabled = false;
          }}
          const modal = document.getElementById('modalCompareOutput');
          if (modal && modal.classList.contains('open') && currentCmpSlug === slug) {{
            loadCompareModalData(slug);
          }}
          setTimeout(() => location.reload(), 1000);
        }})
        .catch(e => {{
          showToast('Lỗi khi chạy lại Rule: ' + e, true);
          if (btn && btn.dataset.origHtml) {{
            btn.innerHTML = btn.dataset.origHtml;
            btn.disabled = false;
          }}
        }});
    }}

    function reprocessAllFeeds(btn) {{
      if (btn) {{
        btn.dataset.origHtml = btn.innerHTML;
        btn.innerHTML = '<svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px; animation: spin 1s linear infinite;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><span>Đang chạy lại toàn bộ...</span>';
        btn.disabled = true;
      }}
      fetch('/api/reprocess-all', {{ method: 'POST' }})
        .then(r => {{
          if (!r.ok) throw new Error('Không thể chạy lại toàn bộ rules');
          return r.json();
        }})
        .then(d => {{
          showToast('⚡ Đã hoàn tất chạy lại Rule cho toàn bộ ' + (d.feeds ? d.feeds.length : '') + ' kênh!');
          setTimeout(() => location.reload(), 1000);
        }})
        .catch(e => {{
          showToast('Lỗi khi chạy lại toàn bộ: ' + e, true);
          if (btn && btn.dataset.origHtml) {{
            btn.innerHTML = btn.dataset.origHtml;
            btn.disabled = false;
          }}
        }});
    }}

    function reprocessModalFeed() {{
      if (!currentCmpSlug) return;
      const btn = document.getElementById('btnCmpReprocess');
      reprocessFeed(currentCmpSlug, btn);
    }}

    function scrapeModalFeed() {{
      if (!currentCmpSlug) return;
      const btn = document.getElementById('btnCmpScrape');
      if (btn) {{
        btn.dataset.origHtml = btn.innerHTML;
        btn.innerHTML = '<svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px; animation: spin 1s linear infinite;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><span>Đang cào...</span>';
        btn.disabled = true;
      }}
      fetch('/api/refresh/' + currentCmpSlug)
        .then(r => {{
          if (!r.ok) throw new Error('Cào mới thất bại');
          return r.json();
        }})
        .then(d => {{
          showToast('Đã cào mới thành công: ' + (d.output_count !== undefined ? d.raw_count + ' ➔ ' + d.output_count : d.count) + ' bài viết');
          if (btn && btn.dataset.origHtml) {{
            btn.innerHTML = btn.dataset.origHtml;
            btn.disabled = false;
          }}
          loadCompareModalData(currentCmpSlug);
        }})
        .catch(e => {{
          showToast('Lỗi khi cào mới: ' + e, true);
          if (btn && btn.dataset.origHtml) {{
            btn.innerHTML = btn.dataset.origHtml;
            btn.disabled = false;
          }}
        }});
    }}

    // DASHBOARD CONTENT EXPLORER (TASK OUTPUT & SCRAPED LIST)
    let dashAllOutputPosts = [];
    let dashAllRawPosts = [];
    let currentDashViewMode = 'split';
    let currentDashFeedFilter = 'all';
    let currentDashSearchQuery = '';

    function setDashboardViewMode(mode) {{
      currentDashViewMode = mode;
      const btnSplit = document.getElementById('btnDashViewSplit');
      const btnOut = document.getElementById('btnDashViewOutput');
      const btnScraped = document.getElementById('btnDashViewScraped');
      const grid = document.getElementById('dashPostsGrid');
      const cardOut = document.getElementById('dashCardOutput');
      const cardScraped = document.getElementById('dashCardScraped');

      if (!grid || !cardOut || !cardScraped) return;

      if (btnSplit) btnSplit.classList.toggle('active', mode === 'split');
      if (btnOut) btnOut.classList.toggle('active', mode === 'output');
      if (btnScraped) btnScraped.classList.toggle('active', mode === 'scraped');

      if (mode === 'split') {{
        grid.style.gridTemplateColumns = (window.innerWidth < 992) ? '1fr' : '1fr 1fr';
        cardOut.style.display = 'flex';
        cardScraped.style.display = 'flex';
      }} else if (mode === 'output') {{
        grid.style.gridTemplateColumns = '1fr';
        cardOut.style.display = 'flex';
        cardScraped.style.display = 'none';
      }} else if (mode === 'scraped') {{
        grid.style.gridTemplateColumns = '1fr';
        cardOut.style.display = 'none';
        cardScraped.style.display = 'flex';
      }}
    }}

    function onDashFeedFilterChange(val) {{
      currentDashFeedFilter = val;
      const sel = document.getElementById('dashFeedFilter');
      if (sel && sel.value !== val) sel.value = val;
      renderDashboardPostsLists();
    }}

    function viewFeedPostsOnDashboard(slug) {{
      onDashFeedFilterChange(slug);
      const explorer = document.getElementById('dashboardExplorer');
      if (explorer) {{
        explorer.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
      }}
    }}

    function onDashPostSearch(val) {{
      currentDashSearchQuery = (val || '').toLowerCase().trim();
      renderDashboardPostsLists();
    }}

    function reloadDashboardPosts() {{
      const outList = document.getElementById('dashOutputList');
      const scrapedList = document.getElementById('dashScrapedList');
      if (outList) outList.innerHTML = '<div style="text-align:center; padding:30px; color:var(--text-dim);"><svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 20px; height: 20px; animation: spin 1s linear infinite; display: inline-block; margin-bottom: 8px;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><br>Đang tải Task Output...</div>';
      if (scrapedList) scrapedList.innerHTML = '<div style="text-align:center; padding:30px; color:var(--text-dim);"><svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 20px; height: 20px; animation: spin 1s linear infinite; display: inline-block; margin-bottom: 8px;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><br>Đang tải dữ liệu scrape được...</div>';

      fetch('/api/dashboard/posts?slug=all')
        .then(r => {{
          if (!r.ok) throw new Error('Không thể tải dữ liệu dashboard posts');
          return r.json();
        }})
        .then(d => {{
          dashAllOutputPosts = d.output_posts || [];
          dashAllRawPosts = d.raw_posts || [];

          const sel = document.getElementById('dashFeedFilter');
          if (sel && sel.options.length <= 1 && d.feeds_summary) {{
            d.feeds_summary.forEach(f => {{
              const opt = document.createElement('option');
              opt.value = f.slug;
              opt.innerText = '[' + f.category + '] ' + f.title;
              sel.appendChild(opt);
            }});
          }}

          const totalOut = dashAllOutputPosts.length;
          const totalRaw = dashAllRawPosts.length;
          const statBadge = document.getElementById('dashTotalStatsBadge');
          if (statBadge) statBadge.innerText = totalOut + ' output / ' + totalRaw + ' raw';

          const outBadge = document.getElementById('dashOutputBadge');
          if (outBadge) outBadge.innerText = totalOut;
          const scBadge = document.getElementById('dashScrapedBadge');
          if (scBadge) scBadge.innerText = totalRaw;

          renderDashboardPostsLists();
        }})
        .catch(err => {{
          console.error(err);
          if (outList) outList.innerHTML = '<div style="text-align:center; padding:20px; color:#f87171;">Lỗi tải dữ liệu: ' + err + '</div>';
          if (scrapedList) scrapedList.innerHTML = '<div style="text-align:center; padding:20px; color:#f87171;">Lỗi tải dữ liệu: ' + err + '</div>';
        }});
    }}

    function renderDashboardPostsLists() {{
      const q = currentDashSearchQuery;
      const feed = currentDashFeedFilter;

      let filteredOutput = dashAllOutputPosts.filter(p => {{
        if (feed !== 'all' && p._feed_slug !== feed) return false;
        if (!q) return true;
        const text = (p._formatted_title || p.title || p.preview_title || '') + ' ' + (p._formatted_html || p.text || p.content || '') + ' ' + (p._feed_title || '') + ' ' + (p.url || '');
        return text.toLowerCase().includes(q);
      }});

      let filteredRaw = dashAllRawPosts.filter(p => {{
        if (feed !== 'all' && p._feed_slug !== feed) return false;
        if (!q) return true;
        const text = (p.preview_title || p.title || p.text || '') + ' ' + (p.url || '') + ' ' + (p._feed_title || '');
        return text.toLowerCase().includes(q);
      }});

      const hOut = document.getElementById('dashCardOutputHeaderCount');
      if (hOut) hOut.innerText = filteredOutput.length + ' bài' + (feed !== 'all' ? ' (Lọc)' : '');
      const hRaw = document.getElementById('dashCardScrapedHeaderCount');
      if (hRaw) hRaw.innerText = filteredRaw.length + ' bài' + (feed !== 'all' ? ' (Lọc)' : '');

      const sOut = document.getElementById('dashOutputFilterStatus');
      if (sOut) sOut.innerText = (feed === 'all' ? 'Tất cả kênh' : feed) + (q ? ' • Tìm: "' + q + '"' : '');
      const sRaw = document.getElementById('dashScrapedFilterStatus');
      if (sRaw) sRaw.innerText = (feed === 'all' ? 'Tất cả kênh' : feed) + (q ? ' • Tìm: "' + q + '"' : '');

      const outList = document.getElementById('dashOutputList');
      if (outList) {{
        if (filteredOutput.length === 0) {{
          outList.innerHTML = '<div style="text-align:center; padding:32px; color:var(--text-dim); background:rgba(255,255,255,0.02); border-radius:8px; border:1px dashed var(--card-border);">Không có bài viết output nào phù hợp với bộ lọc.</div>';
        }} else {{
          outList.innerHTML = filteredOutput.map((item, idx) => renderDashPostItem(item, idx, 'output')).join('');
        }}
      }}

      const scList = document.getElementById('dashScrapedList');
      if (scList) {{
        if (filteredRaw.length === 0) {{
          scList.innerHTML = '<div style="text-align:center; padding:32px; color:var(--text-dim); background:rgba(255,255,255,0.02); border-radius:8px; border:1px dashed var(--card-border);">Không có bài viết cào nào phù hợp với bộ lọc.</div>';
        }} else {{
          scList.innerHTML = filteredRaw.map((item, idx) => renderDashPostItem(item, idx, 'raw')).join('');
        }}
      }}
    }}

    function renderDashPostItem(item, idx, type) {{
      const isOut = type === 'output';
      const feedSlug = item._feed_slug || '';
      const feedTitle = item._feed_title || feedSlug;
      const feedCat = item._feed_category || 'Chung';

      const title = isOut
        ? (item._formatted_title || item.preview_title || item.title || '(Không có tiêu đề)')
        : (item.preview_title || item._formatted_title || item.title || (item.text ? item.text.slice(0, 80) : '') || '(Không có tiêu đề gốc)');

      const link = item.url || item.link || '#';
      const dateStr = item._formatted_date || item.pubDate || item.published || (item.taken_at ? new Date(item.taken_at * 1000).toLocaleString() : '') || item.created_at || '';
      const content = isOut
        ? (item._formatted_html || item.content || item.description || item.text || item.summary || '')
        : (item.text || item.description || item.content || item.summary || item._formatted_html || '');

      const jsonId = 'dash_' + type + '_' + idx;
      const jsonStr = encodeURIComponent(JSON.stringify(item, null, 2));

      let ebookBox = '';
      const driveList = item.gdrive_links || item._extracted_links || [];
      if (isOut) {{
        if (item.enclosure && item.enclosure.url) {{
          ebookBox = `
            <div style="margin-top: 8px; padding: 6px 8px; background: rgba(16,185,129,0.08); border: 1px solid rgba(16,185,129,0.25); border-radius: 6px; display: flex; align-items: center; justify-content: space-between; gap: 6px; flex-wrap: wrap;">
              <div style="display: flex; align-items: center; gap: 4px; font-size: 0.72rem; color: #10b981; font-weight: 500;">
                <span>📥 Enclosure:</span>
                <a href="${{item.enclosure.url}}" target="_blank" rel="noopener" style="color: #34d399; text-decoration: underline; word-break: break-all;">${{item.enclosure.url}}</a>
              </div>
              <a href="${{item.enclosure.url}}" target="_blank" class="btn" style="height: 22px; padding: 0 6px; font-size: 0.65rem; background: rgba(16,185,129,0.18); color: #34d399; border-color: rgba(16,185,129,0.4);">Mở Link</a>
            </div>
          `;
        }} else if (driveList.length > 0) {{
          const linksHtml = driveList.map(l => `<a href="${{l}}" target="_blank" rel="noopener" style="color: #10b981; text-decoration: underline; margin-right: 6px; font-size: 0.7rem; word-break: break-all;">📚 ${{l}}</a>`).join('');
          ebookBox = `
            <div style="margin-top: 6px; padding: 6px 8px; background: rgba(16,185,129,0.08); border: 1px solid rgba(16,185,129,0.25); border-radius: 6px; font-size: 0.72rem;">
              <div style="font-weight: 600; color: #10b981; margin-bottom: 3px;">📥 Kho Ebook / Tài liệu:</div>
              <div>${{linksHtml}}</div>
            </div>
          `;
        }}
      }}

      const tempDiv = document.createElement('div');
      tempDiv.innerHTML = content;
      const cleanSnippet = tempDiv.textContent || tempDiv.innerText || '';
      const truncatedSnippet = cleanSnippet.length > 160 ? cleanSnippet.slice(0, 160) + '...' : cleanSnippet;

      return `
        <div style="padding: 10px 12px; background: rgba(255,255,255,0.02); border: 1px solid var(--card-border); border-radius: 8px; transition: border-color 0.15s;">
          <div style="display: flex; align-items: flex-start; justify-content: space-between; gap: 8px;">
            <div style="flex: 1; min-width: 0;">
              <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 3px; flex-wrap: wrap;">
                <span class="badge gray" style="font-size: 0.65rem; cursor: pointer;" onclick="onDashFeedFilterChange('${{feedSlug}}')" title="Lọc theo kênh này">${{feedTitle}}</span>
                <span class="badge blue" style="font-size: 0.62rem;">${{feedCat}}</span>
                ${{isOut && item._applied_rules && item._applied_rules.length ? '<span style="font-size:0.65rem; color:#a855f7;">⚡ ' + item._applied_rules.join(', ') + '</span>' : ''}}
              </div>
              <a href="${{link}}" target="_blank" rel="noopener" style="font-size: 0.82rem; font-weight: 600; color: #f8fafc; text-decoration: none; line-height: 1.35; display: inline-block;">
                ${{title}}
              </a>
              <div style="display: flex; align-items: center; gap: 8px; margin-top: 3px; font-size: 0.68rem; color: var(--text-dim); flex-wrap: wrap;">
                <span>#${{idx + 1}}</span>
                ${{dateStr ? '<span>•</span><span>' + dateStr + '</span>' : ''}}
                <a href="${{link}}" target="_blank" rel="noopener" style="color: var(--text-dim); text-decoration: underline; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${{link}}</a>
              </div>
            </div>
            <button type="button" class="btn" onclick="toggleItemJson('${{jsonId}}')" style="height: 22px; padding: 0 7px; font-size: 0.65rem; color: var(--text-dim);" title="Xem cấu trúc JSON">
              JSON
            </button>
          </div>
          ${{truncatedSnippet ? '<div style="margin-top: 6px; font-size: 0.74rem; color: var(--text-muted); line-height: 1.4; word-break: break-word;">' + truncatedSnippet + '</div>' : ''}}
          ${{ebookBox}}
          <pre id="${{jsonId}}" style="display: none; margin-top: 8px; padding: 8px; background: #0b0f19; border: 1px solid rgba(255,255,255,0.08); border-radius: 6px; font-family: var(--mono); font-size: 0.66rem; color: #94a3b8; max-height: 200px; overflow: auto; white-space: pre-wrap; word-break: break-all;">${{decodeURIComponent(jsonStr)}}</pre>
        </div>
      `;
    }}

    function toggleFeedPreview(slug) {{
      const drawer = document.getElementById('drawerPreview_' + slug);
      const chevron = document.getElementById('chevronPreview_' + slug);
      if (!drawer) return;
      const isHidden = drawer.style.display === 'none' || drawer.style.display === '';
      if (isHidden) {{
        drawer.style.display = 'block';
        if (chevron) chevron.style.transform = 'rotate(180deg)';
      }} else {{
        drawer.style.display = 'none';
        if (chevron) chevron.style.transform = 'rotate(0deg)';
      }}
    }}

    function viewFeedInScrapers(slug) {{
      switchMainTab('scrapers');
      setTimeout(() => {{
        const card = document.getElementById('feedCard_' + slug);
        if (card) card.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
        const drawer = document.getElementById('drawerPreview_' + slug);
        if (drawer && (drawer.style.display === 'none' || drawer.style.display === '')) {{
          toggleFeedPreview(slug);
        }}
      }}, 120);
    }}

    function openAddFeedModal() {{
      const form = document.getElementById('formAddFeed');
      if (form) form.reset();
      const origSlug = document.getElementById('feedOriginalSlug');
      if (origSlug) origSlug.value = '';
      const slugInput = document.getElementById('feedSlug');
      if (slugInput) delete slugInput.dataset.manual;

      document.getElementById('feedType').value = 'web';
      handleTypeChange('web');
      document.getElementById('feedCookieMode').value = 'none';
      handleCookieModeChange('none');

      if (document.getElementById('selItem')) document.getElementById('selItem').value = '';
      if (document.getElementById('selTitle')) document.getElementById('selTitle').value = '';
      if (document.getElementById('selLink')) document.getElementById('selLink').value = '';
      if (document.getElementById('selDesc')) document.getElementById('selDesc').value = '';
      if (document.getElementById('chkUseFlareSolverr')) document.getElementById('chkUseFlareSolverr').checked = false;
      if (document.getElementById('chkSaveToVault')) document.getElementById('chkSaveToVault').checked = false;
      if (document.getElementById('wrapProfileName')) document.getElementById('wrapProfileName').style.display = 'none';

      // Reset Custom Output & Filters
      if (document.getElementById('outTitleTemplate')) document.getElementById('outTitleTemplate').value = '';
      if (document.getElementById('outContentMode')) document.getElementById('outContentMode').value = 'full';
      if (document.getElementById('outLimit')) document.getElementById('outLimit').value = '';
      if (document.getElementById('outIncludeImages')) document.getElementById('outIncludeImages').checked = true;
      if (document.getElementById('outIncludeSourceLink')) document.getElementById('outIncludeSourceLink').checked = true;
      if (document.getElementById('outIncludeEnclosures')) document.getElementById('outIncludeEnclosures').checked = true;
      if (document.getElementById('outFilterInclude')) document.getElementById('outFilterInclude').value = '';
      if (document.getElementById('outFilterExclude')) document.getElementById('outFilterExclude').value = '';
      if (document.getElementById('feedInterval')) document.getElementById('feedInterval').value = '30';
      updateFormPresetHighlight(30);

      const title = document.getElementById('modalAddTitle');
      if (title) title.innerText = 'Tạo Kênh RSS Mới (Universal Feed Generator)';
      const btnText = document.getElementById('btnSubmitFeedText');
      if (btnText) btnText.innerText = 'Lưu & Kích hoạt Feed';

      populateProfileDropdown();
      populateFeedRulesCheckboxes([]);
      openModal('modalAdd');
    }}

    function editFeedModal(slug) {{
      fetch('/api/feeds/' + slug)
        .then(r => {{
          if (!r.ok) throw new Error('Không thể tải thông tin kênh feed');
          return r.json();
        }})
        .then(f => {{
          const form = document.getElementById('formAddFeed');
          if (form) form.reset();

          const origSlug = document.getElementById('feedOriginalSlug');
          if (origSlug) origSlug.value = f.slug;

          document.getElementById('feedType').value = f.type || 'web';
          handleTypeChange(f.type || 'web');

          document.getElementById('feedTarget').value = f.target || '';
          document.getElementById('feedTitle').value = f.title || '';

          const slugInput = document.getElementById('feedSlug');
          slugInput.value = f.slug;
          slugInput.dataset.manual = "true";

          document.getElementById('feedCategory').value = f.category || '';
          document.getElementById('feedDesc').value = f.description || '';

          if (f.selectors) {{
            if (document.getElementById('selItem')) document.getElementById('selItem').value = f.selectors.item_selector || '';
            if (document.getElementById('selTitle')) document.getElementById('selTitle').value = f.selectors.title_selector || '';
            if (document.getElementById('selLink')) document.getElementById('selLink').value = f.selectors.link_selector || '';
            if (document.getElementById('selDesc')) document.getElementById('selDesc').value = f.selectors.desc_selector || '';
          }} else {{
            if (document.getElementById('selItem')) document.getElementById('selItem').value = '';
            if (document.getElementById('selTitle')) document.getElementById('selTitle').value = '';
            if (document.getElementById('selLink')) document.getElementById('selLink').value = '';
            if (document.getElementById('selDesc')) document.getElementById('selDesc').value = '';
          }}

          if (document.getElementById('chkUseFlareSolverr')) {{
            document.getElementById('chkUseFlareSolverr').checked = Boolean(f.use_flaresolverr);
          }}

          const cookieMode = f.cookie_mode || 'none';
          document.getElementById('feedCookieMode').value = cookieMode;
          handleCookieModeChange(cookieMode);

          // Populate cookie profiles and select f.cookie_profile
          fetch('/api/cookies')
            .then(r => r.json())
            .then(profiles => {{
              const sel = document.getElementById('feedCookieProfile');
              sel.innerHTML = '';
              if (!profiles || profiles.length === 0) {{
                sel.innerHTML = '<option value="">(Chưa có bộ cookie nào trong kho - Bấm "Quản lý Kho Cookie" để thêm)</option>';
              }} else {{
                profiles.forEach(p => {{
                  const opt = document.createElement('option');
                  opt.value = p.id;
                  opt.innerText = '[' + (p.website || 'web') + '] ' + p.name + ' (' + p.masked + ')';
                  if (p.id === f.cookie_profile) opt.selected = true;
                  sel.appendChild(opt);
                }});
              }}
            }});

          document.getElementById('feedCookieCustom').value = f.cookie || '';
          if (document.getElementById('chkSaveToVault')) document.getElementById('chkSaveToVault').checked = false;
          if (document.getElementById('wrapProfileName')) document.getElementById('wrapProfileName').style.display = 'none';

          // Pre-populate Custom Output & Filters
          const cOut = f.custom_output || {{}};
          if (document.getElementById('outTitleTemplate')) document.getElementById('outTitleTemplate').value = cOut.title_template || '';
          if (document.getElementById('outContentMode')) document.getElementById('outContentMode').value = cOut.content_mode || 'full';
          if (document.getElementById('outLimit')) document.getElementById('outLimit').value = (cOut.limit !== null && cOut.limit !== undefined) ? cOut.limit : '';
          if (document.getElementById('outIncludeImages')) document.getElementById('outIncludeImages').checked = cOut.include_images !== false;
          if (document.getElementById('outIncludeSourceLink')) document.getElementById('outIncludeSourceLink').checked = cOut.include_source_link !== false;
          if (document.getElementById('outIncludeEnclosures')) document.getElementById('outIncludeEnclosures').checked = cOut.include_enclosures !== false;
          if (document.getElementById('outFilterInclude')) document.getElementById('outFilterInclude').value = cOut.filter_include || '';
          if (document.getElementById('outFilterExclude')) document.getElementById('outFilterExclude').value = cOut.filter_exclude || '';
          const feedIntervalVal = parseInt(f.interval_minutes) || 30;
          if (document.getElementById('feedInterval')) document.getElementById('feedInterval').value = String(feedIntervalVal);
          updateFormPresetHighlight(feedIntervalVal);

          populateFeedRulesCheckboxes(f.applied_rules || []);

          const title = document.getElementById('modalAddTitle');
          if (title) title.innerText = 'Cấu hình Kênh Scraper: ' + (f.title || f.slug);
          const btnText = document.getElementById('btnSubmitFeedText');
          if (btnText) btnText.innerText = 'Cập nhật Cấu hình';

          openModal('modalAdd');
        }})
        .catch(err => showToast('Lỗi: ' + err, true));
    }}

    function openModal(id) {{
      const m = document.getElementById(id);
      if (m) {{
        m.classList.add('open');
        if (id === 'modalAdd') populateProfileDropdown();
      }}
    }}

    function closeModal(id) {{
      const m = document.getElementById(id);
      if (m) m.classList.remove('open');
    }}

    function handleModalClick(e, id) {{
      if (e.target.id === id) closeModal(id);
    }}

    function autoGenerateSlug(title) {{
      const slugInput = document.getElementById('feedSlug');
      if (!slugInput.dataset.manual) {{
        let s = title.toLowerCase()
          .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
          .replace(/[^a-z0-9]/g, '-')
          .replace(/-+/g, '-')
          .replace(/^-|-$/g, '');
        slugInput.value = s;
      }}
    }}

    document.getElementById('feedSlug').addEventListener('input', function() {{
      this.dataset.manual = "true";
    }});

    function autoGuessWebsite(val) {{
      const vw = document.getElementById('vaultWebsite');
      const feedType = document.getElementById('feedType').value;
      if (feedType === 'threads') {{
        if (vw) vw.value = 'threads.net';
      }} else if (val.startsWith('http://') || val.startsWith('https://')) {{
        try {{
          const u = new URL(val);
          if (vw) vw.value = u.hostname.replace(/^www\./, '');
          const titleInp = document.getElementById('feedTitle');
          if (!titleInp.value) {{
            let hostParts = u.hostname.replace(/^www\./, '').split('.');
            let siteName = hostParts[0].charAt(0).toUpperCase() + hostParts[0].slice(1);
            let pathParts = u.pathname.split('/').filter(Boolean);
            let sub = pathParts.length > 0 ? ' - ' + pathParts[pathParts.length - 1].replace(/[-_.]/g, ' ') : '';
            titleInp.value = siteName + sub;
            autoGenerateSlug(titleInp.value);
          }}
        }} catch(e) {{}}
      }}
    }}

    function handleTypeChange(val) {{
      const lbl = document.getElementById('lblTarget');
      const hint = document.getElementById('hintTarget');
      const inp = document.getElementById('feedTarget');
      const vw = document.getElementById('vaultWebsite');
      const hintEngine = document.getElementById('hintEngine');
      const secWeb = document.getElementById('sectionWebAdvanced');

      if (val === 'web') {{
        lbl.innerText = 'Địa chỉ URL Trang web cần cào *';
        inp.placeholder = 'vd: https://tuoitre.vn/cong-nghe.htm, https://voz.vn/f/chuyen-tro-linh-tinh.17/';
        hint.innerText = 'Nhập URL trang web. Hệ thống tự động phân tích HTML và bóc tách các bài viết mới thành RSS.';
        if (hintEngine) hintEngine.innerText = 'Biến bất kỳ website, diễn đàn (Voz, Tinhte...), báo chí hay blog nào thành RSS chuẩn hóa.';
        if (secWeb) secWeb.style.display = 'block';
        if (inp.value) autoGuessWebsite(inp.value);
      }} else if (val === 'threads') {{
        lbl.innerText = 'Hashtag, Từ khóa hoặc Profile Threads *';
        inp.placeholder = 'vd: bookthreads, congnghe, @nguyetminh6080';
        hint.innerText = 'Cào các bài đăng mới nhất trên Threads.net theo từ khóa, hashtag hoặc trang cá nhân.';
        if (hintEngine) hintEngine.innerText = 'Cào chuyên biệt từ mạng xã hội Threads (hỗ trợ hashtag, tài khoản và lọc link Ebook Drive).';
        if (secWeb) secWeb.style.display = 'none';
        if (vw) vw.value = 'threads.net';
      }} else if (val === 'rsshub') {{
        lbl.innerText = 'Tuyến đường Route RSSHub Upstream *';
        inp.placeholder = 'vd: telegram/channel/duongdancity, bilibili/ranking/0/3';
        hint.innerText = 'Nhập tuyến đường RSSHub được hỗ trợ bởi hệ sinh thái RSSHub.';
        if (hintEngine) hintEngine.innerText = 'Tận dụng hơn 1000+ tuyến đường tích hợp sẵn của RSSHub upstream.';
        if (secWeb) secWeb.style.display = 'none';
        if (vw) vw.value = 'rsshub';
      }} else if (val === 'custom_rss') {{
        lbl.innerText = 'Địa chỉ URL Feed RSS / Atom ngoài *';
        inp.placeholder = 'vd: https://vnexpress.net/rss/tin-moi-nhat.rss';
        hint.innerText = 'Nhập địa chỉ URL RSS/Atom của bất kỳ website hoặc báo chí nào để quản lý tập trung.';
        if (hintEngine) hintEngine.innerText = 'Đồng bộ và quản lý tập trung một URL RSS/Atom có sẵn.';
        if (secWeb) secWeb.style.display = 'none';
        if (inp.value) autoGuessWebsite(inp.value);
      }}
    }}

    function handleCookieModeChange(val) {{
      const grpProfile = document.getElementById('groupCookieProfile');
      const grpCustom = document.getElementById('groupCookieCustom');
      const hintCookie = document.getElementById('hintCookieMode');

      grpProfile.style.display = (val === 'profile') ? 'block' : 'none';
      grpCustom.style.display = (val === 'custom') ? 'block' : 'none';

      if (val === 'profile') {{
        hintCookie.innerText = 'Scraper sẽ gửi kèm bộ Cookie của trang web đã chọn trong Kho.';
      }} else if (val === 'custom') {{
        hintCookie.innerText = 'Scraper chỉ sử dụng chuỗi Cookie riêng biệt nhập bên dưới.';
      }} else if (val === 'none') {{
        hintCookie.innerText = 'Scraper sẽ truy cập công khai mà không gửi cookie xác thực.';
      }}
    }}

    function populateProfileDropdown() {{
      fetch('/api/profiles')
        .then(r => r.json())
        .then(profiles => {{
          const sel = document.getElementById('feedCookieProfile');
          if (!sel) return;
          sel.innerHTML = '';
          if (!profiles || profiles.length === 0) {{
            sel.innerHTML = '<option value="">(Chưa có Profile nào - Bấm tab "Quản lý Profile" để thêm)</option>';
            return;
          }}
          profiles.forEach(p => {{
            const opt = document.createElement('option');
            opt.value = p.id;
            const dom = p.domain || p.website || 'web';
            const auth = p.auth_type ? (' - ' + p.auth_type) : '';
            opt.innerText = '[' + dom + '] ' + p.name + auth + ' (' + p.masked + ')';
            sel.appendChild(opt);
          }});
        }})
        .catch(err => console.error('Error loading profiles:', err));
    }}

    function handleAuthTypeChange(val) {{
      const gCookie = document.getElementById('groupAuthCookie');
      const gLogin = document.getElementById('groupAuthLogin');
      const gApi = document.getElementById('groupAuthApiKey');
      const gHeaders = document.getElementById('groupAuthHeaders');

      if (gCookie) gCookie.style.display = (val === 'cookie') ? 'block' : 'none';
      if (gLogin) gLogin.style.display = (val === 'login') ? 'block' : 'none';
      if (gApi) gApi.style.display = (val === 'api_key') ? 'block' : 'none';
      if (gHeaders) gHeaders.style.display = (val === 'custom_header') ? 'block' : 'none';
    }}

    function openAddProfileModal() {{
      const form = document.getElementById('formProfileEdit');
      if (form) form.reset();
      const idEl = document.getElementById('editProfileId');
      if (idEl) idEl.value = '';
      const titleEl = document.getElementById('lblProfileModalTitle');
      if (titleEl) titleEl.innerText = 'Thêm Profile Xác thực Mới';
      const selAuth = document.getElementById('profAuthType');
      if (selAuth) {{
        selAuth.value = 'cookie';
        handleAuthTypeChange('cookie');
      }}
      openModal('modalProfileEdit');
    }}

    function openAddProfileForDomain(domain) {{
      openAddProfileModal();
      const domEl = document.getElementById('profDomain');
      if (domEl) domEl.value = domain;
      const nameEl = document.getElementById('profName');
      if (nameEl) nameEl.value = 'Profile ' + domain;
    }}

    function openEditProfileModal(id) {{
      fetch('/api/profiles/' + id)
        .then(r => {{
          if (!r.ok) throw new Error('Không thể tải profile ' + id);
          return r.json();
        }})
        .then(p => {{
          const form = document.getElementById('formProfileEdit');
          if (form) form.reset();

          const idEl = document.getElementById('editProfileId');
          if (idEl) idEl.value = p.id || id;

          const domEl = document.getElementById('profDomain');
          if (domEl) domEl.value = p.domain || p.website || '';

          const nameEl = document.getElementById('profName');
          if (nameEl) nameEl.value = p.name || '';

          const authType = p.auth_type || 'cookie';
          const selAuth = document.getElementById('profAuthType');
          if (selAuth) {{
            selAuth.value = authType;
            handleAuthTypeChange(authType);
          }}

          if (document.getElementById('profCookie')) {{
            document.getElementById('profCookie').value = p.cookie || '';
          }}
          if (document.getElementById('profUsername')) {{
            document.getElementById('profUsername').value = p.username || '';
          }}
          if (document.getElementById('profPassword')) {{
            document.getElementById('profPassword').value = '';
            document.getElementById('profPassword').placeholder = '(Để trống nếu giữ nguyên mật khẩu cũ)';
          }}
          if (document.getElementById('profHeaderName')) {{
            document.getElementById('profHeaderName').value = p.header_name || 'Authorization';
          }}
          if (document.getElementById('profApiKey')) {{
            document.getElementById('profApiKey').value = p.api_key || '';
          }}
          if (document.getElementById('profCustomHeaders')) {{
            document.getElementById('profCustomHeaders').value = p.custom_headers ? JSON.stringify(p.custom_headers, null, 2) : '';
          }}
          if (document.getElementById('profDesc')) {{
            document.getElementById('profDesc').value = p.description || '';
          }}

          const titleEl = document.getElementById('lblProfileModalTitle');
          if (titleEl) titleEl.innerText = 'Chỉnh sửa Profile: ' + (p.name || id);

          openModal('modalProfileEdit');
        }})
        .catch(err => showToast('Lỗi: ' + err, true));
    }}

    function handleSaveProfile(e) {{
      e.preventDefault();
      const btn = document.getElementById('btnSubmitProfile');
      if (btn) {{
        btn.disabled = true;
        btn.innerText = 'Đang lưu...';
      }}

      const id = document.getElementById('editProfileId').value.trim();
      const domain = document.getElementById('profDomain').value.trim().toLowerCase();
      const name = document.getElementById('profName').value.trim();
      const authType = document.getElementById('profAuthType').value;
      const desc = document.getElementById('profDesc') ? document.getElementById('profDesc').value.trim() : '';

      let customHeaders = {{}};
      const rawHdrs = document.getElementById('profCustomHeaders') ? document.getElementById('profCustomHeaders').value.trim() : '';
      if (rawHdrs) {{
        try {{
          customHeaders = JSON.parse(rawHdrs);
        }} catch(err) {{
          showToast('Custom Headers JSON không hợp lệ!', true);
          if (btn) {{ btn.disabled = false; btn.innerText = 'Lưu Profile'; }}
          return;
        }}
      }}

      const payload = {{
        id: id,
        name: name,
        domain: domain,
        website: domain,
        auth_type: authType,
        cookie: document.getElementById('profCookie') ? document.getElementById('profCookie').value.trim() : '',
        username: document.getElementById('profUsername') ? document.getElementById('profUsername').value.trim() : '',
        password: document.getElementById('profPassword') ? document.getElementById('profPassword').value : '',
        header_name: document.getElementById('profHeaderName') ? document.getElementById('profHeaderName').value.trim() : 'Authorization',
        api_key: document.getElementById('profApiKey') ? document.getElementById('profApiKey').value.trim() : '',
        custom_headers: customHeaders,
        description: desc
      }};

      fetch('/api/profiles', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload)
      }})
      .then(r => {{
        if (!r.ok) return r.json().then(e => Promise.reject(e.detail || 'Lỗi server'));
        return r.json();
      }})
      .then(d => {{
        showToast('Đã lưu thành công Profile [' + payload.name + ']!');
        closeModal('modalProfileEdit');
        setTimeout(() => location.reload(), 600);
      }})
      .catch(err => {{
        showToast('Lỗi: ' + err, true);
        if (btn) {{ btn.disabled = false; btn.innerText = 'Lưu Profile'; }}
      }});
    }}

    function deleteProfileModal(id) {{
      if (!confirm('Bạn có chắc chắn muốn xóa Profile [' + id + '] không?')) return;
      fetch('/api/profiles/' + id, {{ method: 'DELETE' }})
        .then(r => {{
          if (!r.ok) return r.json().then(e => Promise.reject(e.detail || 'Lỗi'));
          return r.json();
        }})
        .then(d => {{
          showToast('Đã xóa thành công profile [' + id + ']');
          setTimeout(() => location.reload(), 600);
        }})
        .catch(err => showToast('Lỗi khi xóa: ' + err, true));
    }}

    function copyProfileCredential(id) {{
      fetch('/api/profiles/' + id)
        .then(r => r.json())
        .then(p => {{
          let secret = '';
          if (p.auth_type === 'cookie') secret = p.cookie || '';
          else if (p.auth_type === 'login') secret = (p.username || '') + (p.password ? (':' + p.password) : '');
          else if (p.auth_type === 'api_key') secret = (p.header_name || 'Authorization') + ': ' + (p.api_key || '');
          else if (p.auth_type === 'custom_header') secret = JSON.stringify(p.custom_headers || {{}});
          
          if (!secret) {{
            showToast('Profile không có chuỗi xác thực để sao chép', true);
            return;
          }}
          navigator.clipboard.writeText(secret).then(() => {{
            showToast('Đã chép thông tin xác thực của profile [' + (p.name || id) + ']!');
          }}).catch(() => {{
            showToast('Không thể sao chép thông tin!', true);
          }});
        }})
        .catch(err => showToast('Lỗi: ' + err, true));
    }}

    function openCookieVaultModal() {{
      switchMainTab('profiles');
    }}

    function handleSaveFeed(e) {{
      e.preventDefault();
      const btn = document.getElementById('btnSubmitFeed');
      btn.disabled = true;
      btn.innerHTML = '<svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:14px; height:14px; animation:spin 1s linear infinite;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><span>Đang lưu...</span>';

      const selItem = document.getElementById('selItem') ? document.getElementById('selItem').value.trim() : '';
      let selectors = null;
      if (selItem) {{
        selectors = {{
          item_selector: selItem,
          title_selector: document.getElementById('selTitle') ? document.getElementById('selTitle').value.trim() : '',
          link_selector: document.getElementById('selLink') ? document.getElementById('selLink').value.trim() : '',
          desc_selector: document.getElementById('selDesc') ? document.getElementById('selDesc').value.trim() : ''
        }};
      }}

      const originalSlug = document.getElementById('feedOriginalSlug') ? document.getElementById('feedOriginalSlug').value.trim() : '';
      const customOutput = {{
        title_template: document.getElementById('outTitleTemplate') ? document.getElementById('outTitleTemplate').value.trim() : '',
        content_mode: document.getElementById('outContentMode') ? document.getElementById('outContentMode').value : 'full',
        limit: (document.getElementById('outLimit') && document.getElementById('outLimit').value) ? parseInt(document.getElementById('outLimit').value) : null,
        include_images: document.getElementById('outIncludeImages') ? document.getElementById('outIncludeImages').checked : true,
        include_source_link: document.getElementById('outIncludeSourceLink') ? document.getElementById('outIncludeSourceLink').checked : true,
        include_enclosures: document.getElementById('outIncludeEnclosures') ? document.getElementById('outIncludeEnclosures').checked : true,
        filter_include: document.getElementById('outFilterInclude') ? document.getElementById('outFilterInclude').value.trim() : '',
        filter_exclude: document.getElementById('outFilterExclude') ? document.getElementById('outFilterExclude').value.trim() : ''
      }};

      const appliedRules = [];
      document.querySelectorAll('#feedAppliedRulesContainer input[type="checkbox"]:checked').forEach(cb => {{
        appliedRules.push(cb.value);
      }});

      const payload = {{
        original_slug: originalSlug,
        title: document.getElementById('feedTitle').value.trim(),
        slug: document.getElementById('feedSlug').value.trim(),
        type: document.getElementById('feedType').value,
        target: document.getElementById('feedTarget').value.trim(),
        category: document.getElementById('feedCategory').value.trim() || 'Chung',
        description: document.getElementById('feedDesc').value.trim(),
        cookie_mode: document.getElementById('feedCookieMode').value,
        cookie_profile: document.getElementById('feedCookieProfile').value,
        cookie: document.getElementById('feedCookieCustom').value.trim(),
        save_to_vault: document.getElementById('chkSaveToVault').checked,
        vault_profile_name: document.getElementById('vaultProfileName').value.trim(),
        vault_website: document.getElementById('vaultWebsite').value.trim(),
        use_flaresolverr: document.getElementById('chkUseFlareSolverr') ? document.getElementById('chkUseFlareSolverr').checked : false,
        selectors: selectors,
        custom_output: customOutput,
        applied_rules: appliedRules,
        interval_minutes: parseInt(document.getElementById('feedInterval') ? document.getElementById('feedInterval').value : 0) || 0
      }};

      fetch('/api/feeds', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload)
      }})
      .then(r => {{
        if (!r.ok) return r.json().then(err => Promise.reject(err.detail || 'Lỗi server'));
        return r.json();
      }})
      .then(d => {{
        const isEdit = Boolean(originalSlug);
        showToast(isEdit ? ('Đã cập nhật kênh feed [' + d.slug + ']!') : ('Đã lưu thành công kênh feed [' + d.slug + ']!'));
        closeModal('modalAdd');
        setTimeout(() => location.reload(), 800);
      }})
      .catch(err => {{
        showToast('Lỗi: ' + err, true);
        btn.disabled = false;
        btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:15px; height:15px;"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg><span id="btnSubmitFeedText">' + (originalSlug ? 'Cập nhật Cấu hình' : 'Lưu &amp; Kích hoạt Feed') + '</span>';
      }});
    }}

    function openFeedIntervalModal(slug, title, currentInterval) {{
      document.getElementById('targetFeedSlug').value = slug;
      document.getElementById('lblFeedIntervalTitle').innerText = 'Tần suất cào: ' + title;
      document.getElementById('lblFeedIntervalSub').innerText = 'Thiết lập chu kỳ tự động cào bài viết mới cho riêng scraper [' + slug + '].';
      const val = parseInt(currentInterval) || 30;
      document.getElementById('inputFeedIntervalVal').value = val;
      updateFeedPresetHighlight(val);
      openModal('modalFeedInterval');
    }}

    function selectFormPresetInterval(val) {{
      const input = document.getElementById('feedInterval');
      if (input) input.value = val;
      updateFormPresetHighlight(val);
    }}

    function updateFormPresetHighlight(val) {{
      val = parseInt(val);
      document.querySelectorAll('#formAddFeed .preset-interval-btn').forEach(b => {{
        b.style.borderColor = 'var(--card-border)';
        b.style.background = 'rgba(255, 255, 255, 0.04)';
        b.style.color = 'var(--text-muted)';
      }});
      const activeBtn = document.getElementById('btnFormPreset' + val);
      if (activeBtn) {{
        activeBtn.style.borderColor = '#c084fc';
        activeBtn.style.background = 'rgba(192, 132, 252, 0.2)';
        activeBtn.style.color = '#e9d5ff';
      }}
    }}

    function selectFeedPresetInterval(val) {{
      document.getElementById('inputFeedIntervalVal').value = val;
      updateFeedPresetHighlight(val);
    }}

    function updateFeedPresetHighlight(val) {{
      val = parseInt(val);
      document.querySelectorAll('#modalFeedInterval .preset-interval-btn').forEach(b => {{
        b.style.borderColor = 'var(--card-border)';
        b.style.background = 'rgba(255, 255, 255, 0.04)';
        b.style.color = 'var(--text-muted)';
      }});
      const activeBtn = document.getElementById('btnFeedPreset' + val);
      if (activeBtn) {{
        activeBtn.style.borderColor = 'var(--primary)';
        activeBtn.style.background = 'rgba(14, 165, 233, 0.2)';
        activeBtn.style.color = '#38bdf8';
      }}
    }}

    function handleSaveFeedInterval(e) {{
      e.preventDefault();
      const slug = document.getElementById('targetFeedSlug').value;
      const val = parseInt(document.getElementById('inputFeedIntervalVal').value);
      if (!val || val < 1 || val > 1440) {{
        showToast('Tần suất phải từ 1 đến 1440 phút (24h)', true);
        return;
      }}
      const btn = document.getElementById('btnSaveFeedInterval');
      btn.disabled = true;
      btn.innerText = 'Đang lưu...';

      fetch('/api/feeds/' + slug + '/interval', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ interval_minutes: val }})
      }})
      .then(r => {{
        if (!r.ok) return r.json().then(e => Promise.reject(e.detail || 'Lỗi server'));
        return r.json();
      }})
      .then(d => {{
        showToast('Đã lưu tần suất cào cho [' + slug + ']: ' + val + ' phút!');
        closeModal('modalFeedInterval');
        setTimeout(() => location.reload(), 600);
      }})
      .catch(err => {{
        showToast('Lỗi: ' + err, true);
        btn.disabled = false;
        btn.innerText = 'Lưu tần suất kênh này';
      }});
    }}

    function openAllIntervalsModal() {{
      const container = document.getElementById('allIntervalsList');
      container.innerHTML = '<div style="text-align:center; padding:20px; color:var(--text-dim); font-size:0.8rem;">Đang nạp danh sách kênh...</div>';
      openModal('modalAllIntervals');

      fetch('/api/feeds')
        .then(r => r.json())
        .then(feeds => {{
          const slugs = Object.keys(feeds);
          if (slugs.length === 0) {{
            container.innerHTML = '<div style="text-align:center; padding:20px; color:var(--text-dim); font-size:0.8rem;">Chưa có kênh feed nào.</div>';
            return;
          }}
          let html = '';
          slugs.forEach(s => {{
            const f = feeds[s];
            const interval = f.interval_minutes || 30;
            const title = (f.title || s).replace(/'/g, "\\'");
            html += `
            <div style="background:rgba(15, 23, 42, 0.65); border:1px solid var(--card-border); border-radius:12px; padding:10px 14px; display:flex; align-items:center; justify-content:space-between; gap:10px;">
              <div style="min-width:0; flex:1;">
                <div style="display:flex; align-items:center; gap:8px;">
                  <span style="font-weight:700; font-size:0.88rem; color:#fff;">${{f.title || s}}</span>
                  <span style="font-size:0.65rem; padding:2px 6px; border-radius:4px; background:rgba(14,165,233,0.12); color:#38bdf8;">${{f.category || 'Chung'}}</span>
                </div>
                <div style="font-size:0.72rem; color:var(--text-dim); font-family:var(--mono); margin-top:2px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">
                  ${{f.target || s}}
                </div>
              </div>
              <div style="display:flex; align-items:center; gap:8px; flex-shrink:0;">
                <button type="button" class="btn" onclick="closeModal('modalAllIntervals'); openFeedIntervalModal('${{s}}', '${{title}}', ${{interval}})" style="height:32px; padding:0 12px; font-size:0.75rem; background:rgba(168,85,247,0.15); border-color:rgba(168,85,247,0.35); color:#c084fc;">
                  <span>⏱️ ${{interval}} phút</span>
                  <span style="opacity:0.6; font-size:0.7rem; margin-left:2px;">(Đổi)</span>
                </button>
              </div>
            </div>
            `;
          }});
          container.innerHTML = html;
        }})
        .catch(err => {{
          container.innerHTML = '<div style="color:#fb7185; padding:12px; font-size:0.8rem;">Lỗi tải danh sách: ' + err + '</div>';
        }});
    }}

    function toggleSidebar(open) {{
      const sb = document.getElementById('appSidebar');
      const bd = document.getElementById('sidebarBackdrop');
      if (open) {{
        sb.classList.add('open');
        bd.style.display = 'block';
      }} else {{
        sb.classList.remove('open');
        bd.style.display = 'none';
      }}
    }}

    function filterFeeds(cat, pill) {{
      document.querySelectorAll('.filter-pills .pill').forEach(p => p.classList.remove('active'));
      if (pill) pill.classList.add('active');

      const items = document.querySelectorAll('.feed-item');
      items.forEach(it => {{
        if (cat === 'all' || it.dataset.category === cat) {{
          it.style.display = 'flex';
        }} else {{
          it.style.display = 'none';
        }}
      }});
    }}

    function populateFeedRulesCheckboxes(appliedIds = []) {{
      const container = document.getElementById('feedAppliedRulesContainer');
      if (!container) return;
      container.innerHTML = '<div style="color: var(--text-dim); font-size: 0.75rem; grid-column: span 2;">Đang tải danh sách rule...</div>';

      fetch('/api/rules')
        .then(r => r.json())
        .then(rules => {{
          if (!rules || rules.length === 0) {{
            container.innerHTML = '<div style="color: var(--text-dim); font-size: 0.75rem; grid-column: span 2;">Chưa có rule nào trong kho. Bấm "+ Quản lý kho Rule" để tạo mới!</div>';
            return;
          }}
          container.innerHTML = '';
          rules.forEach(r => {{
            const isChecked = appliedIds && appliedIds.includes(r.id);
            let typeIcon = '🔄';
            if (r.rule_type === 'filter') typeIcon = '⛔';
            else if (r.rule_type === 'format') typeIcon = '✨';
            else if (r.rule_type === 'extract_links') typeIcon = '🔗';

            const label = document.createElement('label');
            label.style.cssText = 'display: flex; align-items: flex-start; gap: 8px; font-size: 0.74rem; color: #e2e8f0; cursor: pointer; padding: 6px 8px; background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.06); border-radius: 6px; transition: all 0.15s;';
            label.innerHTML = `
              <input type="checkbox" value="${{r.id}}" ${{isChecked ? 'checked' : ''}} style="margin-top: 2px;">
              <div style="min-width: 0;">
                <div style="font-weight: 600; color: #fff; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">
                  <span>${{typeIcon}}</span> <span>${{r.name}}</span>
                </div>
                <div style="font-size: 0.65rem; color: var(--text-dim); font-family: var(--mono);">${{r.id}}</div>
              </div>
            `;
            container.appendChild(label);
          }});
        }})
        .catch(err => {{
          container.innerHTML = '<div style="color: #f87171; font-size: 0.75rem; grid-column: span 2;">Lỗi tải rules: ' + err + '</div>';
        }});
    }}

    // TAB NAVIGATION & STATE
    function switchMainTab(tab, updateHash = true) {{
      const validTabs = ['dashboard', 'scrapers', 'profiles', 'rules'];
      if (!validTabs.includes(tab)) tab = 'dashboard';

      // Update sidebar nav items
      validTabs.forEach(t => {{
        const navEl = document.getElementById('navItem_' + t);
        if (navEl) {{
          if (t === tab) navEl.classList.add('active');
          else navEl.classList.remove('active');
        }}
      }});

      // Update tab views
      validTabs.forEach(t => {{
        const viewEl = document.getElementById('view_' + t);
        if (viewEl) {{
          viewEl.style.display = (t === tab) ? 'block' : 'none';
        }}
      }});

      // Update topbar action groups
      validTabs.forEach(t => {{
        const actEl = document.getElementById('topbarActions_' + t);
        if (actEl) {{
          actEl.style.display = (t === tab) ? 'flex' : 'none';
        }}
      }});

      // Update Page Title
      const titleEl = document.getElementById('mainPageTitle');
      if (titleEl) {{
        if (tab === 'dashboard') titleEl.innerText = 'Dashboard & Giám sát Lượt cào';
        else if (tab === 'scrapers') titleEl.innerText = 'Quản lý Scraper & Kênh Feed';
        else if (tab === 'profiles') titleEl.innerText = 'Quản lý Profile & Xác thực Domain';
        else if (tab === 'rules') titleEl.innerText = 'Quản lý Rule & Xử lý Output';
      }}

      // Sync URL hash
      if (updateHash && window.location.hash !== '#' + tab) {{
        history.replaceState(null, null, '#' + tab);
      }}

      // Close mobile sidebar
      toggleSidebar(false);

      if (tab === 'dashboard' && (!dashAllOutputPosts || dashAllOutputPosts.length === 0)) {{
        reloadDashboardPosts();
      }}
    }}

    // RULES UI MANAGEMENT
    function autoGenerateRuleId(name) {{
      const isEdit = document.getElementById('ruleIsEdit').value === 'true';
      if (isEdit) return;
      const slug = (name || '').toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');
      document.getElementById('ruleId').value = slug;
    }}

    function handleRuleTypeChange(type) {{
      document.querySelectorAll('.rule-type-group').forEach(el => el.style.display = 'none');
      if (type === 'replace') {{
        document.getElementById('groupRuleReplace').style.display = 'block';
      }} else if (type === 'filter') {{
        document.getElementById('groupRuleFilter').style.display = 'block';
      }} else if (type === 'format') {{
        document.getElementById('groupRuleFormat').style.display = 'block';
      }} else if (type === 'extract_links') {{
        document.getElementById('groupRuleExtract').style.display = 'block';
      }}
    }}

    function openAddRuleModal() {{
      const form = document.getElementById('formRuleEdit');
      if (form) form.reset();
      document.getElementById('ruleIsEdit').value = 'false';
      const idInput = document.getElementById('ruleId');
      idInput.readOnly = false;
      idInput.style.opacity = '1';

      document.getElementById('ruleType').value = 'replace';
      handleRuleTypeChange('replace');

      document.getElementById('ruleReplaceTarget').value = 'both';
      document.getElementById('ruleReplaceIsRegex').checked = false;
      document.getElementById('ruleFilterCondition').value = 'exclude';
      document.getElementById('ruleFilterTarget').value = 'both';
      document.getElementById('ruleFilterIsRegex').checked = true;
      document.getElementById('ruleFormatCleanHtml').checked = true;
      document.getElementById('ruleFormatStripTags').value = 'script,style,iframe,object,embed,form';
      document.getElementById('chkExtractGdrive').checked = true;
      document.getElementById('chkExtractFshare').checked = true;
      document.getElementById('chkExtractMega').checked = true;
      document.getElementById('chkExtractMediafire').checked = true;
      document.getElementById('ruleExtractEnclosure').checked = true;

      document.getElementById('lblModalRuleTitle').innerText = 'Thêm Rule Xử Lý Output';
      document.getElementById('btnSubmitRuleText').innerText = 'Lưu Rule';
      openModal('modalRuleEdit');
    }}

    function openEditRuleModal(ruleId) {{
      fetch('/api/rules/' + ruleId)
        .then(r => {{
          if (!r.ok) throw new Error('Không thể tải rule');
          return r.json();
        }})
        .then(rule => {{
          document.getElementById('ruleIsEdit').value = 'true';
          document.getElementById('ruleName').value = rule.name || '';
          const idInput = document.getElementById('ruleId');
          idInput.value = rule.id || '';
          idInput.readOnly = true;
          idInput.style.opacity = '0.7';

          const rType = rule.rule_type || 'replace';
          document.getElementById('ruleType').value = rType;
          handleRuleTypeChange(rType);

          if (rType === 'replace') {{
            document.getElementById('ruleReplaceTarget').value = rule.target_field || 'both';
            document.getElementById('ruleReplaceIsRegex').checked = Boolean(rule.is_regex);
            document.getElementById('ruleReplacePattern').value = rule.pattern || '';
            document.getElementById('ruleReplaceReplacement').value = rule.replacement || '';
          }} else if (rType === 'filter') {{
            document.getElementById('ruleFilterCondition').value = rule.condition || 'exclude';
            document.getElementById('ruleFilterTarget').value = rule.target_field || 'both';
            document.getElementById('ruleFilterIsRegex').checked = rule.is_regex !== false;
            document.getElementById('ruleFilterPattern').value = rule.pattern || '';
          }} else if (rType === 'format') {{
            document.getElementById('ruleFormatCleanHtml').checked = rule.clean_html !== false;
            document.getElementById('ruleFormatStripTags').value = rule.strip_tags || 'script,style,iframe,object,embed,form';
            document.getElementById('ruleFormatTitlePrefix').value = rule.title_prefix || '';
            document.getElementById('ruleFormatTitleSuffix').value = rule.title_suffix || '';
            document.getElementById('ruleFormatTruncate').value = rule.truncate_chars || '';
          }} else if (rType === 'extract_links') {{
            const types = rule.link_types || ['gdrive'];
            document.getElementById('chkExtractGdrive').checked = types.includes('gdrive');
            document.getElementById('chkExtractFshare').checked = types.includes('fshare');
            document.getElementById('chkExtractMega').checked = types.includes('mega');
            document.getElementById('chkExtractMediafire').checked = types.includes('mediafire');
            document.getElementById('ruleExtractCustomRegex').value = rule.custom_regex || '';
            document.getElementById('ruleExtractAutoTag').value = rule.auto_tag || '';
            document.getElementById('ruleExtractEnclosure').checked = rule.add_enclosure !== false;
          }}

          document.getElementById('ruleDesc').value = rule.description || '';
          document.getElementById('lblModalRuleTitle').innerText = 'Sửa Rule: ' + (rule.name || rule.id);
          document.getElementById('btnSubmitRuleText').innerText = 'Cập nhật Rule';
          openModal('modalRuleEdit');
        }})
        .catch(err => showToast('Lỗi: ' + err, true));
    }}

    function duplicateRule(ruleId) {{
      fetch('/api/rules/' + ruleId)
        .then(r => {{
          if (!r.ok) throw new Error('Không thể tải rule');
          return r.json();
        }})
        .then(rule => {{
          document.getElementById('ruleIsEdit').value = 'false';
          document.getElementById('ruleName').value = (rule.name || '') + ' (Copy)';
          const idInput = document.getElementById('ruleId');
          idInput.value = (rule.id || '') + '_copy';
          idInput.readOnly = false;
          idInput.style.opacity = '1';

          const rType = rule.rule_type || 'replace';
          document.getElementById('ruleType').value = rType;
          handleRuleTypeChange(rType);

          if (rType === 'replace') {{
            document.getElementById('ruleReplaceTarget').value = rule.target_field || 'both';
            document.getElementById('ruleReplaceIsRegex').checked = Boolean(rule.is_regex);
            document.getElementById('ruleReplacePattern').value = rule.pattern || '';
            document.getElementById('ruleReplaceReplacement').value = rule.replacement || '';
          }} else if (rType === 'filter') {{
            document.getElementById('ruleFilterCondition').value = rule.condition || 'exclude';
            document.getElementById('ruleFilterTarget').value = rule.target_field || 'both';
            document.getElementById('ruleFilterIsRegex').checked = rule.is_regex !== false;
            document.getElementById('ruleFilterPattern').value = rule.pattern || '';
          }} else if (rType === 'format') {{
            document.getElementById('ruleFormatCleanHtml').checked = rule.clean_html !== false;
            document.getElementById('ruleFormatStripTags').value = rule.strip_tags || 'script,style,iframe,object,embed,form';
            document.getElementById('ruleFormatTitlePrefix').value = rule.title_prefix || '';
            document.getElementById('ruleFormatTitleSuffix').value = rule.title_suffix || '';
            document.getElementById('ruleFormatTruncate').value = rule.truncate_chars || '';
          }} else if (rType === 'extract_links') {{
            const types = rule.link_types || ['gdrive'];
            document.getElementById('chkExtractGdrive').checked = types.includes('gdrive');
            document.getElementById('chkExtractFshare').checked = types.includes('fshare');
            document.getElementById('chkExtractMega').checked = types.includes('mega');
            document.getElementById('chkExtractMediafire').checked = types.includes('mediafire');
            document.getElementById('ruleExtractCustomRegex').value = rule.custom_regex || '';
            document.getElementById('ruleExtractAutoTag').value = rule.auto_tag || '';
            document.getElementById('ruleExtractEnclosure').checked = rule.add_enclosure !== false;
          }}

          document.getElementById('ruleDesc').value = rule.description || '';
          document.getElementById('lblModalRuleTitle').innerText = 'Nhân bản Rule: ' + (rule.name || rule.id);
          document.getElementById('btnSubmitRuleText').innerText = 'Lưu Rule mới';
          openModal('modalRuleEdit');
        }})
        .catch(err => showToast('Lỗi: ' + err, true));
    }}

    function handleSaveRule(e) {{
      e.preventDefault();
      const btn = document.getElementById('btnSubmitRule');
      btn.disabled = true;
      btn.innerText = 'Đang lưu...';

      const rType = document.getElementById('ruleType').value;
      const payload = {{
        id: document.getElementById('ruleId').value.trim(),
        name: document.getElementById('ruleName').value.trim(),
        rule_type: rType,
        description: document.getElementById('ruleDesc').value.trim()
      }};

      if (rType === 'replace') {{
        payload.target_field = document.getElementById('ruleReplaceTarget').value;
        payload.is_regex = document.getElementById('ruleReplaceIsRegex').checked;
        payload.pattern = document.getElementById('ruleReplacePattern').value;
        payload.replacement = document.getElementById('ruleReplaceReplacement').value;
      }} else if (rType === 'filter') {{
        payload.condition = document.getElementById('ruleFilterCondition').value;
        payload.target_field = document.getElementById('ruleFilterTarget').value;
        payload.is_regex = document.getElementById('ruleFilterIsRegex').checked;
        payload.pattern = document.getElementById('ruleFilterPattern').value;
      }} else if (rType === 'format') {{
        payload.clean_html = document.getElementById('ruleFormatCleanHtml').checked;
        payload.strip_tags = document.getElementById('ruleFormatStripTags').value.trim();
        payload.title_prefix = document.getElementById('ruleFormatTitlePrefix').value;
        payload.title_suffix = document.getElementById('ruleFormatTitleSuffix').value;
        const trunc = parseInt(document.getElementById('ruleFormatTruncate').value);
        payload.truncate_chars = isNaN(trunc) ? null : trunc;
      }} else if (rType === 'extract_links') {{
        const linkTypes = [];
        if (document.getElementById('chkExtractGdrive').checked) linkTypes.push('gdrive');
        if (document.getElementById('chkExtractFshare').checked) linkTypes.push('fshare');
        if (document.getElementById('chkExtractMega').checked) linkTypes.push('mega');
        if (document.getElementById('chkExtractMediafire').checked) linkTypes.push('mediafire');
        payload.link_types = linkTypes;
        payload.custom_regex = document.getElementById('ruleExtractCustomRegex').value.trim();
        payload.auto_tag = document.getElementById('ruleExtractAutoTag').value.trim();
        payload.add_enclosure = document.getElementById('ruleExtractEnclosure').checked;
      }}

      fetch('/api/rules', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload)
      }})
      .then(r => {{
        if (!r.ok) return r.json().then(err => Promise.reject(err.detail || 'Lỗi server'));
        return r.json();
      }})
      .then(d => {{
        showToast('Đã lưu rule [' + d.id + '] thành công!');
        closeModal('modalRuleEdit');
        setTimeout(() => location.reload(), 800);
      }})
      .catch(err => {{
        showToast('Lỗi: ' + err, true);
        btn.disabled = false;
        btn.innerText = 'Lưu Rule';
      }});
    }}

    function deleteRuleModal(ruleId) {{
      if (!confirm('Bạn có chắc chắn muốn xóa rule [' + ruleId + '] không?')) return;
      fetch('/api/rules/' + ruleId, {{ method: 'DELETE' }})
        .then(r => {{
          if (!r.ok) return r.json().then(err => Promise.reject(err.detail || 'Lỗi server'));
          return r.json();
        }})
        .then(d => {{
          showToast('Đã xóa rule [' + d.id + ']!');
          setTimeout(() => location.reload(), 800);
        }})
        .catch(err => showToast('Lỗi: ' + err, true));
    }}

    function filterRules(type, pill) {{
      document.querySelectorAll('#ruleFilterPills .pill').forEach(p => p.classList.remove('active'));
      if (pill) pill.classList.add('active');

      const items = document.querySelectorAll('#viewRulesList .rule-item');
      items.forEach(it => {{
        if (type === 'all' || it.dataset.type === type) {{
          it.style.display = 'flex';
        }} else {{
          it.style.display = 'none';
        }}
      }});
    }}

    function filterRulesBySearch(query) {{
      const q = (query || '').toLowerCase().trim();
      const items = document.querySelectorAll('#viewRulesList .rule-item');
      items.forEach(it => {{
        if (!q || it.textContent.toLowerCase().includes(q)) {{
          it.style.display = 'flex';
        }} else {{
          it.style.display = 'none';
        }}
      }});
    }}

    function filterDashboardTable(query) {{
      const q = (query || '').toLowerCase().trim();
      const rows = document.querySelectorAll('#dashboardTable tbody tr');
      rows.forEach(r => {{
        if (!q || r.textContent.toLowerCase().includes(q)) {{
          r.style.display = '';
        }} else {{
          r.style.display = 'none';
        }}
      }});
    }}

    function filterScrapersBySearch(query) {{
      const q = (query || '').toLowerCase().trim();
      const cards = document.querySelectorAll('#viewFeeds .feed-item');
      cards.forEach(c => {{
        if (!q || c.textContent.toLowerCase().includes(q)) {{
          c.style.display = 'flex';
        }} else {{
          c.style.display = 'none';
        }}
      }});
    }}

    function filterProfiles(domain, pill) {{
      document.querySelectorAll('#domainFilterPills .pill').forEach(p => p.classList.remove('active'));
      if (pill) pill.classList.add('active');

      const groups = document.querySelectorAll('#viewProfilesList .domain-group-section');
      groups.forEach(g => {{
        if (domain === 'all' || g.dataset.domain === domain) {{
          g.style.display = 'block';
        }} else {{
          g.style.display = 'none';
        }}
      }});
    }}

    function filterProfilesBySearch(query) {{
      const q = (query || '').toLowerCase().trim();
      const groups = document.querySelectorAll('#viewProfilesList .domain-group-section');
      groups.forEach(g => {{
        let hasMatch = false;
        const items = g.querySelectorAll('.profile-item');
        items.forEach(it => {{
          if (!q || it.textContent.toLowerCase().includes(q)) {{
            it.style.display = 'flex';
            hasMatch = true;
          }} else {{
            it.style.display = 'none';
          }}
        }});
        g.style.display = hasMatch ? 'block' : 'none';
      }});
    }}

    window.addEventListener('DOMContentLoaded', () => {{
      const hash = window.location.hash.replace('#', '');
      if (['dashboard', 'scrapers', 'profiles', 'rules'].includes(hash)) {{
        switchMainTab(hash, false);
      }} else {{
        switchMainTab('dashboard', false);
      }}
      reloadDashboardPosts();
    }});

    window.addEventListener('hashchange', () => {{
      const hash = window.location.hash.replace('#', '');
      if (['dashboard', 'scrapers', 'profiles', 'rules'].includes(hash)) {{
        switchMainTab(hash, false);
      }}
    }});
  </script>
  <style>
    @keyframes spin {{ 100% {{ transform: rotate(360deg); }} }}
  </style>
</body>
</html>
"""
    return HTMLResponse(content=html_content)

@app.get("/{path:path}")
async def handle_feed_or_proxy(path: str, request: Request):
    ext = "xml"
    clean_path = path.strip("/")
    
    if "." in clean_path:
        base, extension = clean_path.rsplit(".", 1)
        extension = extension.lower()
        if extension in ("xml", "rss", "json", "atom"):
            ext = extension
            clean_path = base

    feeds = load_feeds()
    if clean_path in feeds or clean_path == "bookthreads":
        meta = feeds.get(clean_path, {})
        posts = await asyncio.to_thread(get_feed_posts, clean_path, False)
        if not isinstance(posts, list):
            posts = list(posts.values()) if isinstance(posts, dict) else []

        q_params = dict(request.query_params)
        if ext in ("xml", "rss"):
            content = generate_rss_xml(clean_path, posts, BASE_URL, meta, q_params)
            return Response(content=content, media_type="application/rss+xml; charset=utf-8")
        elif ext == "json":
            data = generate_json_feed(clean_path, posts, BASE_URL, meta, q_params)
            return JSONResponse(content=data, media_type="application/feed+json; charset=utf-8")
        elif ext == "atom":
            content = generate_atom_xml(clean_path, posts, BASE_URL, meta, q_params)
            return Response(content=content, media_type="application/atom+xml; charset=utf-8")

    upstream_url = f"{RSSHUB_UPSTREAM}/{path}"
    if request.url.query:
        upstream_url += f"?{request.url.query}"
        
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(upstream_url, headers={"User-Agent": "ClaraOS-RSS-Wrapper/1.0"})
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers=dict(resp.headers)
            )
    except Exception as e:
        logger.warning(f"Upstream proxy failed for {upstream_url}: {e}")
        raise HTTPException(status_code=404, detail="Feed not found or upstream unavailable")
