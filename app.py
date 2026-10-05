import os
import json
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
    resolve_effective_cookie
)
from formatters.rss import generate_rss_xml
from formatters.json_feed import generate_json_feed
from formatters.atom import generate_atom_xml

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
                    await asyncio.to_thread(get_feed_posts, slug, True)
                    meta["last_scraped_at"] = now
                    feeds[slug] = meta
                    save_feeds(feeds)
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
        await asyncio.sleep(60)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_scheduler())

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "claraos-rss-hub"}

@app.get("/api/cookies")
def api_get_cookies():
    return get_vault_summary()

@app.get("/api/cookies/{profile_id}")
def api_get_cookie_detail(profile_id: str):
    vault = load_vault()
    if profile_id in vault:
        return vault[profile_id]
    raise HTTPException(status_code=404, detail="Không tìm thấy profile")

@app.post("/api/cookies")
async def api_create_or_update_cookie(request: Request):
    data = await request.json()
    raw_id = data.get("id") or data.get("name", "")
    profile_id = sanitize_slug(raw_id)
    if not profile_id:
        raise HTTPException(status_code=400, detail="Tên hoặc ID profile cookie không hợp lệ")
    
    name = data.get("name", "").strip() or profile_id
    website = data.get("website", "").strip().lower() or "generic"
    scraper_type = data.get("scraper_type", "").strip().lower() or "web"
    cookie = data.get("cookie", "").strip()
    description = data.get("description", "").strip()

    vault = load_vault()
    vault[profile_id] = {
        "id": profile_id,
        "name": name,
        "website": website,
        "scraper_type": scraper_type,
        "cookie": cookie,
        "description": description,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    save_vault(vault)
    return {"status": "ok", "id": profile_id}

@app.delete("/api/cookies/{profile_id}")
def api_delete_cookie(profile_id: str):
    vault = load_vault()
    if profile_id in vault:
        del vault[profile_id]
        save_vault(vault)
        return {"status": "ok", "id": profile_id}
    raise HTTPException(status_code=404, detail="Không tìm thấy bộ cookie này")

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

    # If slug was renamed, delete old key and move history cache
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
        "interval_minutes": int(data.get("interval_minutes", 30) or 30),
        "last_scraped_at": feeds.get(slug, {}).get("last_scraped_at") or (feeds.get(original_slug, {}).get("last_scraped_at") if original_slug else 0) or 0,
        "created_at": created_at,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    save_feeds(feeds)
    
    # Trigger initial scrape in background
    asyncio.create_task(asyncio.to_thread(get_feed_posts, slug, True))
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
    posts = await asyncio.to_thread(get_feed_posts, clean_tag, True)
    return {"status": "ok", "tag": clean_tag, "count": len(posts)}

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    feeds = load_feeds()
    settings = load_settings()
    global_interval = int(settings.get("scrape_interval_minutes", 30))
    feed_cards = ""
    total_posts = 0
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

        # Recent items preview (sleek single row per article)
        preview_items_html = ""
        for p in posts[:3]:
            user = p.get("username", "web")
            first_line = p.get("preview_title") or (p.get("text", "").split("\n")[0][:110] if p.get("text") else "Bài viết không có tiêu đề")
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
        <div class="task-card feed-item" data-category="{cat}">
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
                            <span style="color: #34d399; font-weight: 600;">{count} bài viết</span>
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

                    <!-- Toggle Preview -->
                    <button class="btn" onclick="toggleFeedPreview('{slug}')" id="btnPreview_{slug}" title="Xem trước bài viết vừa cào" style="height: 30px; padding: 0 9px; font-size: 0.74rem;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                        <span>Bài viết</span>
                        <svg id="chevronPreview_{slug}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 12px; height: 12px; transition: transform 0.2s;"><polyline points="6 9 12 15 18 9"/></svg>
                    </button>

                    <!-- Edit Scraper Config -->
                    <button class="btn" onclick="editFeedModal('{slug}')" title="Sửa cấu hình Feed này" style="height: 30px; padding: 0 9px; font-size: 0.74rem;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><path d="M12 20h9M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"/></svg>
                        <span>Sửa</span>
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
                    <span>Bài viết vừa cào gần đây</span>
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
      padding: 18px 32px 80px 32px;
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

    /* Floating Dock Footer */
    .content-footer {{
      position: fixed;
      bottom: 20px;
      left: calc(50% + (var(--sidebar-w) / 2));
      transform: translateX(-50%);
      z-index: 45;
      background: rgba(15, 23, 42, 0.85);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      border: 1px solid rgba(255, 255, 255, 0.12);
      border-radius: 9999px;
      padding: 6px 12px;
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 16px 36px -4px rgba(0, 0, 0, 0.6), 0 0 0 1px rgba(255, 255, 255, 0.05);
      max-width: calc(100vw - var(--sidebar-w) - 32px);
      pointer-events: auto;
    }}
    .footer-stats-strip {{
      display: flex;
      align-items: center;
      gap: 8px;
      overflow-x: auto;
      scrollbar-width: none;
    }}
    .footer-stats-strip::-webkit-scrollbar {{ display: none; }}
    .footer-stat-chip {{
      height: 32px;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 0 12px;
      border-radius: 9999px;
      background: rgba(15, 23, 42, 0.7);
      border: 1px solid rgba(255, 255, 255, 0.08);
      font-size: 0.74rem;
      white-space: nowrap;
      transition: all 0.2s ease;
      box-sizing: border-box;
    }}
    .footer-stat-chip:hover {{
      border-color: rgba(255, 255, 255, 0.25);
      background: rgba(30, 41, 59, 0.9);
    }}
    .chip-label {{
      color: var(--text-dim, #94a3b8);
      font-weight: 600;
      font-size: 0.72rem;
    }}
    .chip-val {{
      font-weight: 700;
      color: #fff;
      font-family: var(--mono);
      font-size: 0.78rem;
    }}
    .chip-cyan .chip-val {{ color: #38bdf8; }}
    .chip-emerald .chip-val {{ color: #34d399; }}
    .chip-amber .chip-val {{ color: #fbbf24; }}
    .chip-violet .chip-val {{ color: #c084fc; }}

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
        padding: 16px 12px 70px;
      }}
      .stats-grid {{
        grid-template-columns: 1fr !important;
        gap: 10px !important;
      }}
      .content-footer {{
        left: 50%;
        width: calc(100% - 24px);
        max-width: 100%;
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
      .footer-stat-chip.clickable {{
        cursor: pointer;
        transition: all 0.2s ease;
      }}
      .footer-stat-chip.clickable:hover {{
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(245, 158, 11, 0.35);
        filter: brightness(1.15);
      }}
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

    <!-- Navigation items -->
    <nav class="sidebar-nav">
      <div class="nav-section-title">ĐIỀU HƯỚNG FEEDS</div>
      <a href="/" class="nav-item active">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect width="18" height="18" x="3" y="3" rx="2"/><path d="M7 8h10M7 12h10M7 16h6"/></svg>
        <span>Danh sách Feeds</span>
      </a>
      <button class="nav-item" onclick="openAddFeedModal()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
        <span>Thêm kênh mới</span>
      </button>
      <button class="nav-item" onclick="openCookieVaultModal()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 2l-2 2m-1.5 1.5L14 9l-1.5-1.5L11 9l-1.5-1.5L8 9l-1.5-1.5-4 4a5 5 0 0 0 7 7l4-4 1.5 1.5L16 15l1.5-1.5L19 15l1.5-1.5 2-2"/></svg>
        <span>Kho Cookie Vault</span>
      </button>
      <button class="nav-item" onclick="openAllIntervalsModal()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
        <span>Lịch cào từng kênh</span>
      </button>
      <button class="nav-item" onclick="refreshAllFeeds()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
        <span>Cào mới tất cả</span>
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
      <div style="display: flex; align-items: center; gap: 14px; min-width: 0;">
        <button class="hamburger-btn" onclick="toggleSidebar(true)">☰</button>
        <h1 class="page-title">Quản lý Feeds &amp; Cấu hình Nguồn</h1>
      </div>

      <div style="display: flex; align-items: center; gap: 10px; flex-shrink: 0;">
        <button class="btn" onclick="openAllIntervalsModal()" title="Xem và điều chỉnh lịch cào của từng Scraper">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:14px; height:14px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
          <span>Tần suất cào</span>
        </button>
        <button class="btn" onclick="openCookieVaultModal()" title="Quản lý Kho Cookie theo Trang web &amp; Scraper">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:15px; height:15px;"><path d="M21 2l-2 2m-1.5 1.5L14 9l-1.5-1.5L11 9l-1.5-1.5L8 9l-1.5-1.5-4 4a5 5 0 0 0 7 7l4-4 1.5 1.5L16 15l1.5-1.5L19 15l1.5-1.5 2-2"/></svg>
          <span>Kho Cookie</span>
        </button>
        <button class="btn primary" onclick="openAddFeedModal()">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 16px; height: 16px;"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
          <span>Thêm kênh Feed</span>
        </button>
        <button id="btnRefreshTop" class="btn" onclick="refreshAllFeeds()">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 16px; height: 16px;"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
          <span>Làm mới</span>
        </button>
      </div>
    </header>

    <main>
      <!-- Stats Grid (Suite Template) -->
      <div class="stats-grid">
        <div class="stat-card">
          <div class="stat-card-icon stat-blue">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="5" cy="19" r="1.5"/><path d="M4 4a16 16 0 0 1 16 16"/><path d="M4 11a9 9 0 0 1 9 9"/></svg>
          </div>
          <div class="stat-info">
            <div class="label">Tổng số Feed</div>
            <div class="val">{len(feeds)} Kênh</div>
            <div class="desc">Web, Threads, RSSHub</div>
          </div>
        </div>

        <div class="stat-card">
          <div class="stat-card-icon stat-emerald">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg>
          </div>
          <div class="stat-info">
            <div class="label">Bài viết đã cào</div>
            <div class="val">{total_posts} Bài</div>
            <div class="desc">Tự động phân loại Ebook</div>
          </div>
        </div>

        <div class="stat-card clickable" onclick="openAllIntervalsModal()" title="Bấm để xem lịch cào riêng của từng Scraper">
          <div class="stat-card-icon stat-violet">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
          </div>
          <div class="stat-info">
            <div class="label" style="display: flex; align-items: center; justify-content: space-between;">
              <span>Chu kỳ cào</span>
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 12px; height: 12px; opacity: 0.7;"><path d="M12 20h9M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"/></svg>
            </div>
            <div class="val">Riêng từng kênh</div>
            <div class="desc">{len(feeds)} Scrapers độc lập</div>
          </div>
        </div>

        <div class="stat-card">
          <div class="stat-card-icon stat-amber">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>
          </div>
          <div class="stat-info">
            <div class="label">Public Gateway</div>
            <div class="val">Online</div>
            <div class="desc">rss.data1box.win</div>
          </div>
        </div>
      </div>

      <!-- Controls Bar & Filter Pills -->
      <div class="controls-bar">
        <div class="filter-pills">
          {cat_pills}
        </div>

        <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
          <button class="btn primary" onclick="openAddFeedModal()" title="Thêm nguồn RSS mới">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
            <span>Thêm kênh mới</span>
          </button>
          <button class="btn" onclick="location.reload()" title="Làm mới trang">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
          </button>
        </div>
      </div>

      <!-- Task / Feeds List -->
      <div id="viewFeeds" class="task-list">
        {feed_cards if feed_cards else '<div style="text-align:center; padding:40px; color:var(--text-dim);">Chưa có kênh feed nào. Bấm "+ Thêm kênh Feed" để bắt đầu!</div>'}
      </div>
    </main>

    <!-- Floating Dock Footer -->
    <div class="content-footer">
      <div class="footer-stats-strip">
        <div class="footer-stat-chip chip-cyan">
          <span class="chip-label">Kênh:</span>
          <span class="chip-val">{len(feeds)} Feeds</span>
        </div>
        <div class="footer-stat-chip chip-emerald">
          <span class="chip-label">Bài viết:</span>
          <span class="chip-val">{total_posts}</span>
        </div>
        <div class="footer-stat-chip chip-amber clickable" onclick="openAllIntervalsModal()" title="Xem lịch cào riêng của các Scraper">
          <span class="chip-label">Tần suất:</span>
          <span class="chip-val">Riêng từng Scraper</span>
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 11px; height: 11px; opacity: 0.8; margin-left: 2px;"><polyline points="6 9 12 15 18 9"/></svg>
        </div>
        <div class="footer-stat-chip chip-violet">
          <span class="chip-label">Gateway:</span>
          <span class="chip-val">rss.data1box.win</span>
        </div>
      </div>
    </div>
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

        <!-- 3.1 TẦN SUẤT CÀO DỮ LIỆU CỦA SCRAPER NÀY -->
        <div class="form-group">
          <label class="form-label">Tần suất cào dữ liệu của Scraper này *</label>
          <select id="feedInterval" class="form-select" style="font-size: 0.85rem;">
            <option value="5">⚡ 5 phút / lần (Cập nhật cực nhanh)</option>
            <option value="10">⚡ 10 phút / lần</option>
            <option value="15">15 phút / lần</option>
            <option value="30" selected>30 phút / lần (Tiêu chuẩn khuyến nghị)</option>
            <option value="60">1 giờ / lần (60 phút)</option>
            <option value="120">2 giờ / lần (120 phút)</option>
            <option value="360">6 giờ / lần (360 phút)</option>
            <option value="720">12 giờ / lần (720 phút)</option>
            <option value="1440">24 giờ / lần (1 ngày)</option>
          </select>
          <div class="form-hint">Mỗi Scraper hoạt động với chu kỳ cào riêng độc lập, tự động quét theo đúng lịch hẹn đã chọn.</div>
        </div>

        <!-- 4. CẤU HÌNH COOKIE XÁC THỰC CHO SCRAPER -->
        <div style="background: rgba(7, 12, 24, 0.7); border: 1px solid var(--card-border); border-radius: 14px; padding: 14px; margin-bottom: 16px;">
          <div class="form-group" style="margin-bottom: 10px;">
            <label class="form-label" style="display:flex; align-items:center; justify-content:space-between;">
              <span>Cấu hình Cookie cho Scraper này</span>
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

        <!-- 5. TÙY BIẾN ĐỊNH DẠNG ĐẦU RA (CUSTOM OUTPUT & FILTERS) -->
        <div style="background: rgba(7, 12, 24, 0.7); border: 1px solid var(--card-border); border-radius: 14px; padding: 14px; margin-bottom: 16px;">
          <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px;">
            <div style="display: flex; align-items: center; gap: 8px;">
              <span style="font-size: 0.8rem; font-weight: 700; color: #38bdf8;">⚙️ Cấu hình Định dạng Đầu ra (Custom Output &amp; Filters)</span>
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

  <!-- MODAL: COOKIE VAULT MANAGEMENT -->
  <div id="modalCookieVault" class="modal-overlay" onclick="handleModalClick(event, 'modalCookieVault')">
    <div class="modal-card" style="width: min(680px, 100%);">
      <div class="modal-head">
        <div>
          <h2 class="modal-title">Quản lý Cookie theo Trang web &amp; Scraper</h2>
          <div style="font-size:0.75rem; color:var(--text-dim); margin-top:2px;">Thêm và quản lý Cookie xác thực phân loại theo từng trang web/domain để các Scraper tái sử dụng linh hoạt.</div>
        </div>
        <button class="modal-close" onclick="closeModal('modalCookieVault')">✕</button>
      </div>

      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px;">
        <span style="font-size:0.75rem; font-weight:700; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.04em;">Danh sách Cookie theo Trang web</span>
        <button class="btn primary" onclick="showAddCookieForm(true)" style="height:32px; font-size:0.75rem;">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:13px; height:13px;"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
          <span>Thêm Cookie mới</span>
        </button>
      </div>

      <!-- Add/Edit Cookie Form -->
      <div id="boxAddCookie" style="display:none; background:rgba(7, 12, 24, 0.85); border:1px solid var(--primary-glow); border-radius:14px; padding:16px; margin-bottom:16px;">
        <div id="lblFormCookieTitle" style="font-size:0.85rem; font-weight:700; color:#fff; margin-bottom:10px;">Thêm Cookie cho Trang web</div>
        <form onsubmit="handleSaveVaultCookie(event)">
          <input type="hidden" id="editCookieId" value="">
          <div style="display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-bottom:10px;">
            <div>
              <label class="form-label">Tên cấu hình *</label>
              <input type="text" id="newVaultName" class="form-input" required placeholder="vd: Threads Acc 1, Voz VIP, Báo Tuổi Trẻ">
            </div>
            <div>
              <label class="form-label">Trang web / Domain áp dụng *</label>
              <input type="text" id="newVaultWebsite" class="form-input" required placeholder="vd: threads.net, voz.vn, tuoitre.vn">
            </div>
          </div>
          <div class="form-group" style="margin-bottom:10px;">
            <label class="form-label">Loại Scraper tương thích</label>
            <select id="newVaultScraperType" class="form-select">
              <option value="threads">Threads Scraper</option>
              <option value="web">Web / RSS Scraper thông thường</option>
              <option value="rsshub">RSSHub Upstream</option>
            </select>
          </div>
          <div class="form-group" style="margin-bottom:10px;">
            <label class="form-label">Chuỗi Cookie xác thực *</label>
            <textarea id="newVaultCookie" class="form-textarea" required rows="2" placeholder="sessionid=...; token=...; hoặc dán JSON từ Cookie-Editor" style="font-family:var(--mono); font-size:0.75rem;"></textarea>
          </div>
          <div class="form-group" style="margin-bottom:12px;">
            <label class="form-label">Ghi chú (Tùy chọn)</label>
            <input type="text" id="newVaultDesc" class="form-input" placeholder="vd: Tài khoản đọc báo trả phí, nhóm kín...">
          </div>
          <div style="display:flex; justify-content:flex-end; gap:8px;">
            <button type="button" class="btn" onclick="showAddCookieForm(false)" style="height:32px; font-size:0.75rem;">Hủy</button>
            <button type="submit" class="btn primary" style="height:32px; font-size:0.75rem;">Lưu Cookie</button>
          </div>
        </form>
      </div>

      <!-- Cookie Vault List -->
      <div id="vaultList" style="display:flex; flex-direction:column; gap:10px;">
        <div style="text-align:center; padding:20px; color:var(--text-dim); font-size:0.8rem;">Đang tải danh sách Cookie...</div>
      </div>

      <div style="display:flex; justify-content:flex-end; margin-top:20px; padding-top:14px; border-top:1px solid var(--card-border);">
        <button class="btn" onclick="closeModal('modalCookieVault')">Đóng</button>
      </div>
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
          showToast('Đã cào mới thành công: ' + d.count + ' bài viết');
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

      const title = document.getElementById('modalAddTitle');
      if (title) title.innerText = 'Tạo Kênh RSS Mới (Universal Feed Generator)';
      const btnText = document.getElementById('btnSubmitFeedText');
      if (btnText) btnText.innerText = 'Lưu & Kích hoạt Feed';

      populateProfileDropdown();
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
          if (document.getElementById('feedInterval')) document.getElementById('feedInterval').value = String(f.interval_minutes || 30);

          const title = document.getElementById('modalAddTitle');
          if (title) title.innerText = 'Chỉnh sửa Cấu hình Kênh: ' + (f.title || f.slug);
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
      fetch('/api/cookies')
        .then(r => r.json())
        .then(profiles => {{
          const sel = document.getElementById('feedCookieProfile');
          sel.innerHTML = '';
          if (!profiles || profiles.length === 0) {{
            sel.innerHTML = '<option value="">(Chưa có bộ cookie nào trong kho - Bấm "Quản lý Kho Cookie" để thêm)</option>';
            return;
          }}
          profiles.forEach(p => {{
            const opt = document.createElement('option');
            opt.value = p.id;
            opt.innerText = '[' + (p.website || 'web') + '] ' + p.name + ' (' + p.masked + ')';
            sel.appendChild(opt);
          }});
        }});
    }}

    function openCookieVaultModal() {{
      openModal('modalCookieVault');
      renderCookieVault();
    }}

    function showAddCookieForm(show, profile = null) {{
      const box = document.getElementById('boxAddCookie');
      box.style.display = show ? 'block' : 'none';
      const title = document.getElementById('lblFormCookieTitle');
      const inpId = document.getElementById('editCookieId');
      const inpName = document.getElementById('newVaultName');
      const inpWeb = document.getElementById('newVaultWebsite');
      const selType = document.getElementById('newVaultScraperType');
      const txtCookie = document.getElementById('newVaultCookie');
      const inpDesc = document.getElementById('newVaultDesc');

      if (profile) {{
        title.innerText = 'Chỉnh sửa Cookie: ' + profile.name;
        inpId.value = profile.id;
        inpName.value = profile.name;
        inpWeb.value = profile.website || '';
        selType.value = profile.scraper_type || 'web';
        inpDesc.value = profile.description || '';
        txtCookie.value = '';
        txtCookie.placeholder = '(Giữ nguyên cookie hiện tại hoặc dán cookie mới...)';
        txtCookie.required = false;
      }} else {{
        title.innerText = 'Thêm Cookie cho Trang web';
        inpId.value = '';
        inpName.value = '';
        inpWeb.value = '';
        selType.value = 'web';
        txtCookie.value = '';
        txtCookie.placeholder = 'sessionid=...; token=...; hoặc dán JSON từ Cookie-Editor';
        txtCookie.required = true;
        inpDesc.value = '';
      }}
    }}

    function renderCookieVault() {{
      const box = document.getElementById('vaultList');
      box.innerHTML = '<div style="text-align:center; padding:20px; color:var(--text-dim); font-size:0.8rem;">Đang nạp Kho Cookie...</div>';

      fetch('/api/cookies')
        .then(r => r.json())
        .then(profiles => {{
          if (!profiles || profiles.length === 0) {{
            box.innerHTML = '<div style="text-align:center; padding:24px; color:var(--text-dim); font-size:0.8rem; background:rgba(15,23,42,0.4); border-radius:12px; border:1px dashed var(--card-border);">Kho cookie hiện đang trống. Hãy bấm "+ Thêm Cookie mới" để thêm cookie cho từng trang web.</div>';
            return;
          }}
          let html = '';
          profiles.forEach(p => {{
            const websiteBadge = `<span style="font-size:0.68rem; font-weight:700; padding:2px 7px; border-radius:4px; background:rgba(14,165,233,0.15); color:#38bdf8; border:1px solid rgba(14,165,233,0.3);">🌐 ${{p.website}}</span>`;
            const scraperBadge = `<span style="font-size:0.65rem; font-weight:600; padding:2px 6px; border-radius:4px; background:rgba(255,255,255,0.06); color:var(--text-muted); text-transform:uppercase;">${{p.scraper_type}}</span>`;

            html += `
            <div style="background:rgba(15, 23, 42, 0.7); border:1px solid var(--card-border); border-radius:12px; padding:12px 16px; display:flex; align-items:center; justify-content:space-between; gap:12px; transition:border-color 0.2s;">
              <div style="min-width:0; flex:1;">
                <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                  <span style="font-weight:700; font-size:0.9rem; color:#fff;">${{p.name}}</span>
                  ${{websiteBadge}}
                  ${{scraperBadge}}
                </div>
                <div style="font-family:var(--mono); font-size:0.75rem; color:#cbd5e1; margin-top:4px;">
                  Cookie: <span style="color:#38bdf8;">${{p.masked}}</span>
                </div>
                <div style="font-size:0.72rem; color:var(--text-dim); margin-top:2px;">
                  ${{p.description || 'Không có mô tả'}}
                </div>
              </div>
              <div style="display:flex; align-items:center; gap:8px; flex-shrink:0;">
                <button class="btn" onclick="editCookiePrompt('${{p.id}}')" style="height:30px; padding:0 10px; font-size:0.74rem;">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:13px; height:13px;"><path d="M12 20h9M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"/></svg>
                  <span>Sửa</span>
                </button>
                <button class="btn danger" onclick="deleteVaultCookie('${{p.id}}')" style="height:30px; padding:0 10px; font-size:0.74rem;">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:13px; height:13px;"><path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                  <span>Xóa</span>
                </button>
              </div>
            </div>
            `;
          }});
          box.innerHTML = html;
        }})
        .catch(e => {{
          box.innerHTML = '<div style="color:#fb7185; padding:10px; font-size:0.8rem;">Lỗi tải kho cookie: ' + e + '</div>';
        }});
    }}

    function editCookiePrompt(profileId) {{
      fetch('/api/cookies/' + profileId)
        .then(r => r.json())
        .then(p => {{
          showAddCookieForm(true, p);
        }})
        .catch(err => showToast('Lỗi: ' + err, true));
    }}

    function handleSaveVaultCookie(e) {{
      e.preventDefault();
      const payload = {{
        id: document.getElementById('editCookieId').value.trim(),
        name: document.getElementById('newVaultName').value.trim(),
        website: document.getElementById('newVaultWebsite').value.trim().toLowerCase(),
        scraper_type: document.getElementById('newVaultScraperType').value.trim().toLowerCase(),
        cookie: document.getElementById('newVaultCookie').value.trim(),
        description: document.getElementById('newVaultDesc').value.trim()
      }};
      fetch('/api/cookies', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload)
      }})
      .then(r => r.json())
      .then(d => {{
        showToast('Đã lưu thành công bộ cookie cho trang [' + payload.website + ']!');
        showAddCookieForm(false);
        renderCookieVault();
        populateProfileDropdown();
      }})
      .catch(err => showToast('Lỗi: ' + err, true));
    }}

    function deleteVaultCookie(id) {{
      if (!confirm('Xóa bộ cookie [' + id + '] khỏi Kho?')) return;
      fetch('/api/cookies/' + id, {{ method: 'DELETE' }})
        .then(r => r.json())
        .then(d => {{
          showToast('Đã xóa bộ cookie');
          renderCookieVault();
          populateProfileDropdown();
        }})
        .catch(err => showToast('Lỗi khi xóa: ' + err, true));
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
