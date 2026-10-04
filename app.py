import os
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
SCRAPE_INTERVAL_MINUTES = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "30"))

async def background_scheduler():
    while True:
        try:
            feeds = load_feeds()
            logger.info(f"Running periodic scrape for {len(feeds)} feeds...")
            for slug, meta in feeds.items():
                await asyncio.to_thread(get_feed_posts, slug, True)
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
        await asyncio.sleep(SCRAPE_INTERVAL_MINUTES * 60)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_scheduler())

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "claraos-rss-hub"}

@app.get("/api/cookies")
def api_get_cookies():
    return get_vault_summary()

@app.post("/api/cookies")
async def api_create_or_update_cookie(request: Request):
    data = await request.json()
    profile_id = sanitize_slug(data.get("id") or data.get("name", ""))
    if not profile_id:
        raise HTTPException(status_code=400, detail="Tên hoặc ID profile cookie không hợp lệ")
    
    name = data.get("name", "").strip() or profile_id
    platform = data.get("platform", "generic").strip()
    cookie = data.get("cookie", "").strip()
    description = data.get("description", "").strip()

    vault = load_vault()
    is_system = vault.get(profile_id, {}).get("is_system", False)
    vault[profile_id] = {
        "id": profile_id,
        "name": name,
        "platform": platform,
        "cookie": cookie,
        "is_system": is_system,
        "description": description,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    save_vault(vault)
    return {"status": "ok", "id": profile_id}

@app.delete("/api/cookies/{profile_id}")
def api_delete_cookie(profile_id: str):
    vault = load_vault()
    if profile_id in vault:
        if vault[profile_id].get("is_system"):
            vault[profile_id]["cookie"] = ""
            save_vault(vault)
            return {"status": "ok", "action": "cleared"}
        del vault[profile_id]
        save_vault(vault)
        return {"status": "ok", "action": "deleted"}
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
    feed_type = data.get("type", "threads")
    target = data.get("target", "").strip() or slug
    category = data.get("category", "Chung").strip() or "Chung"
    description = data.get("description", "").strip()
    
    cookie_mode = data.get("cookie_mode", "default")
    cookie_profile = data.get("cookie_profile", "").strip()
    cookie = data.get("cookie", "").strip()
    save_to_vault = bool(data.get("save_to_vault", False))
    vault_profile_name = data.get("vault_profile_name", "").strip()

    # Save to cookie vault if requested
    if save_to_vault and cookie and vault_profile_name:
        vault = load_vault()
        p_id = sanitize_slug(vault_profile_name) or f"cookie_{int(datetime.now().timestamp())}"
        vault[p_id] = {
            "id": p_id,
            "name": vault_profile_name,
            "platform": feed_type,
            "cookie": cookie,
            "is_system": False,
            "description": f"Được lưu tự động từ feed [{title}]",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        save_vault(vault)
        cookie_mode = "profile"
        cookie_profile = p_id

    feeds = load_feeds()
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
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    save_feeds(feeds)
    
    # Trigger initial scrape in background
    asyncio.create_task(asyncio.to_thread(get_feed_posts, slug, True))
    return {"status": "ok", "slug": slug}

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

@app.get("/api/refresh/{tag}")
async def refresh_feed(tag: str):
    clean_tag = sanitize_slug(tag.lstrip("#"))
    posts = await asyncio.to_thread(get_feed_posts, clean_tag, True)
    return {"status": "ok", "tag": clean_tag, "count": len(posts)}

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    feeds = load_feeds()
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
        
        feed_type = meta.get("type", "threads")
        type_badge = "THREADS"
        if feed_type == "rsshub": type_badge = "RSSHUB"
        elif feed_type == "custom_rss": type_badge = "EXTERNAL RSS"

        # Cookie Mode Badge
        cookie_mode = meta.get("cookie_mode", "default")
        cookie_profile = meta.get("cookie_profile", "")
        cookie_val = meta.get("cookie", "")
        
        cookie_badge = ""
        if cookie_mode == "default" and feed_type == "threads":
            cookie_badge = '<span class="badge cyan" style="padding: 2px 7px; font-size: 0.65rem;" title="Đang dùng Cookie Threads mặc định của hệ thống"><span class="dot" style="background:#0ea5e9;"></span> Threads Shared Cookie</span>'
        elif cookie_mode == "profile":
            cookie_badge = f'<span class="badge green" style="padding: 2px 7px; font-size: 0.65rem;" title="Dùng bộ Cookie [{cookie_profile}]"><span class="dot" style="background:#10b981;"></span> Vault: {cookie_profile}</span>'
        elif cookie_mode == "custom" and cookie_val:
            cookie_badge = '<span class="badge" style="padding: 2px 7px; font-size: 0.65rem; background:rgba(245, 158, 11, 0.12); color:#fbbf24; border-color:rgba(245, 158, 11, 0.3);" title="Đang dùng Cookie riêng của kênh"><span class="dot" style="background:#f59e0b;"></span> Custom Cookie</span>'
        elif cookie_mode == "none":
            cookie_badge = '<span class="badge" style="padding: 2px 7px; font-size: 0.65rem; opacity:0.65;" title="Chế độ Guest / Không dùng cookie">Guest</span>'

        # Recent items preview
        preview_items_html = ""
        for p in posts[:3]:
            user = p.get("username", "user")
            raw_text = p.get("text", "").strip()
            first_line = raw_text.split("\n")[0][:110] if raw_text else "Bài viết không có tiêu đề"
            gdrive = p.get("gdrive_links", [])
            badge = '<span class="badge cyan" style="padding: 2px 8px; font-size: 0.68rem;"><span class="dot"></span> Ebook Drive</span>' if gdrive else ""
            
            preview_items_html += f"""
            <div style="background: rgba(15, 23, 42, 0.65); border: 1px solid rgba(255, 255, 255, 0.06); border-radius: 12px; padding: 10px 14px; display: flex; flex-direction: column; gap: 4px;">
                <div style="display: flex; align-items: center; justify-content: space-between;">
                    <span style="font-weight: 600; font-size: 0.75rem; color: #38bdf8;">@{user}</span>
                    {badge}
                </div>
                <div style="color: var(--text-muted); font-size: 0.76rem; line-height: 1.45; overflow: hidden; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;">
                    {first_line}...
                </div>
            </div>
            """

        feed_cards += f"""
        <div class="task-card feed-item" data-category="{cat}">
            <div class="task-card-head">
                <div style="display: flex; align-items: center; gap: 14px;">
                    <div class="brand-icon" style="background: linear-gradient(135deg, rgba(14, 165, 233, 0.25), rgba(99, 102, 241, 0.25)); border: 1px solid rgba(14, 165, 233, 0.3);">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 22px; height: 22px; color: #38bdf8;">
                            <path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/>
                            <path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/>
                        </svg>
                    </div>
                    <div>
                        <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
                            <span style="font-size: 0.68rem; font-weight: 700; color: #38bdf8; text-transform: uppercase; letter-spacing: 0.05em;">{cat}</span>
                            <span style="font-size: 0.65rem; font-weight: 700; padding: 2px 6px; border-radius: 4px; background: rgba(255,255,255,0.06); color: var(--text-dim); border: 1px solid var(--card-border);">{type_badge}</span>
                            {cookie_badge}
                        </div>
                        <div class="task-title" style="margin-top: 2px;">{meta['title']}</div>
                    </div>
                </div>

                <div class="badge green">
                    <span class="dot"></span>
                    <span>{count} bài viết</span>
                </div>
            </div>

            <div style="color: var(--text-muted); font-size: 0.8rem; line-height: 1.5; margin: 4px 0 8px 0;">
                {meta.get('description', '')}
            </div>

            <!-- Feed URL Rows -->
            <div style="display: flex; flex-direction: column; gap: 8px;">
                <!-- RSS 2.0 -->
                <div style="display: flex; align-items: center; gap: 8px; background: rgba(7, 12, 24, 0.8); border: 1px solid var(--card-border); border-radius: 12px; padding: 6px 10px;">
                    <span style="font-size: 0.7rem; font-weight: 700; padding: 4px 8px; border-radius: 6px; background: rgba(245, 158, 11, 0.12); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); text-align: center; width: 62px;">RSS 2.0</span>
                    <input type="text" readonly value="{BASE_URL}/{slug}.xml" style="background: transparent; border: none; outline: none; color: #cbd5e1; font-family: var(--mono); font-size: 0.78rem; flex: 1; min-width: 0;" />
                    <a href="{BASE_URL}/{slug}.xml" target="_blank" class="btn" style="height: 30px; padding: 0 10px; font-size: 0.74rem;" title="Mở trong tab mới">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                        <span>Mở</span>
                    </a>
                    <button class="btn" onclick="copyLink('{BASE_URL}/{slug}.xml', this)" style="height: 30px; padding: 0 10px; font-size: 0.74rem;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/></svg>
                        <span>Sao chép</span>
                    </button>
                </div>

                <!-- JSON Feed -->
                <div style="display: flex; align-items: center; gap: 8px; background: rgba(7, 12, 24, 0.8); border: 1px solid var(--card-border); border-radius: 12px; padding: 6px 10px;">
                    <span style="font-size: 0.7rem; font-weight: 700; padding: 4px 8px; border-radius: 6px; background: rgba(14, 165, 233, 0.12); color: #38bdf8; border: 1px solid rgba(14, 165, 233, 0.3); text-align: center; width: 62px;">JSON</span>
                    <input type="text" readonly value="{BASE_URL}/{slug}.json" style="background: transparent; border: none; outline: none; color: #cbd5e1; font-family: var(--mono); font-size: 0.78rem; flex: 1; min-width: 0;" />
                    <a href="{BASE_URL}/{slug}.json" target="_blank" class="btn" style="height: 30px; padding: 0 10px; font-size: 0.74rem;" title="Mở trong tab mới">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                        <span>Mở</span>
                    </a>
                    <button class="btn" onclick="copyLink('{BASE_URL}/{slug}.json', this)" style="height: 30px; padding: 0 10px; font-size: 0.74rem;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/></svg>
                        <span>Sao chép</span>
                    </button>
                </div>

                <!-- ATOM Feed -->
                <div style="display: flex; align-items: center; gap: 8px; background: rgba(7, 12, 24, 0.8); border: 1px solid var(--card-border); border-radius: 12px; padding: 6px 10px;">
                    <span style="font-size: 0.7rem; font-weight: 700; padding: 4px 8px; border-radius: 6px; background: rgba(139, 92, 246, 0.12); color: #c084fc; border: 1px solid rgba(139, 92, 246, 0.3); text-align: center; width: 62px;">ATOM</span>
                    <input type="text" readonly value="{BASE_URL}/{slug}.atom" style="background: transparent; border: none; outline: none; color: #cbd5e1; font-family: var(--mono); font-size: 0.78rem; flex: 1; min-width: 0;" />
                    <a href="{BASE_URL}/{slug}.atom" target="_blank" class="btn" style="height: 30px; padding: 0 10px; font-size: 0.74rem;" title="Mở trong tab mới">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 13px; height: 13px;"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                        <span>Mở</span>
                    </a>
                    <button class="btn" onclick="copyLink('{BASE_URL}/{slug}.atom', this)" style="height: 30px; padding: 0 10px; font-size: 0.74rem;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/></svg>
                        <span>Sao chép</span>
                    </button>
                </div>
            </div>

            <!-- Recent Items Box -->
            <div style="margin-top: 6px;">
                <div style="font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-dim); margin-bottom: 8px; display: flex; align-items: center; gap: 6px;">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                    <span>Bài viết vừa cào gần đây</span>
                </div>
                <div style="display: grid; grid-template-columns: 1fr; gap: 8px;">
                    {preview_items_html if preview_items_html else '<div style="color:var(--text-dim); font-size:0.75rem; font-style:italic;">Chưa có dữ liệu bài viết (bấm cào mới ngay bên dưới).</div>'}
                </div>
            </div>

            <!-- Card Action Footer -->
            <div class="task-card-footer" style="margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--card-border);">
                <div style="display: flex; align-items: center; gap: 8px; font-size: 0.74rem; color: var(--text-dim);">
                    <span class="dot" style="background: #0ea5e9;"></span>
                    <span>Nguồn: <strong style="color:var(--text); font-family:var(--mono);">{meta.get('target', slug)}</strong></span>
                </div>
                <div class="task-actions-row">
                    <button class="btn danger" onclick="deleteFeed('{slug}')" title="Xóa kênh Feed này" style="height: 32px; padding: 0 10px;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                        <span>Xóa</span>
                    </button>
                    <button class="btn primary" onclick="refreshFeed('{slug}', this)" style="height: 32px; padding: 0 12px;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
                        <span>Cào mới ngay</span>
                    </button>
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
      gap: 16px;
    }}
    .task-card {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 16px;
      padding: 20px;
      backdrop-filter: blur(12px);
      display: flex;
      flex-direction: column;
      gap: 14px;
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

    /* Modal Form (Debrid Standard) */
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
      width: min(600px, 100%);
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
      <button class="nav-item" onclick="openModal('modalAdd')">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
        <span>Thêm kênh mới</span>
      </button>
      <button class="nav-item" onclick="openCookieVaultModal()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 2l-2 2m-1.5 1.5L14 9l-1.5-1.5L11 9l-1.5-1.5L8 9l-1.5-1.5-4 4a5 5 0 0 0 7 7l4-4 1.5 1.5L16 15l1.5-1.5L19 15l1.5-1.5 2-2"/></svg>
        <span>Kho Cookie Vault</span>
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
        <button class="btn" onclick="openCookieVaultModal()" title="Quản lý Kho Cookie tái sử dụng">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:15px; height:15px;"><path d="M21 2l-2 2m-1.5 1.5L14 9l-1.5-1.5L11 9l-1.5-1.5L8 9l-1.5-1.5-4 4a5 5 0 0 0 7 7l4-4 1.5 1.5L16 15l1.5-1.5L19 15l1.5-1.5 2-2"/></svg>
          <span>Kho Cookie</span>
        </button>
        <button class="btn primary" onclick="openModal('modalAdd')">
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
            <div class="desc">Threads, RSSHub, URLs</div>
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

        <div class="stat-card">
          <div class="stat-card-icon stat-violet">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
          </div>
          <div class="stat-info">
            <div class="label">Chu kỳ quét</div>
            <div class="val">{SCRAPE_INTERVAL_MINUTES} Phút</div>
            <div class="desc">Background Daemon</div>
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
          <button class="btn primary" onclick="openModal('modalAdd')" title="Thêm nguồn RSS mới">
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
        <div class="footer-stat-chip chip-amber">
          <span class="chip-label">Tần suất:</span>
          <span class="chip-val">{SCRAPE_INTERVAL_MINUTES}m</span>
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
        <h2 class="modal-title">Cấu hình Kênh RSS Mới</h2>
        <button class="modal-close" onclick="closeModal('modalAdd')">✕</button>
      </div>

      <form id="formAddFeed" onsubmit="handleSaveFeed(event)">
        <div class="form-group">
          <label class="form-label">Tên hiển thị kênh (Title) *</label>
          <input type="text" id="feedTitle" class="form-input" required placeholder="Ví dụ: Tin tức Công nghệ Threads, VnExpress Mới, ..." oninput="autoGenerateSlug(this.value)">
        </div>

        <div class="form-group">
          <label class="form-label">Định danh URL (Slug) *</label>
          <div style="display:flex; align-items:center; gap:8px;">
            <span style="font-size:0.75rem; color:var(--text-dim); font-family:var(--mono);">{BASE_URL}/</span>
            <input type="text" id="feedSlug" class="form-input" required placeholder="congnghe" style="font-family:var(--mono);">
            <span style="font-size:0.75rem; color:var(--text-dim); font-family:var(--mono);">.xml / .json</span>
          </div>
          <div class="form-hint">Chỉ gồm chữ cái viết thường không dấu, số và gạch ngang (a-z, 0-9, -).</div>
        </div>

        <div class="form-group">
          <label class="form-label">Loại nguồn cào (Source Type) *</label>
          <select id="feedType" class="form-select" onchange="handleTypeChange(this.value)">
            <option value="threads">Threads.net (Hashtag hoặc từ khóa)</option>
            <option value="rsshub">RSSHub Upstream Route (Hệ thống RSSHub)</option>
            <option value="custom_rss">URL RSS / Atom ngoài (Website, Báo chí, Blog)</option>
          </select>
        </div>

        <div class="form-group">
          <label class="form-label" id="lblTarget">Từ khóa Hashtag trên Threads *</label>
          <input type="text" id="feedTarget" class="form-input" required placeholder="vd: congnghe, reviewphim, kinhte, manga">
          <div class="form-hint" id="hintTarget">Nhập hashtag hoặc từ khóa cần tìm kiếm và tạo RSS trên mạng xã hội Threads.</div>
        </div>

        <div class="form-group">
          <label class="form-label">Thể loại / Chuyên mục (Category)</label>
          <input type="text" id="feedCategory" class="form-input" placeholder="vd: Công nghệ, Tin tức, Sách &amp; Ebooks, Giải trí">
        </div>

        <div class="form-group">
          <label class="form-label">Mô tả ngắn</label>
          <textarea id="feedDesc" class="form-textarea" rows="2" placeholder="Mô tả nội dung kênh feed này..."></textarea>
        </div>

        <!-- Cookie Configuration Section -->
        <div style="background: rgba(7, 12, 24, 0.7); border: 1px solid var(--card-border); border-radius: 14px; padding: 14px; margin-bottom: 16px;">
          <div class="form-group" style="margin-bottom: 10px;">
            <label class="form-label" style="display:flex; align-items:center; justify-content:space-between;">
              <span>Xác thực Cookie / Phiên đăng nhập</span>
              <a href="javascript:void(0)" onclick="openCookieVaultModal()" style="color:#38bdf8; font-size:0.72rem; text-decoration:none;">Quản lý Kho Cookie &rarr;</a>
            </label>
            <select id="feedCookieMode" class="form-select" onchange="handleCookieModeChange(this.value)">
              <option value="default" id="optCookieDefault">🔹 Dùng Cookie Threads mặc định (Hệ thống)</option>
              <option value="profile">📂 Chọn từ Kho Cookie đã lưu (Cookie Vault)...</option>
              <option value="custom">✏️ Nhập Cookie riêng biệt cho kênh này</option>
              <option value="none">🌐 Không dùng Cookie (Chế độ công khai / Guest)</option>
            </select>
            <div class="form-hint" id="hintCookieMode">Tự động sử dụng cookie Threads đã cấu hình sẵn của hệ thống.</div>
          </div>

          <div id="groupCookieProfile" class="form-group" style="display: none; margin-bottom: 10px;">
            <label class="form-label">Chọn Bộ Cookie đã lưu *</label>
            <select id="feedCookieProfile" class="form-select"></select>
          </div>

          <div id="groupCookieCustom" class="form-group" style="display: none; margin-bottom: 0;">
            <label class="form-label">Dán Cookie riêng *</label>
            <textarea id="feedCookieCustom" class="form-textarea" rows="2" placeholder="vd: sessionid=xyz...; token=abc...; hoặc dán JSON từ Cookie-Editor" style="font-family: var(--mono); font-size: 0.76rem;"></textarea>
            
            <div style="margin-top: 8px;">
              <label style="display: flex; align-items: center; gap: 8px; font-size: 0.78rem; color: var(--text-dim); cursor: pointer;">
                <input type="checkbox" id="chkSaveToVault" onchange="document.getElementById('wrapProfileName').style.display = this.checked ? 'block' : 'none';">
                <span style="color: var(--text-muted); font-weight: 500;">Lưu bộ cookie này vào Kho Cookie để tái sử dụng cho các kênh khác</span>
              </label>
              <div id="wrapProfileName" style="display: none; margin-top: 6px;">
                <input type="text" id="vaultProfileName" class="form-input" placeholder="Đặt tên bộ cookie (vd: Threads Acc Phụ, Voz VIP, Báo Trả Phí)">
              </div>
            </div>
          </div>
        </div>

        <div style="display:flex; align-items:center; justify-content:flex-end; gap:10px; margin-top:20px; padding-top:16px; border-top:1px solid var(--card-border);">
          <button type="button" class="btn" onclick="closeModal('modalAdd')">Hủy bỏ</button>
          <button type="submit" id="btnSubmitFeed" class="btn primary">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:15px; height:15px;"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
            <span>Lưu &amp; Kích hoạt Feed</span>
          </button>
        </div>
      </form>
    </div>
  </div>

  <!-- MODAL: COOKIE VAULT MANAGEMENT -->
  <div id="modalCookieVault" class="modal-overlay" onclick="handleModalClick(event, 'modalCookieVault')">
    <div class="modal-card" style="width: min(650px, 100%);">
      <div class="modal-head">
        <div>
          <h2 class="modal-title">Kho Cookie Xác Thực (Cookie Vault)</h2>
          <div style="font-size:0.75rem; color:var(--text-dim); margin-top:2px;">Quản lý các bộ Cookie tái sử dụng cho từng nền tảng hoặc tài khoản riêng biệt.</div>
        </div>
        <button class="modal-close" onclick="closeModal('modalCookieVault')">✕</button>
      </div>

      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px;">
        <span style="font-size:0.75rem; font-weight:700; color:var(--text-dim); text-transform:uppercase;">Danh sách bộ Cookie</span>
        <button class="btn primary" onclick="showAddCookieForm(true)" style="height:30px; font-size:0.74rem;">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:13px; height:13px;"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
          <span>Thêm bộ Cookie mới</span>
        </button>
      </div>

      <!-- Add Cookie Form (collapsible) -->
      <div id="boxAddCookie" style="display:none; background:rgba(7, 12, 24, 0.85); border:1px solid var(--primary-glow); border-radius:14px; padding:14px; margin-bottom:16px;">
        <div style="font-size:0.85rem; font-weight:700; color:#fff; margin-bottom:10px;">Thêm bộ Cookie vào Kho lưu trữ</div>
        <form onsubmit="handleSaveVaultCookie(event)">
          <div class="form-group" style="margin-bottom:10px;">
            <label class="form-label">Tên định danh *</label>
            <input type="text" id="newVaultName" class="form-input" required placeholder="vd: Threads Acc 2, Diễn đàn Voz, Facebook Group">
          </div>
          <div class="form-group" style="margin-bottom:10px;">
            <label class="form-label">Nền tảng / Domain *</label>
            <input type="text" id="newVaultPlatform" class="form-input" required placeholder="vd: threads, voz.vn, tuoitre.vn">
          </div>
          <div class="form-group" style="margin-bottom:10px;">
            <label class="form-label">Chuỗi Cookie *</label>
            <textarea id="newVaultCookie" class="form-textarea" required rows="2" placeholder="sessionid=...; token=...; hoặc dán JSON từ Cookie-Editor" style="font-family:var(--mono); font-size:0.75rem;"></textarea>
          </div>
          <div style="display:flex; justify-content:flex-end; gap:8px;">
            <button type="button" class="btn" onclick="showAddCookieForm(false)" style="height:30px; font-size:0.74rem;">Đóng</button>
            <button type="submit" class="btn primary" style="height:30px; font-size:0.74rem;">Lưu vào Kho</button>
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

    function handleTypeChange(val) {{
      const lbl = document.getElementById('lblTarget');
      const hint = document.getElementById('hintTarget');
      const inp = document.getElementById('feedTarget');
      const optDefault = document.getElementById('optCookieDefault');
      const modeSelect = document.getElementById('feedCookieMode');
      const hintCookie = document.getElementById('hintCookieMode');

      if (val === 'threads') {{
        lbl.innerText = 'Từ khóa / Hashtag Threads *';
        inp.placeholder = 'vd: congnghe, reviewphim, kinhte, manga';
        hint.innerText = 'Nhập hashtag hoặc từ khóa cần cào và lọc link Ebook trên Threads.';
        optDefault.innerText = '🔹 Dùng Cookie Threads mặc định (Hệ thống có sẵn)';
        modeSelect.value = 'default';
        hintCookie.innerText = 'Tự động sử dụng cookie Threads đã cấu hình sẵn của hệ thống.';
      }} else if (val === 'rsshub') {{
        lbl.innerText = 'Route RSSHub Upstream *';
        inp.placeholder = 'vd: telegram/channel/duongdancity, bilibili/ranking/0/3';
        hint.innerText = 'Nhập đường dẫn route được hỗ trợ bởi hệ thống RSSHub.';
        optDefault.innerText = '🌐 Mặc định theo nguồn (Public / Không cookie)';
        modeSelect.value = 'default';
        hintCookie.innerText = 'Truy vấn trực tiếp qua RSSHub Core mà không gửi thêm cookie.';
      }} else if (val === 'custom_rss') {{
        lbl.innerText = 'Đường dẫn URL RSS / Atom ngoài *';
        inp.placeholder = 'vd: https://vnexpress.net/rss/tin-moi-nhat.rss';
        hint.innerText = 'Nhập địa chỉ URL RSS/Atom của bất kỳ website hoặc báo chí nào.';
        optDefault.innerText = '🌐 Mặc định theo nguồn (Public / Không cookie)';
        modeSelect.value = 'default';
        hintCookie.innerText = 'Tải trực tiếp URL công khai.';
      }}
      handleCookieModeChange(modeSelect.value);
    }}

    function handleCookieModeChange(val) {{
      const grpProfile = document.getElementById('groupCookieProfile');
      const grpCustom = document.getElementById('groupCookieCustom');
      const hintCookie = document.getElementById('hintCookieMode');

      grpProfile.style.display = (val === 'profile') ? 'block' : 'none';
      grpCustom.style.display = (val === 'custom') ? 'block' : 'none';

      if (val === 'profile') {{
        hintCookie.innerText = 'Kế thừa bộ cookie đã lưu từ Kho Cookie Vault.';
      }} else if (val === 'custom') {{
        hintCookie.innerText = 'Dán chuỗi cookie riêng chỉ áp dụng cho riêng kênh feed này.';
      }} else if (val === 'none') {{
        hintCookie.innerText = 'Bỏ qua toàn bộ xác thực cookie (truy cập dạng khách).';
      }}
    }}

    function populateProfileDropdown() {{
      fetch('/api/cookies')
        .then(r => r.json())
        .then(profiles => {{
          const sel = document.getElementById('feedCookieProfile');
          sel.innerHTML = '';
          if (!profiles || profiles.length === 0) {{
            sel.innerHTML = '<option value="">(Chưa có bộ cookie nào trong kho)</option>';
            return;
          }}
          profiles.forEach(p => {{
            const opt = document.createElement('option');
            opt.value = p.id;
            opt.innerText = p.name + ' [' + p.platform + '] (' + p.masked + ')';
            sel.appendChild(opt);
          }});
        }});
    }}

    function openCookieVaultModal() {{
      openModal('modalCookieVault');
      renderCookieVault();
    }}

    function showAddCookieForm(show) {{
      document.getElementById('boxAddCookie').style.display = show ? 'block' : 'none';
    }}

    function renderCookieVault() {{
      const box = document.getElementById('vaultList');
      box.innerHTML = '<div style="text-align:center; padding:20px; color:var(--text-dim); font-size:0.8rem;">Đang nạp Kho Cookie...</div>';

      fetch('/api/cookies')
        .then(r => r.json())
        .then(profiles => {{
          if (!profiles || profiles.length === 0) {{
            box.innerHTML = '<div style="text-align:center; padding:20px; color:var(--text-dim); font-size:0.8rem;">Kho cookie hiện đang trống.</div>';
            return;
          }}
          let html = '';
          profiles.forEach(p => {{
            const sysBadge = p.is_system ? '<span class="badge cyan" style="padding:1px 6px; font-size:0.65rem;">System</span>' : '';
            html += `
            <div style="background:rgba(15, 23, 42, 0.7); border:1px solid var(--card-border); border-radius:12px; padding:12px 14px; display:flex; align-items:center; justify-content:space-between; gap:12px;">
              <div>
                <div style="display:flex; align-items:center; gap:8px;">
                  <span style="font-weight:700; font-size:0.85rem; color:#fff;">${{p.name}}</span>
                  <span style="font-size:0.68rem; font-weight:600; padding:2px 6px; border-radius:4px; background:rgba(255,255,255,0.06); color:var(--text-muted); text-transform:uppercase;">${{p.platform}}</span>
                  ${{sysBadge}}
                </div>
                <div style="font-family:var(--mono); font-size:0.75rem; color:#38bdf8; margin-top:3px;">
                  Cookie: ${{p.masked}}
                </div>
                <div style="font-size:0.7rem; color:var(--text-dim); margin-top:2px;">
                  ${{p.description || 'Không có mô tả'}}
                </div>
              </div>
              <div style="display:flex; align-items:center; gap:6px;">
                ${{!p.is_system ? `<button class="btn danger" onclick="deleteVaultCookie('${{p.id}}')" style="height:28px; padding:0 8px; font-size:0.72rem;">Xóa</button>` : `<span style="font-size:0.7rem; color:var(--text-dim);">Mặc định</span>`}}
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

    function handleSaveVaultCookie(e) {{
      e.preventDefault();
      const payload = {{
        name: document.getElementById('newVaultName').value.trim(),
        platform: document.getElementById('newVaultPlatform').value.trim().toLowerCase(),
        cookie: document.getElementById('newVaultCookie').value.trim()
      }};
      fetch('/api/cookies', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload)
      }})
      .then(r => r.json())
      .then(d => {{
        showToast('Đã lưu thành công bộ cookie vào Kho!');
        showAddCookieForm(false);
        renderCookieVault();
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
        }})
        .catch(err => showToast('Lỗi khi xóa: ' + err, true));
    }}

    function handleSaveFeed(e) {{
      e.preventDefault();
      const btn = document.getElementById('btnSubmitFeed');
      btn.disabled = true;
      btn.innerHTML = '<svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:14px; height:14px; animation:spin 1s linear infinite;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><span>Đang lưu...</span>';

      const payload = {{
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
        vault_profile_name: document.getElementById('vaultProfileName').value.trim()
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
        showToast('Đã lưu thành công kênh feed [' + d.slug + ']!');
        closeModal('modalAdd');
        setTimeout(() => location.reload(), 1000);
      }})
      .catch(err => {{
        showToast('Lỗi: ' + err, true);
        btn.disabled = false;
        btn.innerHTML = 'Lưu & Kích hoạt Feed';
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

        if ext in ("xml", "rss"):
            content = generate_rss_xml(clean_path, posts, BASE_URL, meta)
            return Response(content=content, media_type="application/rss+xml; charset=utf-8")
        elif ext == "json":
            data = generate_json_feed(clean_path, posts, BASE_URL, meta)
            return JSONResponse(content=data, media_type="application/feed+json; charset=utf-8")
        elif ext == "atom":
            content = generate_atom_xml(clean_path, posts, BASE_URL, meta)
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
