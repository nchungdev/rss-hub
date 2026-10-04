import os
import asyncio
import logging
from typing import Optional
import httpx
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from scrapers.threads import get_or_update_feed
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

CUSTOM_FEEDS = {
    "bookthreads": {
        "title": "Book Threads (Cộng đồng Sách)",
        "category": "Cộng đồng & Sách",
        "description": "Các bài chia sẻ sách, review và link Ebook Google Drive từ cộng đồng Book Threads Việt Nam.",
        "icon": "book",
        "accent": "cyan"
    }
}

async def background_scheduler():
    while True:
        try:
            logger.info("Running periodic scrape for custom feeds...")
            for tag in list(CUSTOM_FEEDS.keys()):
                await asyncio.to_thread(get_or_update_feed, tag, True)
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
        await asyncio.sleep(SCRAPE_INTERVAL_MINUTES * 60)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_scheduler())

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "claraos-rss-hub"}

@app.get("/api/refresh/{tag}")
async def refresh_feed(tag: str):
    clean_tag = tag.lstrip("#").strip().lower()
    posts = await asyncio.to_thread(get_or_update_feed, clean_tag, True)
    return {"status": "ok", "tag": clean_tag, "count": len(posts)}

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    feed_cards = ""
    total_posts = 0

    for tag, meta in CUSTOM_FEEDS.items():
        posts = await asyncio.to_thread(get_or_update_feed, tag, False)
        count = len(posts)
        total_posts += count
        
        # Recent items preview
        preview_items_html = ""
        for p in posts[:4]:
            user = p.get("username", "user")
            raw_text = p.get("text", "").strip()
            first_line = raw_text.split("\n")[0][:110] if raw_text else "Bài viết không có nội dung văn bản"
            gdrive = p.get("gdrive_links", [])
            badge = '<span class="badge cyan" style="padding: 2px 8px; font-size: 0.68rem;"><span class="dot"></span> Ebook Drive</span>' if gdrive else ""
            
            preview_items_html += f"""
            <div style="background: rgba(15, 23, 42, 0.65); border: 1px solid rgba(255, 255, 255, 0.06); border-radius: 12px; padding: 10px 14px; display: flex; flex-direction: column; gap: 4px; transition: border-color 0.2s ease;">
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
        <div class="task-card">
            <div class="task-card-head">
                <div style="display: flex; align-items: center; gap: 14px;">
                    <div class="brand-icon" style="background: linear-gradient(135deg, rgba(14, 165, 233, 0.25), rgba(99, 102, 241, 0.25)); border: 1px solid rgba(14, 165, 233, 0.3);">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 22px; height: 22px; color: #38bdf8;">
                            <path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/>
                            <path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/>
                        </svg>
                    </div>
                    <div>
                        <div style="font-size: 0.7rem; font-weight: 700; color: #38bdf8; text-transform: uppercase; letter-spacing: 0.05em;">{meta['category']}</div>
                        <div class="task-title" style="margin-top: 2px;">{meta['title']}</div>
                    </div>
                </div>

                <div class="badge green">
                    <span class="dot"></span>
                    <span>{count} bài viết</span>
                </div>
            </div>

            <div style="color: var(--text-muted); font-size: 0.8rem; line-height: 1.5; margin: 4px 0 8px 0;">
                {meta['description']}
            </div>

            <!-- Feed URL Rows -->
            <div style="display: flex; flex-direction: column; gap: 8px;">
                <!-- RSS 2.0 -->
                <div style="display: flex; align-items: center; gap: 8px; background: rgba(7, 12, 24, 0.8); border: 1px solid var(--card-border); border-radius: 12px; padding: 6px 10px;">
                    <span style="font-size: 0.7rem; font-weight: 700; padding: 4px 8px; border-radius: 6px; background: rgba(245, 158, 11, 0.12); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); text-align: center; width: 62px;">RSS 2.0</span>
                    <input type="text" readonly value="{BASE_URL}/{tag}.xml" style="background: transparent; border: none; outline: none; color: #cbd5e1; font-family: var(--mono); font-size: 0.78rem; flex: 1; min-width: 0;" />
                    <button class="btn" onclick="copyLink('{BASE_URL}/{tag}.xml', this)" style="height: 30px; padding: 0 10px; font-size: 0.74rem;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/></svg>
                        <span>Sao chép</span>
                    </button>
                </div>

                <!-- JSON Feed -->
                <div style="display: flex; align-items: center; gap: 8px; background: rgba(7, 12, 24, 0.8); border: 1px solid var(--card-border); border-radius: 12px; padding: 6px 10px;">
                    <span style="font-size: 0.7rem; font-weight: 700; padding: 4px 8px; border-radius: 6px; background: rgba(14, 165, 233, 0.12); color: #38bdf8; border: 1px solid rgba(14, 165, 233, 0.3); text-align: center; width: 62px;">JSON</span>
                    <input type="text" readonly value="{BASE_URL}/{tag}.json" style="background: transparent; border: none; outline: none; color: #cbd5e1; font-family: var(--mono); font-size: 0.78rem; flex: 1; min-width: 0;" />
                    <button class="btn" onclick="copyLink('{BASE_URL}/{tag}.json', this)" style="height: 30px; padding: 0 10px; font-size: 0.74rem;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/></svg>
                        <span>Sao chép</span>
                    </button>
                </div>

                <!-- ATOM Feed -->
                <div style="display: flex; align-items: center; gap: 8px; background: rgba(7, 12, 24, 0.8); border: 1px solid var(--card-border); border-radius: 12px; padding: 6px 10px;">
                    <span style="font-size: 0.7rem; font-weight: 700; padding: 4px 8px; border-radius: 6px; background: rgba(139, 92, 246, 0.12); color: #c084fc; border: 1px solid rgba(139, 92, 246, 0.3); text-align: center; width: 62px;">ATOM</span>
                    <input type="text" readonly value="{BASE_URL}/{tag}.atom" style="background: transparent; border: none; outline: none; color: #cbd5e1; font-family: var(--mono); font-size: 0.78rem; flex: 1; min-width: 0;" />
                    <button class="btn" onclick="copyLink('{BASE_URL}/{tag}.atom', this)" style="height: 30px; padding: 0 10px; font-size: 0.74rem;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/></svg>
                        <span>Sao chép</span>
                    </button>
                </div>
            </div>

            <!-- Recent Items Box -->
            <div style="margin-top: 10px;">
                <div style="font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-dim); margin-bottom: 8px; display: flex; align-items: center; gap: 6px;">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                    <span>Bài viết vừa cào gần đây</span>
                </div>
                <div style="display: grid; grid-template-columns: 1fr; gap: 8px;">
                    {preview_items_html}
                </div>
            </div>

            <!-- Card Action Footer -->
            <div class="task-card-footer" style="margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--card-border);">
                <div style="display: flex; align-items: center; gap: 8px; font-size: 0.74rem; color: var(--text-dim);">
                    <span class="dot" style="background: #0ea5e9;"></span>
                    <span>Tự động quét mỗi {SCRAPE_INTERVAL_MINUTES} phút &bull; Tương thích Miniflux, Feedly, Telegram</span>
                </div>
                <div class="task-actions-row">
                    <button class="btn primary" onclick="refreshFeed('{tag}', this)">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 15px; height: 15px;"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
                        <span>Cào mới ngay</span>
                    </button>
                </div>
            </div>
        </div>
        """

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

    /* ClaraOS Sidebar */
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

    /* Controls Bar & Filter Pills (Standardized Suite Template) */
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
      <a href="#bookthreads" class="nav-item">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/></svg>
        <span>Sách &amp; Ebooks</span>
      </a>
      <a href="javascript:void(0)" onclick="refreshFeed('bookthreads', document.getElementById('btnRefreshAll'))" class="nav-item">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
        <span>Cào mới tức thì</span>
      </a>

      <div class="nav-section-title" style="margin-top: 14px;">HỆ THỐNG CLARAOS</div>
      <a href="{CLARAOS_URL}" target="_blank" class="nav-item">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
        <span>ClaraOS Portal</span>
      </a>
      <a href="https://debrid.data1box.win" target="_blank" class="nav-item">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
        <span>Debrid Manager</span>
      </a>
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
        <h1 class="page-title">Kênh Feed &amp; Tự Động Hóa</h1>
      </div>

      <div style="display: flex; align-items: center; gap: 10px; flex-shrink: 0;">
        <button id="btnRefreshAll" class="btn primary" onclick="refreshFeed('bookthreads', this)">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 16px; height: 16px;"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
          <span>Làm mới tất cả</span>
        </button>
        <a class="btn" href="{CLARAOS_URL}" target="_blank" title="Về ClaraOS Portal">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 16px; height: 16px;"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
          <span>ClaraOS</span>
        </a>
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
            <div class="val">1 Kênh</div>
            <div class="desc">Threads &amp; Ebooks VN</div>
          </div>
        </div>

        <div class="stat-card">
          <div class="stat-card-icon stat-emerald">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg>
          </div>
          <div class="stat-info">
            <div class="label">Bài viết đã cào</div>
            <div class="val">{total_posts} Bài</div>
            <div class="desc">Phân loại Ebook Drive</div>
          </div>
        </div>

        <div class="stat-card">
          <div class="stat-card-icon stat-violet">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
          </div>
          <div class="stat-info">
            <div class="label">Chu kỳ quét</div>
            <div class="val">{SCRAPE_INTERVAL_MINUTES} Phút</div>
            <div class="desc">Tự động chạy ngầm</div>
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

      <!-- Controls Bar & Filter Pills (Standardized Suite Template) -->
      <div class="controls-bar">
        <div class="filter-pills">
          <button class="pill active" onclick="filterFeeds('all', this)">
            <span>Tất cả Feeds</span>
            <span class="pill-count">1</span>
          </button>
          <button class="pill" onclick="filterFeeds('books', this)">
            <span>Sách &amp; Ebooks</span>
            <span class="pill-count">1</span>
          </button>
          <button class="pill" onclick="filterFeeds('threads', this)">
            <span>Threads VN</span>
            <span class="pill-count">1</span>
          </button>
        </div>

        <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
          <button class="btn" onclick="copyLink('{BASE_URL}/bookthreads.xml', this)" title="Sao chép link RSS 2.0 nhanh">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/></svg>
            <span>Sao chép RSS URL</span>
          </button>
          <button class="btn" onclick="location.reload()" title="Làm mới trang">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 14px; height: 14px;"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
          </button>
        </div>
      </div>

      <!-- Task / Feeds List -->
      <div id="viewFeeds" class="task-list">
        {feed_cards}
      </div>

      <!-- Quick Ecosystem Guide Box -->
      <div style="margin-top: 24px; background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 16px; padding: 20px; backdrop-filter: blur(12px);">
        <div style="display: flex; align-items: flex-start; gap: 14px;">
          <div style="width: 40px; height: 40px; border-radius: 10px; background: rgba(14, 165, 233, 0.15); border: 1px solid rgba(14, 165, 233, 0.3); display: flex; align-items: center; justify-content: center; flex-shrink: 0; color: #38bdf8;">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 20px; height: 20px;"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>
          </div>
          <div style="font-size: 0.8rem; color: var(--text-muted); line-height: 1.6;">
            <div style="font-size: 0.92rem; font-weight: 700; color: #fff; margin-bottom: 4px;">Hướng dẫn tích hợp ClaraOS RSS Engine</div>
            <div>&bull; <strong>Dành cho app đọc tin (Feedly, NetNewsWire, Miniflux):</strong> Dùng URL định dạng <code style="font-family: var(--mono); color: #fbbf24; background: rgba(0,0,0,0.4); padding: 2px 6px; border-radius: 6px;">.xml</code> hoặc <code style="font-family: var(--mono); color: #c084fc; background: rgba(0,0,0,0.4); padding: 2px 6px; border-radius: 6px;">.atom</code>.</div>
            <div>&bull; <strong>Dành cho Automation (n8n, Bot Telegram, Cron Script):</strong> Dùng URL định dạng <code style="font-family: var(--mono); color: #38bdf8; background: rgba(0,0,0,0.4); padding: 2px 6px; border-radius: 6px;">.json</code> (chuẩn JSON Feed v1.1 RFC).</div>
            <div>&bull; <strong>Bộ lọc Ebook thông minh:</strong> Tự động phân tích metadata, phát hiện link Google Drive và tệp sách .epub / .pdf kèm badge nhận diện.</div>
          </div>
        </div>
      </div>
    </main>

    <!-- Floating Dock Footer (Standardized Suite Template) -->
    <div class="content-footer">
      <div class="footer-stats-strip">
        <div class="footer-stat-chip chip-cyan">
          <span class="chip-label">Kênh:</span>
          <span class="chip-val">1 Active</span>
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

    function refreshFeed(tag, btn) {{
      if (btn) {{
        btn.dataset.origHtml = btn.innerHTML;
        btn.innerHTML = '<svg class="spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 15px; height: 15px; animation: spin 1s linear infinite;"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg><span>Đang cào...</span>';
        btn.disabled = true;
      }}
      fetch('/api/refresh/' + tag)
        .then(r => r.json())
        .then(d => {{
          showToast('Đã cào mới thành công: ' + d.count + ' bài viết');
          if (btn) {{
            btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 15px; height: 15px; color: #34d399;"><polyline points="20 6 9 17 4 12"/></svg><span>Xong (' + d.count + ')</span>';
          }}
          setTimeout(() => location.reload(), 1000);
        }})
        .catch(e => {{
          showToast('Lỗi khi cào dữ liệu: ' + e, true);
          if (btn && btn.dataset.origHtml) {{
            btn.innerHTML = btn.dataset.origHtml;
            btn.disabled = false;
          }}
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

    if clean_path in CUSTOM_FEEDS or clean_path == "bookthreads":
        posts = await asyncio.to_thread(get_or_update_feed, clean_path, False)
        if ext in ("xml", "rss"):
            content = generate_rss_xml(clean_path, posts, BASE_URL)
            return Response(content=content, media_type="application/rss+xml; charset=utf-8")
        elif ext == "json":
            data = generate_json_feed(clean_path, posts, BASE_URL)
            return JSONResponse(content=data, media_type="application/feed+json; charset=utf-8")
        elif ext == "atom":
            content = generate_atom_xml(clean_path, posts, BASE_URL)
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
