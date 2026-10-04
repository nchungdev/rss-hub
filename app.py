import os
import asyncio
import logging
from typing import Optional
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
        "icon": "fa-solid fa-book-open-reader",
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
    for tag, meta in CUSTOM_FEEDS.items():
        posts = await asyncio.to_thread(get_or_update_feed, tag, False)
        count = len(posts)
        
        # Recent items preview
        preview_items_html = ""
        for p in posts[:4]:
            user = p.get("username", "user")
            text = p.get("text", "").split("\n")[0][:90]
            gdrive = p.get("gdrive_links", [])
            badge = '<span class="bg-cyan-500/20 text-cyan-300 text-[10px] px-1.5 py-0.5 rounded font-medium border border-cyan-500/30">Ebook Drive</span>' if gdrive else ""
            preview_items_html += f"""
            <div class="p-2.5 rounded-lg bg-slate-900/60 border border-slate-800/80 text-xs flex flex-col gap-1 hover:border-slate-700 transition">
                <div class="flex items-center justify-between text-slate-400 text-[11px]">
                    <span class="font-medium text-slate-300">@{user}</span>
                    {badge}
                </div>
                <div class="text-slate-300 line-clamp-2 leading-relaxed">{text}...</div>
            </div>
            """

        feed_cards += f"""
        <div class="glass-card rounded-2xl p-6 flex flex-col justify-between relative overflow-hidden group">
            <div class="absolute -top-12 -right-12 w-32 h-32 bg-cyan-500/10 rounded-full blur-2xl group-hover:bg-cyan-500/20 transition duration-500"></div>
            
            <div>
                <div class="flex items-start justify-between mb-4">
                    <div class="flex items-center gap-3">
                        <div class="w-12 h-12 rounded-xl bg-gradient-to-tr from-cyan-600/30 to-blue-600/20 border border-cyan-500/30 flex items-center justify-center text-cyan-400 text-xl shadow-lg shadow-cyan-950/50">
                            <i class="{meta['icon']}"></i>
                        </div>
                        <div>
                            <span class="text-[11px] font-semibold uppercase tracking-wider text-cyan-400 block">{meta['category']}</span>
                            <h2 class="text-lg font-bold text-slate-100 group-hover:text-white transition">{meta['title']}</h2>
                        </div>
                    </div>
                    <span class="bg-cyan-500/10 text-cyan-400 text-xs px-2.5 py-1 rounded-full border border-cyan-500/20 font-medium shrink-0 flex items-center gap-1.5">
                        <span class="w-1.5 h-1.5 rounded-full bg-cyan-400 animate-pulse"></span> {count} bài viết
                    </span>
                </div>
                
                <p class="text-slate-400 text-xs leading-relaxed mb-5">{meta['description']}</p>

                <!-- URL Endpoint Rows -->
                <div class="space-y-2 mb-5">
                    <div class="flex items-center gap-2 bg-slate-950/70 p-1.5 rounded-xl border border-slate-800/80 focus-within:border-cyan-500/50 transition">
                        <span class="text-[11px] font-bold tracking-wider px-2 py-1 rounded bg-amber-500/10 text-amber-400 border border-amber-500/20 uppercase w-14 text-center">RSS</span>
                        <input type="text" readonly value="{BASE_URL}/{tag}.xml" class="bg-transparent text-slate-300 font-mono text-xs flex-1 outline-none select-all px-1" />
                        <button onclick="copyLink('{BASE_URL}/{tag}.xml', this)" class="bg-slate-800 hover:bg-slate-700 text-slate-200 hover:text-white px-3 py-1 rounded-lg text-xs font-medium transition flex items-center gap-1 border border-slate-700">
                            <i class="fa-solid fa-copy text-[11px]"></i> Copy
                        </button>
                    </div>

                    <div class="flex items-center gap-2 bg-slate-950/70 p-1.5 rounded-xl border border-slate-800/80 focus-within:border-cyan-500/50 transition">
                        <span class="text-[11px] font-bold tracking-wider px-2 py-1 rounded bg-cyan-500/10 text-cyan-400 border border-cyan-500/20 uppercase w-14 text-center">JSON</span>
                        <input type="text" readonly value="{BASE_URL}/{tag}.json" class="bg-transparent text-slate-300 font-mono text-xs flex-1 outline-none select-all px-1" />
                        <button onclick="copyLink('{BASE_URL}/{tag}.json', this)" class="bg-slate-800 hover:bg-slate-700 text-slate-200 hover:text-white px-3 py-1 rounded-lg text-xs font-medium transition flex items-center gap-1 border border-slate-700">
                            <i class="fa-solid fa-copy text-[11px]"></i> Copy
                        </button>
                    </div>

                    <div class="flex items-center gap-2 bg-slate-950/70 p-1.5 rounded-xl border border-slate-800/80 focus-within:border-cyan-500/50 transition">
                        <span class="text-[11px] font-bold tracking-wider px-2 py-1 rounded bg-purple-500/10 text-purple-400 border border-purple-500/20 uppercase w-14 text-center">ATOM</span>
                        <input type="text" readonly value="{BASE_URL}/{tag}.atom" class="bg-transparent text-slate-300 font-mono text-xs flex-1 outline-none select-all px-1" />
                        <button onclick="copyLink('{BASE_URL}/{tag}.atom', this)" class="bg-slate-800 hover:bg-slate-700 text-slate-200 hover:text-white px-3 py-1 rounded-lg text-xs font-medium transition flex items-center gap-1 border border-slate-700">
                            <i class="fa-solid fa-copy text-[11px]"></i> Copy
                        </button>
                    </div>
                </div>

                <!-- Articles Mini Preview -->
                <div class="space-y-1.5 mb-5">
                    <span class="text-[11px] font-semibold text-slate-400 uppercase tracking-wider block mb-1">
                        <i class="fa-solid fa-clock-rotate-left mr-1"></i> Bài viết gần đây
                    </span>
                    <div class="space-y-1.5">
                        {preview_items_html}
                    </div>
                </div>
            </div>

            <div class="pt-3 border-t border-slate-800/80 flex items-center justify-between text-xs">
                <span class="text-slate-500 text-[11px]"><i class="fa-solid fa-bolt text-cyan-500 mr-1"></i> Quét mỗi 30p</span>
                <button onclick="refreshFeed('{tag}', this)" class="text-cyan-400 hover:text-cyan-300 font-medium flex items-center gap-1.5 py-1 px-2.5 rounded-lg hover:bg-cyan-500/10 transition">
                    <i class="fa-solid fa-rotate"></i> Cào mới ngay
                </button>
            </div>
        </div>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="vi" class="dark">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>RSS Hub &bull; ClaraOS</title>
        <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='8' fill='%2306b6d4'/><path d='M8 24a3 3 0 1 0 0-6 3 3 0 0 0 0 6zm0-10a13 13 0 0 1 13 13h-3a10 10 0 0 0-10-10v-3zm0-6a19 19 0 0 1 19 19h-3A16 16 0 0 0 8 11V8z' fill='white'/></svg>">
        
        <!-- Google Fonts: Plus Jakarta Sans & JetBrains Mono -->
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
        
        <!-- Font Awesome 6.4.0 -->
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
        
        <!-- Tailwind CSS CDN -->
        <script src="https://cdn.tailwindcss.com"></script>
        
        <style>
            :root {{
                --clara-bg: #080c14;
                --clara-surface: #0f172a;
                --clara-surface-card: rgba(15, 23, 42, 0.75);
                --clara-surface-card-hover: rgba(30, 41, 59, 0.85);
                --clara-border: rgba(255, 255, 255, 0.08);
                --clara-border-hover: rgba(255, 255, 255, 0.18);
                --clara-cyan: #06b6d4;
                --clara-font-sans: "Plus Jakarta Sans", system-ui, -apple-system, sans-serif;
                --clara-font-mono: "JetBrains Mono", monospace;
            }}

            body {{
                background-color: var(--clara-bg);
                background-image: 
                    radial-gradient(at 0% 0%, rgba(6, 182, 212, 0.08) 0px, transparent 50%),
                    radial-gradient(at 100% 0%, rgba(99, 102, 241, 0.08) 0px, transparent 50%),
                    radial-gradient(at 50% 100%, rgba(16, 185, 129, 0.05) 0px, transparent 50%);
                background-attachment: fixed;
                font-family: var(--clara-font-sans);
                color: #f8fafc;
            }}

            .font-mono {{ font-family: var(--clara-font-mono); }}

            .glass-panel {{
                background: var(--clara-surface-card);
                backdrop-filter: blur(16px);
                -webkit-backdrop-filter: blur(16px);
                border: 1px solid var(--clara-border);
            }}

            .glass-card {{
                background: var(--clara-surface-card);
                backdrop-filter: blur(16px);
                -webkit-backdrop-filter: blur(16px);
                border: 1px solid var(--clara-border);
                box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
                transition: all 0.25s cubic-bezier(0.16, 1, 0.3, 1);
            }}

            .glass-card:hover {{
                background: var(--clara-surface-card-hover);
                border-color: rgba(6, 182, 212, 0.35);
                box-shadow: 0 12px 40px 0 rgba(6, 182, 212, 0.15);
                transform: translateY(-2px);
            }}

            ::-webkit-scrollbar {{ width: 6px; height: 6px; }}
            ::-webkit-scrollbar-track {{ background: transparent; }}
            ::-webkit-scrollbar-thumb {{ background: rgba(255, 255, 255, 0.15); border-radius: 9999px; }}
            ::-webkit-scrollbar-thumb:hover {{ background: rgba(255, 255, 255, 0.25); }}
        </style>
    </head>
    <body class="min-h-screen flex flex-col antialiased">
        <!-- ClaraOS Header -->
        <header class="glass-panel sticky top-0 z-30 border-b border-slate-800/80">
            <div class="max-w-6xl mx-auto px-4 h-16 flex items-center justify-between">
                <div class="flex items-center gap-3">
                    <a href="{CLARAOS_URL}" class="flex items-center gap-3 group">
                        <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-cyan-600 to-blue-600 flex items-center justify-center text-white shadow-lg shadow-cyan-500/25 group-hover:shadow-cyan-500/40 transition">
                            <i class="fa-solid fa-rss text-lg"></i>
                        </div>
                        <div>
                            <div class="flex items-center gap-2">
                                <span class="font-extrabold text-base tracking-wide bg-gradient-to-r from-white via-slate-100 to-slate-400 bg-clip-text text-transparent">RSS Hub</span>
                                <span class="text-[10px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded bg-cyan-500/20 text-cyan-300 border border-cyan-500/30">ClaraOS</span>
                            </div>
                            <span class="text-[11px] text-slate-400 block -mt-0.5 font-normal">Multi-source Feed Generator &amp; Scraper</span>
                        </div>
                    </a>
                </div>

                <div class="flex items-center gap-3">
                    <div class="flex items-center gap-2 px-3 py-1.5 rounded-full bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-xs font-medium">
                        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
                        <span>Gateway Active</span>
                    </div>

                    <a href="{CLARAOS_URL}" class="px-3 py-1.5 rounded-xl bg-slate-800/70 hover:bg-slate-700/80 border border-slate-700/70 text-slate-300 hover:text-white text-xs font-medium transition flex items-center gap-1.5" title="Trở về ClaraOS">
                        <i class="fa-solid fa-arrow-left text-[11px]"></i>
                        <span class="hidden sm:inline">ClaraOS</span>
                    </a>
                </div>
            </div>
        </header>

        <!-- Main Content -->
        <main class="max-w-6xl mx-auto px-4 py-8 flex-1 w-full">
            <div class="flex flex-col md:flex-row md:items-end justify-between mb-8 gap-4">
                <div>
                    <h2 class="text-2xl sm:text-3xl font-extrabold text-white tracking-tight">Kênh Feed Hoạt Động</h2>
                    <p class="text-sm text-slate-400 mt-1">Các nguồn RSS được tối ưu riêng biệt kèm bộ lọc Ebook và link tải Google Drive.</p>
                </div>

                <div class="flex items-center gap-2 text-xs text-slate-400 font-mono bg-slate-900/60 px-3 py-2 rounded-xl border border-slate-800">
                    <i class="fa-solid fa-link text-cyan-400"></i>
                    <span>Subdomain: <strong class="text-slate-200">rss.data1box.win</strong></span>
                </div>
            </div>

            <!-- Cards Grid -->
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {feed_cards}
            </div>

            <!-- Quick Guide / Tips -->
            <div class="mt-10 glass-card rounded-2xl p-6 border border-slate-800/80 relative overflow-hidden">
                <div class="flex items-start gap-4">
                    <div class="w-10 h-10 rounded-xl bg-cyan-500/10 border border-cyan-500/20 flex items-center justify-center text-cyan-400 shrink-0 text-lg">
                        <i class="fa-solid fa-lightbulb"></i>
                    </div>
                    <div class="text-xs text-slate-400 space-y-1.5 flex-1">
                        <h4 class="text-sm font-bold text-white mb-1">Hướng dẫn sử dụng nhanh trong hệ sinh thái ClaraOS:</h4>
                        <p>&bull; <strong>Dành cho app đọc tin (Feedly, NetNewsWire, Miniflux):</strong> Copy đường link định dạng <code class="text-amber-400 font-mono bg-slate-950 px-1 py-0.5 rounded border border-slate-800">.xml</code>.</p>
                        <p>&bull; <strong>Dành cho Automation (n8n, Bot Telegram, Script):</strong> Copy đường link định dạng <code class="text-cyan-400 font-mono bg-slate-950 px-1 py-0.5 rounded border border-slate-800">.json</code> (chuẩn JSON Feed v1.1).</p>
                        <p>&bull; <strong>Tự động nhận diện Ebook:</strong> Các bài đăng chứa link Google Drive hoặc file Ebook (.epub, .pdf) sẽ được tự động đóng khung tải về tiện dụng.</p>
                    </div>
                </div>
            </div>
        </main>

        <footer class="border-t border-slate-800/80 py-6 text-center text-xs text-slate-500">
            ClaraOS &bull; RSS Hub Wrapper Service &bull; Designed with Clara Design System
        </footer>

        <script>
            function copyLink(url, btn) {{
                navigator.clipboard.writeText(url).then(() => {{
                    const orig = btn.innerHTML;
                    btn.innerHTML = '<i class="fa-solid fa-check text-emerald-400"></i> Đã chép!';
                    btn.classList.add('bg-emerald-500/20', 'border-emerald-500/40', 'text-emerald-300');
                    setTimeout(() => {{
                        btn.innerHTML = orig;
                        btn.classList.remove('bg-emerald-500/20', 'border-emerald-500/40', 'text-emerald-300');
                    }}, 2000);
                }});
            }}

            function refreshFeed(tag, btn) {{
                const orig = btn.innerHTML;
                btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Đang cào...';
                btn.disabled = true;
                fetch('/api/refresh/' + tag)
                    .then(r => r.json())
                    .then(d => {{
                        btn.innerHTML = '<i class="fa-solid fa-check"></i> Xong (' + d.count + ')';
                        setTimeout(() => location.reload(), 1000);
                    }})
                    .catch(e => {{
                        btn.innerHTML = '<i class="fa-solid fa-triangle-exclamation text-rose-400"></i> Lỗi';
                        setTimeout(() => {{ btn.innerHTML = orig; btn.disabled = false; }}, 2000);
                    }});
            }}
        </script>
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
