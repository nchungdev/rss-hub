import os
import asyncio
import logging
from typing import Optional
from fastapi import FastAPI, Request, Response, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
import httpx

from scrapers.threads import get_or_update_feed
from formatters.rss import generate_rss_xml
from formatters.json_feed import generate_json_feed
from formatters.atom import generate_atom_xml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rsshub.wrapper")

app = FastAPI(title="ClaraOS RSS Hub", description="Unified Multi-source RSS/JSON/Atom Feed Wrapper")

RSSHUB_UPSTREAM = os.getenv("RSSHUB_UPSTREAM", "http://127.0.0.1:1200")
BASE_URL = os.getenv("BASE_URL", "https://rss.data1box.win")
SCRAPE_INTERVAL_MINUTES = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "30"))

CUSTOM_FEEDS = {
    "bookthreads": {
        "title": "Book Threads (Cộng đồng Sách)",
        "description": "Các bài chia sẻ sách, review và link Ebook Google Drive từ cộng đồng Book Threads.",
        "icon": "📚"
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
        last_item = posts[0] if posts else {}
        last_title = last_item.get("text", "").split("\n")[0][:80] if last_item else "Chưa có bài"
        
        feed_cards += f"""
        <div class="bg-gray-800 rounded-xl p-6 border border-gray-700 hover:border-blue-500 transition shadow-lg">
            <div class="flex items-center justify-between mb-3">
                <span class="text-3xl">{meta['icon']}</span>
                <span class="bg-blue-900/50 text-blue-400 text-xs px-2.5 py-1 rounded-full border border-blue-800">{count} bài viết</span>
            </div>
            <h2 class="text-xl font-bold text-white mb-1">{meta['title']}</h2>
            <p class="text-gray-400 text-sm mb-4">{meta['description']}</p>
            <div class="bg-gray-900/60 p-3 rounded-lg mb-4 text-xs text-gray-300">
                <span class="text-gray-500">Mới nhất:</span> {last_title}...
            </div>
            <div class="space-y-2">
                <div class="flex items-center gap-2">
                    <span class="text-xs font-semibold uppercase text-orange-400 w-12">RSS</span>
                    <input type="text" readonly value="{BASE_URL}/{tag}.xml" class="bg-gray-900 text-gray-300 text-xs rounded px-2.5 py-1.5 flex-1 border border-gray-700 select-all" />
                    <button onclick="navigator.clipboard.writeText('{BASE_URL}/{tag}.xml'); alert('Đã copy link RSS!')" class="bg-blue-600 hover:bg-blue-500 text-white text-xs px-2.5 py-1.5 rounded transition">Copy</button>
                </div>
                <div class="flex items-center gap-2">
                    <span class="text-xs font-semibold uppercase text-yellow-400 w-12">JSON</span>
                    <input type="text" readonly value="{BASE_URL}/{tag}.json" class="bg-gray-900 text-gray-300 text-xs rounded px-2.5 py-1.5 flex-1 border border-gray-700 select-all" />
                    <button onclick="navigator.clipboard.writeText('{BASE_URL}/{tag}.json'); alert('Đã copy link JSON Feed!')" class="bg-gray-700 hover:bg-gray-600 text-white text-xs px-2.5 py-1.5 rounded transition">Copy</button>
                </div>
                <div class="flex items-center gap-2">
                    <span class="text-xs font-semibold uppercase text-purple-400 w-12">ATOM</span>
                    <input type="text" readonly value="{BASE_URL}/{tag}.atom" class="bg-gray-900 text-gray-300 text-xs rounded px-2.5 py-1.5 flex-1 border border-gray-700 select-all" />
                    <button onclick="navigator.clipboard.writeText('{BASE_URL}/{tag}.atom'); alert('Đã copy link Atom!')" class="bg-gray-700 hover:bg-gray-600 text-white text-xs px-2.5 py-1.5 rounded transition">Copy</button>
                </div>
            </div>
            <div class="mt-4 pt-3 border-t border-gray-700 flex justify-end">
                <button onclick="fetch('/api/refresh/{tag}').then(r=>r.json()).then(d=>{{alert('Đã cào mới thành công: '+d.count+' bài'); location.reload();}})" class="text-xs text-blue-400 hover:text-blue-300 flex items-center gap-1">
                    🔄 Cào bài mới ngay
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
        <title>ClaraOS RSS Hub</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>📡</text></svg>">
    </head>
    <body class="bg-gray-950 text-gray-100 min-h-screen flex flex-col font-sans">
        <header class="border-b border-gray-800 bg-gray-900/50 backdrop-blur sticky top-0 z-10">
            <div class="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between">
                <div class="flex items-center gap-3">
                    <div class="w-9 h-9 rounded-lg bg-gradient-to-tr from-blue-600 to-indigo-500 flex items-center justify-center text-xl shadow">📡</div>
                    <div>
                        <h1 class="font-bold text-lg text-white leading-tight">ClaraOS RSS Hub</h1>
                        <p class="text-xs text-gray-400">Multi-source Feed Generator &amp; Wrapper</p>
                    </div>
                </div>
                <div class="flex items-center gap-2">
                    <span class="flex h-2.5 w-2.5 rounded-full bg-emerald-500 animate-pulse"></span>
                    <span class="text-xs text-emerald-400 font-medium">Online</span>
                </div>
            </div>
        </header>

        <main class="max-w-6xl mx-auto px-4 py-8 flex-1 w-full">
            <div class="mb-8">
                <h2 class="text-2xl font-extrabold text-white tracking-tight">Kênh Feed Hoạt Động</h2>
                <p class="text-sm text-gray-400 mt-1">Các nguồn RSS được tối ưu riêng biệt kèm bộ lọc Ebook và link tải Google Drive.</p>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {feed_cards}
            </div>

            <div class="mt-12 bg-gray-900/40 rounded-xl p-6 border border-gray-800">
                <h3 class="font-bold text-white mb-2">💡 Hướng dẫn thêm vào App đọc tin (Feedly, NetNewsWire, Miniflux):</h3>
                <ul class="text-sm text-gray-400 space-y-1.5 list-disc list-inside">
                    <li>Copy link <code class="text-orange-400 bg-gray-900 px-1.5 py-0.5 rounded">.xml</code> dán trực tiếp vào mục <strong>Add Feed</strong> của ứng dụng.</li>
                    <li>Sử dụng link <code class="text-yellow-400 bg-gray-900 px-1.5 py-0.5 rounded">.json</code> nếu bạn muốn kết nối dữ liệu vào Bot Telegram hoặc tự động hóa n8n.</li>
                    <li>Dữ liệu được tự động quét và làm mới mỗi <strong>30 phút</strong>.</li>
                </ul>
            </div>
        </main>

        <footer class="border-t border-gray-800 py-6 text-center text-xs text-gray-500">
            Powered by ClaraOS &bull; Antigravity Agent Engine
        </footer>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/{path:path}")
async def handle_feed_or_proxy(path: str, request: Request):
    # Detect requested format
    ext = "xml"
    clean_path = path.strip("/")
    
    if "." in clean_path:
        base, extension = clean_path.rsplit(".", 1)
        extension = extension.lower()
        if extension in ("xml", "rss", "json", "atom"):
            ext = extension
            clean_path = base

    # 1. Custom Feed Routing
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

    # 2. Transparent Proxy to RSSHub Upstream
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
