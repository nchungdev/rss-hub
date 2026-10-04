import json
from datetime import datetime, timezone

def generate_json_feed(tag: str, posts: list, base_url: str = "https://rss.data1box.win") -> dict:
    feed_title = f"Threads - #{tag} (Cộng đồng Book Threads)"
    feed_link = f"https://www.threads.com/search?q={tag}&serp_type=tags"
    feed_self = f"{base_url}/{tag}.json"
    feed_desc = f"Các bài viết mới nhất trong cộng đồng #{tag} trên Threads"

    items = []
    for p in posts:
        username = p.get("username", "threads_user")
        text = p.get("text", "").strip()
        taken_at = p.get("taken_at", 0)
        dt = datetime.fromtimestamp(taken_at, tz=timezone.utc)
        iso_date = dt.isoformat()
        post_url = p.get("url", "")
        images = p.get("images", [])
        gdrive_links = p.get("gdrive_links", [])
        preview_title = p.get("preview_title")

        first_line = text.split("\n")[0].strip() if text else "Bài viết mới"
        if len(first_line) > 100:
            first_line = first_line[:97] + "..."
        
        prefix = ""
        if gdrive_links or (preview_title and any(ext in preview_title.lower() for ext in ['.epub', '.pdf', '.mobi', '.azw'])):
            prefix = "[Ebook] "
        
        item_title = f"{prefix}@{username}: {first_line}"

        # HTML Content
        html_desc_parts = []
        if gdrive_links:
            html_desc_parts.append('<div style="background:#e8f4fd; border:1px solid #b6d4fe; border-radius:6px; padding:10px; margin:8px 0;">')
            html_desc_parts.append('<strong>📚 Ebook Google Drive:</strong><br/>')
            for glink in gdrive_links:
                label = preview_title or "Tải Ebook từ Google Drive"
                html_desc_parts.append(f'<p style="margin:4px 0;"><a href="{glink}" target="_blank" style="color:#0d6efd; font-weight:bold;">📥 {label}</a></p>')
            html_desc_parts.append('</div>')
        elif preview_title:
            html_desc_parts.append('<div style="background:#f8f9fa; border:1px solid #dee2e6; border-radius:6px; padding:8px; margin:8px 0;">')
            html_desc_parts.append(f'<strong>🔗 Đính kèm:</strong> {preview_title}')
            html_desc_parts.append('</div>')

        if text:
            escaped_text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br/>\n")
            html_desc_parts.append(f"<p>{escaped_text}</p>")

        for img in images:
            html_desc_parts.append(f'<p><img src="{img}" style="max-width:100%; height:auto;" /></p>')

        html_desc_parts.append(f'<p><a href="{post_url}" target="_blank" style="color:#555;">🔗 Xem bài viết trên Threads</a></p>')
        content_html = "".join(html_desc_parts)

        item = {
            "id": post_url or p.get("code"),
            "url": post_url,
            "title": item_title,
            "content_text": text,
            "content_html": content_html,
            "date_published": iso_date,
            "author": {
                "name": f"@{username}",
                "url": f"https://www.threads.com/@{username}"
            },
            "tags": [tag]
        }
        if images:
            item["image"] = images[0]
        if gdrive_links:
            item["attachments"] = [{"url": l, "mime_type": "application/octet-stream", "title": preview_title or "Ebook"} for l in gdrive_links]

        items.append(item)

    return {
        "version": "https://jsonfeed.org/version/1.1",
        "title": feed_title,
        "home_page_url": feed_link,
        "feed_url": feed_self,
        "description": feed_desc,
        "language": "vi",
        "items": items
    }
