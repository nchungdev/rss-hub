import json
from datetime import datetime, timezone
from formatters.output_processor import process_posts_for_output

def generate_json_feed(tag: str, posts: list, base_url: str = "https://rss.data1box.win", feed_meta: dict = None, query_params: dict = None) -> dict:
    feed_title = (feed_meta or {}).get("title") or f"Feed - #{tag}"
    feed_target = (feed_meta or {}).get("target", "")
    feed_link = (feed_meta or {}).get("site_url") or (feed_target if feed_target.startswith("http") else "") or f"{base_url}/{tag}"
    feed_self = f"{base_url}/{tag}.json"
    feed_desc = (feed_meta or {}).get("description") or f"Các bài viết mới nhất từ {feed_title}"

    processed_posts = process_posts_for_output(posts, feed_meta, query_params)

    items = []
    for p in processed_posts:
        username = p.get("username", "web_author")
        text = (p.get("text") or "").strip()
        iso_date = p["_formatted_date"].isoformat()
        post_url = p.get("url", "")
        images = p["_formatted_images"]
        gdrive_links = p.get("gdrive_links", [])
        preview_title = p.get("preview_title")
        item_title = p["_formatted_title"]
        content_html = p["_formatted_html"]

        if username.startswith("http") or "." in username:
            author_url = f"https://{username}" if not username.startswith("http") else username
            author_name = username
        else:
            author_url = f"https://www.threads.net/@{username}"
            author_name = f"@{username}"

        item = {
            "id": post_url or p.get("code") or str(p.get("taken_at")),
            "url": post_url,
            "title": item_title,
            "content_text": text,
            "content_html": content_html,
            "date_published": iso_date,
            "author": {
                "name": author_name,
                "url": author_url
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
