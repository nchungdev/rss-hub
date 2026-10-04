import html
from datetime import datetime, timezone
from email.utils import format_datetime

def generate_rss_xml(tag: str, posts: list, base_url: str = "https://rss.data1box.win", feed_meta: dict = None) -> str:
    feed_title = (feed_meta or {}).get("title") or f"Feed - #{tag}"
    feed_link = (feed_meta or {}).get("site_url") or f"https://www.threads.com/search?q={tag}&amp;serp_type=tags"
    feed_self = f"{base_url}/{tag}.xml"
    feed_desc = (feed_meta or {}).get("description") or f"Các bài viết mới nhất từ {feed_title}"
    now_rfc822 = format_datetime(datetime.now(timezone.utc))

    xml_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:dc="http://purl.org/dc/elements/1.1/">',
        '  <channel>',
        f'    <title>{html.escape(feed_title)}</title>',
        f'    <link>{feed_link}</link>',
        f'    <description>{html.escape(feed_desc)}</description>',
        '    <language>vi</language>',
        f'    <lastBuildDate>{now_rfc822}</lastBuildDate>',
        f'    <atom:link href="{feed_self}" rel="self" type="application/rss+xml" />',
    ]

    for p in posts:
        username = p.get("username", "threads_user")
        text = p.get("text", "").strip()
        taken_at = p.get("taken_at", 0)
        dt = datetime.fromtimestamp(taken_at, tz=timezone.utc)
        pub_date = format_datetime(dt)
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

        html_desc_parts = []
        if gdrive_links:
            html_desc_parts.append('<div style="background:#e8f4fd; border:1px solid #b6d4fe; border-radius:6px; padding:10px; margin:8px 0;">')
            html_desc_parts.append('<strong>📚 Ebook Google Drive:</strong><br/>')
            for glink in gdrive_links:
                label = preview_title or "Tải Ebook từ Google Drive"
                html_desc_parts.append(f'<p style="margin:4px 0;"><a href="{html.escape(glink)}" target="_blank" style="color:#0d6efd; font-weight:bold;">📥 {html.escape(label)}</a></p>')
            html_desc_parts.append('</div>')
        elif preview_title:
            html_desc_parts.append('<div style="background:#f8f9fa; border:1px solid #dee2e6; border-radius:6px; padding:8px; margin:8px 0;">')
            html_desc_parts.append(f'<strong>🔗 Đính kèm:</strong> {html.escape(preview_title)}')
            html_desc_parts.append('</div>')

        if text:
            escaped_text = html.escape(text).replace("\n", "<br/>\n")
            html_desc_parts.append(f"<p>{escaped_text}</p>")

        for img in images:
            html_desc_parts.append(f'<p><img src="{html.escape(img)}" style="max-width:100%; height:auto;" /></p>')

        html_desc_parts.append(f'<p><a href="{post_url}" target="_blank" style="color:#555;">🔗 Xem bài viết trên Threads</a></p>')
        item_desc = "".join(html_desc_parts)

        xml_lines.append('    <item>')
        xml_lines.append(f'      <title>{html.escape(item_title)}</title>')
        xml_lines.append(f'      <link>{post_url}</link>')
        xml_lines.append(f'      <guid isPermaLink="true">{post_url}</guid>')
        xml_lines.append(f'      <dc:creator>@{html.escape(username)}</dc:creator>')
        xml_lines.append(f'      <pubDate>{pub_date}</pubDate>')
        xml_lines.append(f'      <description><![CDATA[{item_desc}]]></description>')
        xml_lines.append('    </item>')

    xml_lines.append('  </channel>')
    xml_lines.append('</rss>')

    return "\n".join(xml_lines)
