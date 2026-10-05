import html
from datetime import datetime, timezone
from email.utils import format_datetime
from formatters.output_processor import process_posts_for_output

def generate_rss_xml(tag: str, posts: list, base_url: str = "https://rss.data1box.win", feed_meta: dict = None, query_params: dict = None) -> str:
    feed_title = (feed_meta or {}).get("title") or f"Feed - #{tag}"
    feed_target = (feed_meta or {}).get("target", "")
    feed_link = (feed_meta or {}).get("site_url") or (feed_target if feed_target.startswith("http") else "") or f"{base_url}/{tag}"
    feed_self = f"{base_url}/{tag}.xml"
    feed_desc = (feed_meta or {}).get("description") or f"Các bài viết mới nhất từ {feed_title}"
    now_rfc822 = format_datetime(datetime.now(timezone.utc))

    processed_posts = process_posts_for_output(posts, feed_meta, query_params)

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

    for p in processed_posts:
        raw_dt = p.get("_formatted_date")
        if isinstance(raw_dt, str):
            try:
                raw_dt = datetime.fromisoformat(raw_dt)
            except Exception:
                raw_dt = datetime.now(timezone.utc)
        elif not isinstance(raw_dt, datetime):
            raw_dt = datetime.now(timezone.utc)
        pub_date = format_datetime(raw_dt)
        post_url = p.get("url", "")
        item_title = p["_formatted_title"]
        creator = p["_formatted_creator"]
        item_desc = p["_formatted_html"]

        xml_lines.append('    <item>')
        xml_lines.append(f'      <title>{html.escape(item_title)}</title>')
        xml_lines.append(f'      <link>{post_url}</link>')
        xml_lines.append(f'      <guid isPermaLink="true">{post_url}</guid>')
        xml_lines.append(f'      <dc:creator>{html.escape(creator)}</dc:creator>')
        xml_lines.append(f'      <pubDate>{pub_date}</pubDate>')
        xml_lines.append(f'      <description><![CDATA[{item_desc}]]></description>')
        xml_lines.append('    </item>')

    xml_lines.append('  </channel>')
    xml_lines.append('</rss>')

    return "\n".join(xml_lines)
