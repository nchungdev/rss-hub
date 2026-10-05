import html
from datetime import datetime, timezone
from formatters.output_processor import process_posts_for_output

def generate_atom_xml(tag: str, posts: list, base_url: str = "https://rss.data1box.win", feed_meta: dict = None, query_params: dict = None) -> str:
    feed_title = (feed_meta or {}).get("title") or f"Feed - #{tag}"
    feed_target = (feed_meta or {}).get("target", "")
    feed_link = (feed_meta or {}).get("site_url") or (feed_target if feed_target.startswith("http") else "") or f"{base_url}/{tag}"
    feed_self = f"{base_url}/{tag}.atom"
    now_iso = datetime.now(timezone.utc).isoformat()

    processed_posts = process_posts_for_output(posts, feed_meta, query_params)

    xml_lines = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<feed xmlns="http://www.w3.org/2005/Atom">',
        f'  <title>{html.escape(feed_title)}</title>',
        f'  <link href="{feed_link}" />',
        f'  <link rel="self" href="{feed_self}" />',
        f'  <updated>{now_iso}</updated>',
        f'  <id>{feed_link}</id>',
    ]

    for p in processed_posts:
        raw_dt = p.get("_formatted_date")
        if isinstance(raw_dt, str):
            post_iso = raw_dt
        elif hasattr(raw_dt, "isoformat"):
            post_iso = raw_dt.isoformat()
        else:
            post_iso = datetime.now(timezone.utc).isoformat()
        post_url = p.get("url", "")
        item_title = p["_formatted_title"]
        item_desc = p["_formatted_html"]

        if username.startswith("http") or "." in username:
            author_url = f"https://{username}" if not username.startswith("http") else username
            author_name = username
        else:
            author_url = f"https://www.threads.net/@{username}"
            author_name = f"@{username}"

        xml_lines.append('  <entry>')
        xml_lines.append(f'    <title>{html.escape(item_title)}</title>')
        xml_lines.append(f'    <link href="{post_url}" />')
        xml_lines.append(f'    <id>{post_url}</id>')
        xml_lines.append(f'    <updated>{post_iso}</updated>')
        xml_lines.append('    <author>')
        xml_lines.append(f'      <name>{html.escape(author_name)}</name>')
        xml_lines.append(f'      <uri>{html.escape(author_url)}</uri>')
        xml_lines.append('    </author>')
        xml_lines.append(f'    <content type="html"><![CDATA[{item_desc}]]></content>')
        xml_lines.append('  </entry>')

    xml_lines.append('</feed>')
    return "\n".join(xml_lines)
