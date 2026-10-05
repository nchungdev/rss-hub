import html
from datetime import datetime, timezone

def process_posts_for_output(posts: list, feed_meta: dict = None, query_params: dict = None) -> list:
    """
    Applies custom output configurations and query parameters to posts:
    - Keyword filtering (include / exclude)
    - Output post limit
    - Title templating ({title}, {author}, {category}, {date})
    - Content body formatting (full vs summary, images, source link, enclosures)
    """
    feed_meta = feed_meta or {}
    query_params = query_params or {}
    custom_cfg = feed_meta.get("custom_output") or {}

    # 1. Resolve parameters (URL Query Params override feed_meta configuration)
    raw_limit = query_params.get("limit") or custom_cfg.get("limit") or 0
    try:
        limit = int(raw_limit)
    except (ValueError, TypeError):
        limit = 0

    filter_inc_str = query_params.get("filter") or query_params.get("include") or custom_cfg.get("filter_include", "")
    filter_exc_str = query_params.get("exclude") or custom_cfg.get("filter_exclude", "")

    inc_keywords = [k.strip().lower() for k in filter_inc_str.split(",") if k.strip()]
    exc_keywords = [k.strip().lower() for k in filter_exc_str.split(",") if k.strip()]

    content_mode = query_params.get("mode") or custom_cfg.get("content_mode", "full")
    if query_params.get("full") == "1":
        content_mode = "full"
    elif query_params.get("summary") == "1":
        content_mode = "summary"

    # Boolean flags
    include_images = custom_cfg.get("include_images", True)
    if "images" in query_params:
        include_images = query_params.get("images") in ("1", "true", "yes")

    include_source = custom_cfg.get("include_source_link", True)
    if "source" in query_params:
        include_source = query_params.get("source") in ("1", "true", "yes")

    include_enclosures = custom_cfg.get("include_enclosures", True)
    if "enclosures" in query_params:
        include_enclosures = query_params.get("enclosures") in ("1", "true", "yes")

    title_template = custom_cfg.get("title_template", "").strip()

    category = feed_meta.get("category", "")

    # 2. Filter posts
    filtered = []
    for p in posts:
        text = p.get("text", "") or ""
        preview_title = p.get("preview_title") or ""
        searchable_text = f"{preview_title} {text}".lower()

        # Exclude filter
        if exc_keywords and any(kw in searchable_text for kw in exc_keywords):
            continue

        # Include filter
        if inc_keywords and not any(kw in searchable_text for kw in inc_keywords):
            continue

        filtered.append(p)

    # 3. Limit posts
    if limit > 0:
        filtered = filtered[:limit]

    # 4. Transform posts for formatting
    processed = []
    for p in filtered:
        username = p.get("username", "web_author")
        text = (p.get("text") or "").strip()
        taken_at = p.get("taken_at", 0)
        try:
            dt = datetime.fromtimestamp(taken_at, tz=timezone.utc)
        except Exception:
            dt = datetime.now(timezone.utc)
        date_str = dt.strftime("%Y-%m-%d %H:%M")

        post_url = p.get("url", "")
        images = p.get("images", []) if include_images else []
        gdrive_links = p.get("gdrive_links", [])
        preview_title = p.get("preview_title")

        first_line = text.split("\n")[0].strip() if text else "Bài viết mới"
        if len(first_line) > 100:
            first_line = first_line[:97] + "..."

        base_raw_title = preview_title or first_line

        # Title template processing
        if title_template:
            item_title = title_template.replace("{title}", base_raw_title)
            item_title = item_title.replace("{author}", username)
            item_title = item_title.replace("{category}", category)
            item_title = item_title.replace("{date}", date_str)
        else:
            # Smart default prefixing
            prefix = ""
            if gdrive_links or (preview_title and any(ext in preview_title.lower() for ext in ['.epub', '.pdf', '.mobi', '.azw'])):
                prefix = "[Ebook] "

            if preview_title:
                item_title = f"{prefix}{preview_title}"
            elif username and not ("." in username or username.startswith("http")):
                item_title = f"{prefix}@{username}: {first_line}"
            else:
                item_title = f"{prefix}{first_line}"

        creator = username if ("." in username or username.startswith("http")) else f"@{username}"

        # HTML Content construction
        html_desc_parts = []

        # Attachment Box
        if include_enclosures:
            if gdrive_links:
                html_desc_parts.append('<div style="background:#e8f4fd; border:1px solid #b6d4fe; border-radius:6px; padding:10px; margin:8px 0;">')
                html_desc_parts.append('<strong>📚 Ebook Google Drive:</strong><br/>')
                for glink in gdrive_links:
                    label = preview_title or "Tải Ebook từ Google Drive"
                    html_desc_parts.append(f'<p style="margin:4px 0;"><a href="{html.escape(glink)}" target="_blank" style="color:#0d6efd; font-weight:bold;">📥 {html.escape(label)}</a></p>')
                html_desc_parts.append('</div>')
            elif preview_title and preview_title != first_line:
                html_desc_parts.append('<div style="background:#f8f9fa; border:1px solid #dee2e6; border-radius:6px; padding:8px; margin:8px 0;">')
                html_desc_parts.append(f'<strong>🔗 Đính kèm:</strong> {html.escape(preview_title)}')
                html_desc_parts.append('</div>')

        # Text Body (Full vs Summary)
        if text:
            if content_mode == "summary":
                summary_text = text[:280] + ("..." if len(text) > 280 else "")
                escaped_text = html.escape(summary_text).replace("\n", "<br/>\n")
            else:
                escaped_text = html.escape(text).replace("\n", "<br/>\n")
            html_desc_parts.append(f"<p>{escaped_text}</p>")

        # Images
        if include_images:
            for img in images:
                html_desc_parts.append(f'<p><img src="{html.escape(img)}" style="max-width:100%; height:auto;" /></p>')

        # Source link
        if include_source and post_url:
            html_desc_parts.append(f'<p><a href="{post_url}" target="_blank" style="color:#555;">🔗 Xem bài viết gốc</a></p>')

        item_desc = "".join(html_desc_parts)

        # Clone and enrich original post item
        item_data = dict(p)
        item_data["_formatted_title"] = item_title
        item_data["_formatted_creator"] = creator
        item_data["_formatted_html"] = item_desc
        item_data["_formatted_images"] = images
        item_data["_formatted_date"] = dt
        processed.append(item_data)

    return processed
