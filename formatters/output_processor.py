import os
import re
import json
import html
import logging
import urllib.parse
from datetime import datetime, timezone
from formatters.rule_engine import run_pipeline, load_rules

logger = logging.getLogger("rsshub.output_processor")

CONFIG_DIR = os.getenv("CONFIG_DIR", "/app/config")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")

def get_data_dir() -> str:
    for d in [DATA_DIR, CONFIG_DIR]:
        if os.path.exists(d):
            return d
    local_data = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
    os.makedirs(local_data, exist_ok=True)
    return local_data

def get_effective_applied_rules(feed_meta: dict = None) -> list:
    feed_meta = feed_meta or {}
    if "applied_rules" in feed_meta and feed_meta["applied_rules"]:
        return list(feed_meta["applied_rules"])
    # If not explicitly specified on this feed or empty, default to all enabled rules
    all_rules = load_rules()
    return [r_id for r_id, r in all_rules.items() if r.get("enabled", True)]

def get_output_filepath(slug: str) -> str:
    return os.path.join(get_data_dir(), f"output_{slug}.json")

def get_history_filepath(slug: str) -> str:
    return os.path.join(get_data_dir(), f"history_{slug}.json")

def load_feed_output(slug: str) -> dict:
    path = get_output_filepath(slug)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception as e:
            logger.warning(f"Error loading output file {path}: {e}")
    return {}

def save_feed_output(slug: str, output_data: dict):
    path = get_output_filepath(slug)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved task output for [{slug}] ({output_data.get('output_count', 0)} posts) to {path}")
    except Exception as e:
        logger.error(f"Error saving output file {path}: {e}")

def get_raw_feed_posts(slug: str) -> list:
    """Reads raw posts from history_{slug}.json (with fallbacks)."""
    data_dir = get_data_dir()
    path = os.path.join(data_dir, f"history_{slug}.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    return list(data.values())
        except Exception as e:
            logger.warning(f"Error reading {path}: {e}")

    # Fallback search for threads sanitized tags
    if os.path.exists(data_dir):
        for fname in os.listdir(data_dir):
            if fname.startswith("history_") and fname.endswith(".json") and slug in fname:
                try:
                    with open(os.path.join(data_dir, fname), "r", encoding="utf-8") as f:
                        data = json.load(f)
                        posts = data if isinstance(data, list) else list(data.values())
                        if posts:
                            # Normalize and copy to history_{slug}.json
                            try:
                                with open(path, "w", encoding="utf-8") as f_out:
                                    json.dump(posts, f_out, ensure_ascii=False, indent=2)
                            except Exception:
                                pass
                            return posts
                except Exception:
                    pass
    return []

def serialize_post_for_storage(post: dict) -> dict:
    """Ensures datetime and other objects in post are JSON-serializable."""
    item = dict(post)
    if "_formatted_date" in item:
        dt = item["_formatted_date"]
        if hasattr(dt, "isoformat"):
            item["_formatted_date"] = dt.isoformat()
        else:
            item["_formatted_date"] = str(dt)
    return item

def process_posts_for_output(posts: list, feed_meta: dict = None, query_params: dict = None) -> list:
    """
    Applies custom output configurations, rules, and query parameters to posts:
    - Rule Engine Pipeline (replace, conditional filter, format/clean, extract links)
    - Keyword filtering (include / exclude)
    - Output post limit
    - Title templating ({title}, {author}, {category}, {date})
    - Content body formatting (full vs summary, images, source link, enclosures)
    """
    feed_meta = feed_meta or {}
    query_params = query_params or {}
    custom_cfg = feed_meta.get("custom_output") or {}

    # 0. Apply Output Rule Pipeline (Global Rules + Custom Rules)
    applied_rules = feed_meta.get("applied_rules")
    if not applied_rules:
        applied_rules = get_effective_applied_rules(feed_meta)
    custom_rules = feed_meta.get("custom_rules") or []
    if "rules" in query_params:
        applied_rules = [r.strip() for r in query_params["rules"].split(",") if r.strip()]
    
    if applied_rules or custom_rules:
        posts = run_pipeline(posts, rule_ids=applied_rules, custom_rules=custom_rules, feed_meta=feed_meta)

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
            if isinstance(taken_at, (int, float)) and taken_at > 0:
                dt = datetime.fromtimestamp(taken_at, tz=timezone.utc)
            elif isinstance(taken_at, str):
                dt = datetime.fromisoformat(taken_at)
            else:
                dt = datetime.now(timezone.utc)
        except Exception:
            dt = datetime.now(timezone.utc)
        date_str = dt.strftime("%Y-%m-%d %H:%M")

        def clean_threads_redirect(u):
            if not u:
                return ""
            if "l.threads.com" in u and "u=" in u:
                try:
                    parsed_u = urllib.parse.urlparse(u)
                    qs = urllib.parse.parse_qs(parsed_u.query)
                    if "u" in qs and qs["u"]:
                        return qs["u"][0]
                except Exception:
                    pass
            return u

        post_url = p.get("url", "")
        images = p.get("images", []) if include_images else []
        gdrive_links = [clean_threads_redirect(l) for l in p.get("gdrive_links", []) if l]
        preview_title = p.get("preview_title")
        preview_url = clean_threads_redirect(p.get("preview_url"))

        combined_text = f"{text} {p.get('content', '')} {p.get('description', '')}"
        extra_drive = re.findall(r'https?://(?:drive|docs)\.google\.com/[^\s<>"\'\)]+', combined_text)
        for ed in extra_drive:
            if ed not in gdrive_links:
                gdrive_links.append(ed)
        if preview_url and re.match(r'https?://(?:drive|docs)\.google\.com/[^\s<>"\'\)]+', preview_url):
            if preview_url not in gdrive_links:
                gdrive_links.append(preview_url)

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
                if not (base_raw_title and base_raw_title.startswith("[Ebook]")):
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
                html_desc_parts.append('<strong>📚 Ebook Google Drive / Tài liệu:</strong><br/>')
                for glink in gdrive_links:
                    label = preview_title or "Tải Ebook / Tài liệu từ Google Drive"
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

        # Structured attachments (Ebooks, Drive links, direct files, enclosures)
        attachments = []
        seen_attach_urls = set()

        for glink in gdrive_links:
            if glink not in seen_attach_urls:
                seen_attach_urls.add(glink)
                is_sheet = "spreadsheets" in glink
                is_doc = "document" in glink
                default_title = "Google Sheets (Kho Ebook)" if is_sheet else ("Google Docs Tài liệu" if is_doc else "Google Drive Ebook / Tài liệu")
                label = preview_title or default_title
                attachments.append({
                    "type": "gdrive",
                    "title": label,
                    "url": glink,
                    "ext": "gsheet" if is_sheet else ("gdoc" if is_doc else "gdrive")
                })

        cloud_pattern = r'https?://(?:www\.)?(?:mega\.nz|mediafire\.com|fshare\.vn|dropbox\.com|1drv\.ms|onedrive\.live\.com|terabox\.(?:com|app)|box\.com|[a-zA-Z0-9_-]+\.sharepoint\.com)/[^\s<>"\'\)]+'
        found_cloud = re.findall(cloud_pattern, combined_text, re.IGNORECASE)
        if preview_url and re.match(cloud_pattern, preview_url, re.IGNORECASE):
            found_cloud.append(preview_url)
        for clink in found_cloud:
            if clink not in seen_attach_urls:
                seen_attach_urls.add(clink)
                domain = "Cloud Storage"
                if "mega.nz" in clink: domain = "Mega.nz"
                elif "fshare.vn" in clink: domain = "Fshare.vn"
                elif "mediafire.com" in clink: domain = "Mediafire"
                elif "dropbox.com" in clink: domain = "Dropbox"
                elif "onedrive" in clink or "1drv.ms" in clink: domain = "OneDrive"
                elif "sharepoint.com" in clink: domain = "SharePoint"
                elif "terabox" in clink: domain = "TeraBox"
                elif "box.com" in clink: domain = "Box"
                attachments.append({
                    "type": "cloud",
                    "title": preview_title or f"Tài liệu từ {domain}",
                    "url": clink,
                    "ext": "cloud"
                })

        extracted_links = p.get("_extracted_links", [])
        for elink in extracted_links:
            if elink not in seen_attach_urls:
                seen_attach_urls.add(elink)
                ext = elink.split(".")[-1].split("?")[0].lower() if "." in elink else "link"
                attachments.append({
                    "type": "extracted",
                    "title": elink.split("/")[-1].split("?")[0] or "Liên kết trích xuất",
                    "url": elink,
                    "ext": ext
                })

        # Find direct file download links in text (PDF, EPUB, MOBI, AZW, ZIP, RAR, 7Z, DOCX, MP3, MP4)
        file_pattern = r'https?://[^\s<>"\'\)]+\.(?:pdf|epub|mobi|azw3?|zip|rar|7z|docx?|xlsx?|mp3|mp4)(?:\?[^\s<>"\'\)]*)?'
        found_file_links = re.findall(file_pattern, text, re.IGNORECASE)
        for flink in found_file_links:
            if flink not in seen_attach_urls:
                seen_attach_urls.add(flink)
                ext = flink.split(".")[-1].split("?")[0].lower()
                fname = flink.split("/")[-1].split("?")[0]
                attachments.append({
                    "type": "file",
                    "title": fname or f"Tệp tin .{ext}",
                    "url": flink,
                    "ext": ext
                })

        if p.get("enclosure") and isinstance(p["enclosure"], dict) and p["enclosure"].get("url"):
            enc_url = p["enclosure"]["url"]
            if enc_url not in seen_attach_urls:
                seen_attach_urls.add(enc_url)
                enc_type = p["enclosure"].get("type", "")
                ext = enc_url.split(".")[-1].split("?")[0].lower() if "." in enc_url else (enc_type.split("/")[-1] if "/" in enc_type else "file")
                attachments.append({
                    "type": "enclosure",
                    "title": p["enclosure"].get("title") or f"Tệp đính kèm ({ext.upper()})",
                    "url": enc_url,
                    "ext": ext
                })

        # Structured videos
        videos = list(p.get("videos") or [])
        if p.get("video_url") and p["video_url"] not in videos:
            videos.append(p["video_url"])
        if p.get("video") and p["video"] not in videos:
            videos.append(p["video"])
        if p.get("enclosure") and isinstance(p["enclosure"], dict) and p["enclosure"].get("type", "").startswith("video/"):
            if p["enclosure"].get("url") and p["enclosure"]["url"] not in videos:
                videos.append(p["enclosure"]["url"])
        
        # Also extract video from text/html if any <video src="..."> or <iframe src="...">
        raw_combined = f"{text} {p.get('description', '')} {p.get('content', '')}"
        video_srcs = re.findall(r'<(?:video|source|iframe)[^>]+src=["\']([^"\']+)["\']', raw_combined)
        for v in video_srcs:
            if v not in videos:
                videos.append(v)

        # Smart Summary
        raw_text_clean = re.sub(r'<[^>]+>', ' ', text or p.get("summary") or p.get("description") or "").strip()
        raw_text_clean = re.sub(r'\s+', ' ', raw_text_clean)
        
        if len(raw_text_clean) <= 220:
            summary = raw_text_clean
        else:
            match = re.search(r'([.?!])\s', raw_text_clean[:280])
            if match and match.end() > 60:
                summary = raw_text_clean[:match.end()].strip()
            else:
                summary = raw_text_clean[:220].strip() + "..."

        # Clone and enrich original post item
        item_data = dict(p)
        item_data["_formatted_title"] = item_title
        item_data["_formatted_creator"] = creator
        item_data["_formatted_html"] = item_desc
        item_data["_formatted_images"] = images
        item_data["_formatted_videos"] = videos
        item_data["_summary"] = summary
        item_data["_attachments"] = attachments
        item_data["_formatted_date"] = dt
        processed.append(item_data)

    return processed

def process_and_save_feed_output(slug: str, raw_posts: list = None, feed_meta: dict = None) -> dict:
    """
    Applies the full rule pipeline to RAW scraped posts and saves the result
    to output_{slug}.json in DATA_DIR. Also updates feed metrics in feeds.json.
    """
    from scrapers.feed_manager import load_feeds, save_feeds
    feeds = load_feeds()
    if feed_meta is None:
        feed_meta = feeds.get(slug, {})

    if raw_posts is None:
        raw_posts = get_raw_feed_posts(slug)

    applied_rules = get_effective_applied_rules(feed_meta)
    
    # Run through pipeline & formatters
    processed = process_posts_for_output(raw_posts, feed_meta=feed_meta)
    
    # Serialize for JSON storage
    serializable_posts = [serialize_post_for_storage(p) for p in processed]
    
    now_iso = datetime.now(timezone.utc).isoformat()
    output_data = {
        "slug": slug,
        "title": feed_meta.get("title", slug),
        "updated_at": now_iso,
        "raw_count": len(raw_posts),
        "output_count": len(serializable_posts),
        "filtered_count": max(0, len(raw_posts) - len(serializable_posts)),
        "rules_applied": applied_rules,
        "posts": serializable_posts
    }
    
    save_feed_output(slug, output_data)
    
    # Update feed meta in feeds.json
    if slug in feeds:
        feeds[slug]["last_processed_at"] = datetime.now(timezone.utc).timestamp()
        feeds[slug]["raw_post_count"] = len(raw_posts)
        feeds[slug]["output_post_count"] = len(serializable_posts)
        save_feeds(feeds)
        
    return output_data

def reprocess_all_feeds() -> list:
    """
    Re-processes all feeds from their RAW data without re-scraping the web.
    Called when rules are created, updated, or deleted.
    """
    from scrapers.feed_manager import load_feeds
    feeds = load_feeds()
    results = []
    for slug, meta in feeds.items():
        try:
            res = process_and_save_feed_output(slug, feed_meta=meta)
            results.append({
                "slug": slug,
                "title": meta.get("title", slug),
                "raw_count": res.get("raw_count", 0),
                "output_count": res.get("output_count", 0),
                "filtered_count": res.get("filtered_count", 0),
                "rules_applied": res.get("rules_applied", [])
            })
        except Exception as e:
            logger.error(f"Error reprocessing feed {slug}: {e}")
            results.append({"slug": slug, "error": str(e)})
    return results
