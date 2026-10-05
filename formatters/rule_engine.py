import os
import re
import json
import html
import logging
from datetime import datetime, timezone

logger = logging.getLogger("rsshub.rule_engine")

CONFIG_DIR = os.getenv("CONFIG_DIR", "/app/config")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
RULES_FILE = os.path.join(CONFIG_DIR, "rules.json")
BACKUP_RULES_FILE = os.path.join(DATA_DIR, "rules.json")

BUILTIN_RULES = {
    "clean_ads": {
        "id": "clean_ads",
        "name": "Xóa Quảng cáo & Link Rút gọn",
        "description": "Tự động xóa các đoạn text quảng cáo, tiếp thị liên kết (Shopee, Lazada, bit.ly, t.co...) và bài PR.",
        "rule_type": "replace",
        "target_field": "both",
        "pattern": r"(?i)(\[?(quảng cáo|qc|ad|ads|tài trợ|ưu đãi|mua hàng ngay)\]?:?.*?(\n|$)|https?://(shope\.ee|s\.lazada\.vn|tiki\.vn|vt\.tiktok\.com|bit\.ly|tinyurl\.com|t\.co)/\S+)",
        "replacement": "",
        "is_regex": True,
        "case_sensitive": False,
        "enabled": True,
        "builtin": True,
        "updated_at": "2026-10-05T00:00:00Z"
    },
    "extract_drive_ebook": {
        "id": "extract_drive_ebook",
        "name": "Bóc tách Google Drive & File Ebook",
        "description": "Tự động phát hiện liên kết Google Drive, Fshare, Mega và file sách (.epub, .pdf, .mobi) để đóng hộp tải về.",
        "rule_type": "extract_links",
        "link_types": ["gdrive", "fshare", "mega", "custom"],
        "custom_regex": r"https?://[^\s\"'>]+?\.(epub|pdf|mobi|azw3|cbr|cbz)",
        "action": "enclosure",
        "auto_tag": "[Ebook]",
        "enabled": True,
        "builtin": True,
        "updated_at": "2026-10-05T00:00:00Z"
    },
    "sanitize_html_clean": {
        "id": "sanitize_html_clean",
        "name": "Làm sạch HTML & Loại bỏ Tracking",
        "description": "Loại bỏ thẻ script, iframe, noscript, style nhúng và tracking pixels, chuẩn hóa hiển thị sạch.",
        "rule_type": "format",
        "clean_html": True,
        "strip_tags": "script,style,iframe,embed,object,noscript",
        "truncate_content": 0,
        "prefix_title": "",
        "suffix_title": "",
        "enabled": True,
        "builtin": True,
        "updated_at": "2026-10-05T00:00:00Z"
    },
    "exclude_spam_giveaway": {
        "id": "exclude_spam_giveaway",
        "name": "Lọc bỏ Bài viết Rác / Minigame",
        "description": "Tự động bỏ qua không xuất bản các bài viết chứa từ khóa giveaway, trúng thưởng, minigame, ctv online.",
        "rule_type": "filter",
        "condition": "exclude",
        "target_field": "all",
        "pattern": "giveaway,minigame,trúng thưởng,tuyển dụng gấp,ctv online,việc nhẹ lương cao",
        "is_regex": False,
        "case_sensitive": False,
        "enabled": True,
        "builtin": True,
        "updated_at": "2026-10-05T00:00:00Z"
    }
}

def get_rules_path() -> str:
    for d in [CONFIG_DIR, DATA_DIR]:
        if os.path.exists(d):
            return os.path.join(d, "rules.json")
    local_data = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
    if os.path.exists(local_data):
        return os.path.join(local_data, "rules.json")
    os.makedirs(local_data, exist_ok=True)
    return os.path.join(local_data, "rules.json")

def load_rules() -> dict:
    path = get_rules_path()
    rules = dict(BUILTIN_RULES)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                saved = json.load(f)
                if isinstance(saved, dict):
                    rules.update(saved)
        except Exception as e:
            logger.warning(f"Failed to read rules from {path}: {e}")
    else:
        # First save with builtins
        save_rules(rules)
    return rules

def save_rules(rules: dict):
    path = get_rules_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rules, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save rules to {path}: {e}")

def get_rule_by_id(rule_id: str) -> dict:
    rules = load_rules()
    return rules.get(rule_id)

def save_rule(rule_dict: dict) -> str:
    rules = load_rules()
    r_id = rule_dict.get("id") or ""
    if not r_id:
        name = rule_dict.get("name", "rule")
        r_id = re.sub(r"[^a-z0-9]+", "_", name.lower().strip()).strip("_")
        if not r_id or r_id in rules:
            r_id = f"{r_id}_{int(datetime.now(timezone.utc).timestamp())}"
    
    rule_dict["id"] = r_id
    rule_dict["updated_at"] = datetime.now(timezone.utc).isoformat()
    if "enabled" not in rule_dict:
        rule_dict["enabled"] = True
    if "builtin" not in rule_dict:
        rule_dict["builtin"] = False

    rules[r_id] = rule_dict
    save_rules(rules)
    return r_id

def delete_rule(rule_id: str) -> bool:
    rules = load_rules()
    if rule_id in rules:
        del rules[rule_id]
        save_rules(rules)
        return True
    return False

def get_rules_summary(feeds: dict = None) -> list:
    rules = load_rules()
    summary = []

    # Calculate rule usage across feeds
    feed_usage = {}
    if feeds:
        for f_slug, f_meta in feeds.items():
            applied = f_meta.get("applied_rules") or []
            for r_id in applied:
                feed_usage.setdefault(r_id, []).append(f_slug)

    for r_id, r in rules.items():
        used_by = feed_usage.get(r_id, [])
        r_copy = dict(r)
        r_copy["id"] = r_id
        r_copy["used_by"] = used_by
        r_copy["used_count"] = len(used_by)
        summary.append(r_copy)

    # Sort: builtins first, then by name
    summary.sort(key=lambda x: (not x.get("builtin", False), x.get("name", "").lower()))
    return summary

# ==========================================
# RULE EXECUTION ENGINE
# ==========================================

def apply_replace_rule(post: dict, rule: dict) -> dict:
    target_field = rule.get("target_field", "both")
    pattern = rule.get("pattern", "")
    replacement = rule.get("replacement", "")
    is_regex = rule.get("is_regex", False)
    case_sensitive = rule.get("case_sensitive", False)

    if not pattern:
        return post

    flags = 0 if case_sensitive else re.IGNORECASE

    def do_replace(text: str) -> str:
        if not text:
            return ""
        try:
            if is_regex:
                return re.sub(pattern, replacement, text, flags=flags)
            else:
                if not case_sensitive:
                    compiled = re.compile(re.escape(pattern), re.IGNORECASE)
                    return compiled.sub(replacement, text)
                return text.replace(pattern, replacement)
        except Exception as e:
            logger.warning(f"Regex error in rule {rule.get('id')}: {e}")
            return text

    # Process title
    if target_field in ("title", "both"):
        for t_key in ("title", "preview_title", "_formatted_title"):
            if t_key in post and post[t_key]:
                post[t_key] = do_replace(str(post[t_key])).strip()

    # Process content/text
    if target_field in ("content", "both"):
        for c_key in ("description", "text", "content", "_formatted_html"):
            if c_key in post and post[c_key]:
                post[c_key] = do_replace(str(post[c_key])).strip()

    return post

def apply_filter_rule(post: dict, rule: dict) -> bool:
    """
    Returns True if post should be KEPT, False if dropped.
    """
    condition = rule.get("condition", "exclude") # exclude = drop if matched, include = keep only if matched
    target_field = rule.get("target_field", "all")
    pattern = rule.get("pattern", "")
    is_regex = rule.get("is_regex", False)
    case_sensitive = rule.get("case_sensitive", False)
    min_length = int(rule.get("min_length") or 0)
    max_length = int(rule.get("max_length") or 0)
    require_images = rule.get("require_images", False)
    require_links = rule.get("require_links", False)

    text = post.get("text") or post.get("description") or post.get("content") or ""
    title = post.get("title") or post.get("preview_title") or post.get("_formatted_title") or ""
    author = post.get("author") or post.get("username") or ""
    images = post.get("images") or post.get("_formatted_images") or []
    url = post.get("url") or post.get("link") or ""

    # Check structural constraints
    if require_images and not images:
        return False

    if require_links and not (url or post.get("gdrive_links") or re.search(r"https?://", text)):
        return False

    if min_length > 0 and len(text) < min_length:
        return False

    if max_length > 0 and len(text) > max_length:
        return False

    if not pattern:
        return True

    # Assemble search space
    if target_field == "title":
        search_target = title
    elif target_field == "content":
        search_target = text
    elif target_field == "author":
        search_target = author
    else:
        search_target = f"{title} {text} {author}"

    flags = 0 if case_sensitive else re.IGNORECASE
    matched = False

    try:
        if is_regex:
            matched = bool(re.search(pattern, search_target, flags=flags))
        else:
            keywords = [k.strip() for k in pattern.split(",") if k.strip()]
            if not case_sensitive:
                st_lower = search_target.lower()
                matched = any(kw.lower() in st_lower for kw in keywords)
            else:
                matched = any(kw in search_target for kw in keywords)
    except Exception as e:
        logger.warning(f"Filter regex error in rule {rule.get('id')}: {e}")
        matched = False

    if condition == "exclude":
        # Drop if matched
        return not matched
    else:
        # Keep only if matched
        return matched

def apply_format_rule(post: dict, rule: dict) -> dict:
    prefix_title = rule.get("prefix_title", "") or rule.get("title_prefix", "")
    suffix_title = rule.get("suffix_title", "") or rule.get("title_suffix", "")
    truncate_len = int(rule.get("truncate_content") or rule.get("truncate_chars") or 0)
    clean_html = rule.get("clean_html", False)
    strip_tags = rule.get("strip_tags", "")

    # Title prefix/suffix
    if prefix_title or suffix_title:
        for t_key in ("title", "preview_title", "_formatted_title"):
            if t_key in post and post[t_key]:
                cur_title = str(post[t_key])
                if prefix_title and not cur_title.startswith(prefix_title):
                    cur_title = f"{prefix_title}{cur_title}"
                if suffix_title and not cur_title.endswith(suffix_title):
                    cur_title = f"{cur_title}{suffix_title}"
                post[t_key] = cur_title

    # Truncate content
    if truncate_len > 0:
        for c_key in ("description", "text", "content"):
            if post.get(c_key):
                t = str(post[c_key])
                if len(t) > truncate_len:
                    post[c_key] = t[:truncate_len] + "..."

    # Clean HTML
    if clean_html or strip_tags:
        tags_to_strip = [t.strip().lower() for t in strip_tags.split(",") if t.strip()]
        if not tags_to_strip:
            tags_to_strip = ["script", "style", "iframe", "embed", "object", "noscript"]

        for tag in tags_to_strip:
            # Strip <tag ...>...</tag> and self-closing
            pattern = rf"(?is)<{tag}\b[^>]*>.*?</{tag}>|<{tag}\b[^>]*/>"
            for c_key in ("description", "text", "content", "_formatted_html"):
                if post.get(c_key):
                    post[c_key] = re.sub(pattern, "", str(post[c_key]))

        # Also strip on* inline event handlers (onerror, onload...)
        for c_key in ("description", "text", "content", "_formatted_html"):
            if post.get(c_key):
                post[c_key] = re.sub(r'(?i)\s+on\w+="[^"]*"', '', str(post[c_key]))
                post[c_key] = re.sub(r"(?i)\s+on\w+='[^']*'", '', str(post[c_key]))

    return post

def apply_extract_links_rule(post: dict, rule: dict) -> dict:
    link_types = rule.get("link_types") or ["gdrive", "fshare", "mega"]
    custom_regex = rule.get("custom_regex", "")
    auto_tag = rule.get("auto_tag", "")

    text = f"{post.get('title', '')} {post.get('preview_title', '')} {post.get('text', '')} {post.get('description', '')} {post.get('content', '')} {post.get('url', '')} {post.get('link', '')}"
    extracted = list(post.get("gdrive_links", []))

    # Google Drive & Google Docs
    if "gdrive" in link_types:
        d_links = re.findall(r"https?://(?:drive\.google\.com/(?:file/d/|folderview\?id=|drive/(?:mobile/)?folders/|open\?id=)[a-zA-Z0-9_-]+|docs\.google\.com/(?:spreadsheets|document)/d/[a-zA-Z0-9_-]+)", text)
        for dl in d_links:
            if dl not in extracted:
                extracted.append(dl)

    # Fshare
    if "fshare" in link_types:
        f_links = re.findall(r"https?://(?:www\.)?fshare\.vn/(?:file|folder)/[a-zA-Z0-9]+", text)
        for fl in f_links:
            if fl not in extracted:
                extracted.append(fl)

    # Mega
    if "mega" in link_types:
        m_links = re.findall(r"https?://mega\.nz/(?:file|folder)/[a-zA-Z0-9_#-]+", text)
        for ml in m_links:
            if ml not in extracted:
                extracted.append(ml)

    # Mediafire
    if "mediafire" in link_types or "custom" in link_types:
        mf_links = re.findall(r"https?://(?:www\.)?mediafire\.com/(?:file|download)/[a-zA-Z0-9_.-]+", text)
        for ml in mf_links:
            if ml not in extracted:
                extracted.append(ml)

    # Custom regex
    if custom_regex:
        try:
            c_links = re.findall(custom_regex, text)
            for cl in c_links:
                if isinstance(cl, tuple):
                    cl = cl[0]
                if cl.startswith("http") and cl not in extracted:
                    extracted.append(cl)
        except Exception as e:
            logger.warning(f"Error extracting custom regex in rule {rule.get('id')}: {e}")

    post["gdrive_links"] = extracted

    # Auto tag title
    if auto_tag and extracted:
        for t_key in ("title", "preview_title", "_formatted_title"):
            if t_key in post and post[t_key]:
                cur_title = str(post[t_key])
                if auto_tag not in cur_title:
                    post[t_key] = f"{auto_tag} {cur_title}".strip()

    return post

def execute_rule(post: dict, rule: dict) -> tuple[dict, bool]:
    """
    Executes a single rule on a post dictionary.
    Returns (modified_post, keep_post_bool).
    """
    if not rule.get("enabled", True):
        return post, True

    rtype = rule.get("rule_type", "replace")
    if rtype == "filter":
        keep = apply_filter_rule(post, rule)
        return post, keep
    elif rtype == "replace":
        post = apply_replace_rule(post, rule)
        return post, True
    elif rtype == "format":
        post = apply_format_rule(post, rule)
        return post, True
    elif rtype == "extract_links":
        post = apply_extract_links_rule(post, rule)
        return post, True

    return post, True

def run_pipeline(posts: list, rule_ids: list = None, custom_rules: list = None, feed_meta: dict = None) -> list:
    """
    Applies the full rule pipeline to a list of posts.
    """
    rule_ids = rule_ids or []
    custom_rules = custom_rules or []
    all_rules_dict = load_rules()

    # Build sequence of rules
    pipeline = []
    for r_id in rule_ids:
        if r_id in all_rules_dict:
            pipeline.append(all_rules_dict[r_id])

    for cr in custom_rules:
        if isinstance(cr, dict):
            pipeline.append(cr)

    if not pipeline:
        return posts

    result = []
    for p in posts:
        post_item = dict(p)
        keep = True
        for rule in pipeline:
            post_item, keep = execute_rule(post_item, rule)
            if not keep:
                break
        if keep:
            result.append(post_item)

    return result
