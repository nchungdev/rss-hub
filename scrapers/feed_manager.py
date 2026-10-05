import os
import json
import re
import html
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET
import httpx

from scrapers.threads import get_or_update_feed as get_threads_feed

logger = logging.getLogger("rsshub.feed_manager")

CONFIG_DIR = os.getenv("CONFIG_DIR", "/app/config")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
RSSHUB_UPSTREAM = os.getenv("RSSHUB_UPSTREAM", "http://127.0.0.1:1200")

CONFIG_FILE = os.path.join(CONFIG_DIR, "feeds.json")
BACKUP_CONFIG_FILE = os.path.join(DATA_DIR, "feeds.json")

DEFAULT_FEEDS = {
    "bookthreads": {
        "slug": "bookthreads",
        "title": "Book Threads (Cộng đồng Sách)",
        "type": "threads",
        "target": "bookthreads",
        "category": "Cộng đồng & Sách",
        "description": "Các bài chia sẻ sách, review và link Ebook Google Drive từ cộng đồng Book Threads Việt Nam.",
        "icon": "book",
        "site_url": "https://www.threads.com/search?q=bookthreads&serp_type=tags",
        "cookie_mode": "profile",
        "cookie_profile": "threads_main",
        "created_at": "2026-10-04T12:00:00Z"
    }
}

def get_config_path() -> str:
    if os.path.exists(CONFIG_DIR):
        return CONFIG_FILE
    return BACKUP_CONFIG_FILE

def load_feeds() -> dict:
    path = get_config_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and data:
                    return data
        except Exception as e:
            logger.error(f"Error loading feeds config from {path}: {e}")
    
    # Save default if not existing
    save_feeds(DEFAULT_FEEDS)
    return DEFAULT_FEEDS.copy()

def save_feeds(feeds: dict):
    path = get_config_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(feeds, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(feeds)} feeds to {path}")
    except Exception as e:
        logger.error(f"Error saving feeds config to {path}: {e}")

def sanitize_slug(slug: str) -> str:
    slug = slug.strip().lower()
    slug = re.sub(r'[^a-z0-9_-]', '', slug)
    return slug

def parse_xml_feed(xml_text: str) -> list:
    posts = []
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        logger.warning(f"XML parse error: {e}")
        return posts

    # Try RSS 2.0
    channel = root.find("channel")
    if channel is not None:
        for item in channel.findall("item"):
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            guid = item.findtext("guid", "").strip() or link or title
            desc = item.findtext("description", "").strip()
            pub_date_str = item.findtext("pubDate", "").strip()
            
            author = (
                item.findtext("author", "") or 
                item.findtext("{http://purl.org/dc/elements/1.1/}creator", "") or 
                "RSS"
            ).strip()

            taken_at = int(datetime.now(timezone.utc).timestamp())
            if pub_date_str:
                try:
                    dt = parsedate_to_datetime(pub_date_str)
                    taken_at = int(dt.timestamp())
                except Exception:
                    pass

            # Clean HTML from description for preview
            clean_desc = re.sub(r'<[^>]+>', ' ', desc).strip()
            clean_desc = re.sub(r'\s+', ' ', clean_desc)

            # Find google drive links
            gdrive_links = re.findall(r"https?://(?:drive\.google\.com/[^\s\"'<>]+|docs\.google\.com/[^\s\"'<>]+)", desc)

            # Find images
            images = []
            enclosure = item.find("enclosure")
            if enclosure is not None and "image" in enclosure.attrib.get("type", ""):
                images.append(enclosure.attrib.get("url", ""))

            posts.append({
                "id": guid,
                "username": author,
                "text": f"{title}\n{clean_desc}" if title else clean_desc,
                "taken_at": taken_at,
                "url": link,
                "gdrive_links": list(set(gdrive_links)),
                "images": images,
                "preview_title": title
            })
        return posts

    # Try Atom
    entries = root.findall("{http://www.w3.org/2005/Atom}entry") or root.findall("entry")
    for entry in entries:
        title = (entry.findtext("{http://www.w3.org/2005/Atom}title", "") or entry.findtext("title", "")).strip()
        link_elem = entry.find("{http://www.w3.org/2005/Atom}link") or entry.find("link")
        link = link_elem.attrib.get("href", "").strip() if link_elem is not None else ""
        guid = (entry.findtext("{http://www.w3.org/2005/Atom}id", "") or entry.findtext("id", "")).strip() or link or title
        
        summary = (
            entry.findtext("{http://www.w3.org/2005/Atom}summary", "") or 
            entry.findtext("summary", "") or 
            entry.findtext("{http://www.w3.org/2005/Atom}content", "") or 
            entry.findtext("content", "")
        ).strip()
        
        updated_str = (entry.findtext("{http://www.w3.org/2005/Atom}updated", "") or entry.findtext("updated", "")).strip()
        author_elem = entry.find("{http://www.w3.org/2005/Atom}author") or entry.find("author")
        author = author_elem.findtext("{http://www.w3.org/2005/Atom}name", "") if author_elem is not None else "Atom"

        taken_at = int(datetime.now(timezone.utc).timestamp())
        if updated_str:
            try:
                dt = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                taken_at = int(dt.timestamp())
            except Exception:
                pass

        clean_summary = re.sub(r'<[^>]+>', ' ', summary).strip()
        clean_summary = re.sub(r'\s+', ' ', clean_summary)
        gdrive_links = re.findall(r"https?://(?:drive\.google\.com/[^\s\"'<>]+|docs\.google\.com/[^\s\"'<>]+)", summary)

        posts.append({
            "id": guid,
            "username": author,
            "text": f"{title}\n{clean_summary}" if title else clean_summary,
            "taken_at": taken_at,
            "url": link,
            "gdrive_links": list(set(gdrive_links)),
            "images": [],
            "preview_title": title
        })

    return posts

def format_cookie_headers(cookie_raw: str) -> str:
    if not cookie_raw:
        return ""
    cookie_raw = cookie_raw.strip()
    # Check if user pasted JSON array from Cookie-Editor extension
    if cookie_raw.startswith("[") and cookie_raw.endswith("]"):
        try:
            arr = json.loads(cookie_raw)
            if isinstance(arr, list):
                return "; ".join([f"{item.get('name')}={item.get('value')}" for item in arr if item.get('name')])
        except Exception:
            pass
    return cookie_raw

from scrapers.cookie_vault import resolve_effective_cookie
from scrapers.generic_web import scrape_generic_web

def get_feed_posts(slug: str, force_refresh: bool = False) -> list:
    feeds = load_feeds()
    meta = feeds.get(slug, {})
    feed_type = meta.get("type", "web")
    target = meta.get("target", slug)
    cookie = resolve_effective_cookie(meta)

    cache_file = os.path.join(DATA_DIR, f"history_{slug}.json")

    # If cached and not force_refresh
    if not force_refresh and os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    return list(data.values())
        except Exception:
            pass

    if feed_type == "threads":
        posts = get_threads_feed(target, force_refresh, custom_cookie=cookie)
        if posts:
            try:
                os.makedirs(os.path.dirname(cache_file), exist_ok=True)
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(posts, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.warning(f"Failed to write slug cache for threads {slug}: {e}")
        return posts

    # Generic Web Scraper (HTML to RSS)
    if feed_type in ("web", "generic_web"):
        selectors = meta.get("selectors")
        use_flaresolverr = meta.get("use_flaresolverr", False)
        posts = scrape_generic_web(target, custom_cookie=cookie, selectors=selectors, use_flaresolverr=use_flaresolverr)
        if posts:
            try:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(posts, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error(f"Failed to cache feed {slug}: {e}")
            return posts

    # For RSSHub and Custom RSS
    url = target
    if feed_type == "rsshub":
        target_path = target.lstrip("/")
        url = f"{RSSHUB_UPSTREAM}/{target_path}"

    logger.info(f"Fetching feed [{slug}] type={feed_type} from {url}...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    }
    if cookie:
        formatted_cookie = format_cookie_headers(cookie)
        headers["Cookie"] = formatted_cookie
        logger.info(f"Attached custom authentication cookie to feed [{slug}] request")

    try:
        with httpx.Client(timeout=25.0, headers=headers, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                posts = parse_xml_feed(resp.text)
                if posts:
                    # Save cache
                    try:
                        with open(cache_file, "w", encoding="utf-8") as f:
                            json.dump(posts, f, ensure_ascii=False, indent=2)
                    except Exception as e:
                        logger.error(f"Failed to cache feed {slug}: {e}")
                    return posts
            else:
                logger.warning(f"Feed fetch {url} returned HTTP {resp.status_code}")
    except Exception as e:
        logger.error(f"Error fetching feed [{slug}] from {url}: {e}")

    # Return cached if fetch failed
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    return []
