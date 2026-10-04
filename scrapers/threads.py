import os
import json
import re
import logging
from datetime import datetime, timezone, timedelta
import requests

logger = logging.getLogger("rsshub.threads")
VN_TZ = timezone(timedelta(hours=7))

DATA_DIR = os.getenv("DATA_DIR", "/app/data")
COOKIE_FILE = os.getenv("COOKIE_FILE", "/app/config/.session_cookie")
FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://127.0.0.1:8191/v1")

def load_session_cookie() -> str:
    # 1. Check env var
    cookie = os.getenv("THREADS_SESSION_COOKIE")
    if cookie:
        return cookie.strip()
    
    # 2. Check local cookie file
    if os.path.exists(COOKIE_FILE):
        try:
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                c = f.read().strip()
                if c: return c
        except Exception as e:
            logger.error(f"Error reading cookie file {COOKIE_FILE}: {e}")
            
    # 3. Check fallback path on NAS
    fallback_path = "/home/chungnh/scripts/threads_rss/.session_cookie"
    if os.path.exists(fallback_path):
        try:
            with open(fallback_path, "r", encoding="utf-8") as f:
                c = f.read().strip()
                if c: return c
        except Exception:
            pass
    return None

def find_posts_recursive(obj):
    posts = []
    if isinstance(obj, dict):
        if "post" in obj and isinstance(obj["post"], dict):
            posts.append(obj["post"])
        elif "caption" in obj and ("user" in obj or "username" in obj):
            posts.append(obj)
        for v in obj.values():
            posts.extend(find_posts_recursive(v))
    elif isinstance(obj, list):
        for item in obj:
            posts.extend(find_posts_recursive(item))
    return posts

def fetch_threads_posts(tag: str = "bookthreads", max_timeout: int = 60000, custom_cookie: str = None) -> list:
    url = f"https://www.threads.com/search?q={tag}&serp_type=tags"
    logger.info(f"Querying FlareSolverr ({FLARESOLVERR_URL}) for {url}...")
    
    session_cookie = custom_cookie or load_session_cookie()
    payload = {
        "cmd": "request.get",
        "url": url,
        "maxTimeout": max_timeout
    }
    
    if session_cookie:
        logger.info("Using session cookie for personalized community feed...")
        val = session_cookie.strip()
        if "sessionid=" in val:
            val = val.split("sessionid=")[1].split(";")[0].strip()
        payload["cookies"] = [
            {
                "name": "sessionid",
                "value": val,
                "domain": ".threads.com"
            }
        ]

    try:
        r = requests.post(FLARESOLVERR_URL, json=payload, timeout=max_timeout/1000 + 20)
        res = r.json()
        if res.get("status") != "ok":
            logger.error(f"FlareSolverr returned error: {res.get('message')}")
            return []
        
        html_doc = res.get("solution", {}).get("response", "")
        if not html_doc:
            logger.warning("Empty response from FlareSolverr")
            return []
    except Exception as e:
        logger.error(f"FlareSolverr connection error: {e}")
        return []

    scripts = re.findall(r'<script type="application/json"[^>]*>(.*?)</script>', html_doc)
    raw_posts = []
    seen_codes = set()

    for s in scripts:
        if "searchResults" in s or "caption" in s or "text_post_app_info" in s:
            try:
                data = json.loads(s)
                candidates = find_posts_recursive(data)
                for c in candidates:
                    code = c.get("code")
                    if code and code not in seen_codes:
                        seen_codes.add(code)
                        raw_posts.append(c)
            except Exception:
                pass

    parsed_posts = []
    for p in raw_posts:
        code = p.get("code")
        user = p.get("user", {})
        username = user.get("username", "threads_user") if isinstance(user, dict) else "threads_user"
        full_name = user.get("full_name", username) if isinstance(user, dict) else username
        
        caption_data = p.get("caption")
        text = ""
        if isinstance(caption_data, dict):
            text = caption_data.get("text", "")
        elif isinstance(caption_data, str):
            text = caption_data

        taken_at = p.get("taken_at") or int(datetime.now().timestamp())
        
        text_post_app_info = p.get("text_post_app_info", {})
        link_preview = text_post_app_info.get("link_preview_attachment") if isinstance(text_post_app_info, dict) else None
        
        gdrive_links = re.findall(r'https?://drive\.google\.com/[^\s<>"]+', text)
        preview_title = link_preview.get("title") if link_preview else None
        preview_url = link_preview.get("url") if link_preview else None

        images = []
        if "image_versions2" in p:
            cands = p["image_versions2"].get("candidates", [])
            if cands:
                images.append(cands[0].get("url"))
        elif "carousel_media" in p:
            for cm in p.get("carousel_media", []):
                c_cands = cm.get("image_versions2", {}).get("candidates", [])
                if c_cands:
                    images.append(c_cands[0].get("url"))

        post_url = f"https://www.threads.com/t/{code}"

        parsed_posts.append({
            "code": code,
            "username": username,
            "full_name": full_name,
            "text": text,
            "taken_at": taken_at,
            "url": post_url,
            "images": images,
            "gdrive_links": gdrive_links,
            "preview_title": preview_title,
            "preview_url": preview_url
        })

    return parsed_posts

def get_or_update_feed(tag: str = "bookthreads", force_refresh: bool = False, max_items: int = 100, custom_cookie: str = None) -> list:
    os.makedirs(DATA_DIR, exist_ok=True)
    history_file = os.path.join(DATA_DIR, f"history_{tag}.json")
    history = {}
    
    if os.path.exists(history_file):
        try:
            with open(history_file, "r", encoding="utf-8") as f:
                history = json.load(f)
        except Exception as e:
            logger.error(f"Error reading history file: {e}")

    # If force refresh or empty, fetch new
    if force_refresh or not history:
        new_posts = fetch_threads_posts(tag, custom_cookie=custom_cookie)
        for p in new_posts:
            code = p["code"]
            if code not in history:
                history[code] = p
            else:
                history[code].update(p)

    sorted_posts = sorted(history.values(), key=lambda x: x.get("taken_at", 0), reverse=True)
    sorted_posts = sorted_posts[:max_items]

    if force_refresh or not os.path.exists(history_file):
        save_dict = {p["code"]: p for p in sorted_posts}
        try:
            with open(history_file, "w", encoding="utf-8") as f:
                json.dump(save_dict, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving history: {e}")

    return sorted_posts
