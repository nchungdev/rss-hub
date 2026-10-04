import os
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger("rsshub.cookie_vault")

CONFIG_DIR = os.getenv("CONFIG_DIR", "/app/config")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
COOKIE_FILE = os.getenv("COOKIE_FILE", "/app/config/.session_cookie")
VAULT_FILE = os.path.join(CONFIG_DIR, "cookies.json")
BACKUP_VAULT_FILE = os.path.join(DATA_DIR, "cookies.json")

def get_vault_path() -> str:
    if os.path.exists(CONFIG_DIR):
        return VAULT_FILE
    return BACKUP_VAULT_FILE

def read_threads_session_cookie() -> str:
    # 1. From env
    c = os.getenv("THREADS_SESSION_COOKIE")
    if c: return c.strip()
    # 2. From file
    if os.path.exists(COOKIE_FILE):
        try:
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content: return content
        except Exception:
            pass
    # 3. Fallback path
    fallback = "/home/chungnh/scripts/threads_rss/.session_cookie"
    if os.path.exists(fallback):
        try:
            with open(fallback, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content: return content
        except Exception:
            pass
    return ""

def load_vault() -> dict:
    path = get_vault_path()
    vault = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    vault = data
        except Exception as e:
            logger.error(f"Error loading cookie vault from {path}: {e}")

    # Ensure system default Threads cookie exists
    threads_cookie = read_threads_session_cookie()
    if "threads_default" not in vault:
        vault["threads_default"] = {
            "id": "threads_default",
            "name": "Threads (Mặc định hệ thống)",
            "platform": "threads",
            "cookie": threads_cookie,
            "is_system": True,
            "description": "Cookie Threads phiên mặc định của hệ thống ClaraOS",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        save_vault(vault)
    elif threads_cookie and not vault["threads_default"].get("cookie"):
        vault["threads_default"]["cookie"] = threads_cookie
        save_vault(vault)

    return vault

def save_vault(vault: dict):
    path = get_vault_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(vault, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving cookie vault to {path}: {e}")

    # If threads_default was updated, also sync back to .session_cookie
    if "threads_default" in vault and vault["threads_default"].get("cookie"):
        try:
            with open(COOKIE_FILE, "w", encoding="utf-8") as f:
                f.write(vault["threads_default"]["cookie"].strip())
        except Exception:
            pass

def mask_cookie(cookie: str) -> str:
    if not cookie: return "Chưa thiết lập"
    c = cookie.strip()
    if len(c) <= 12:
        return "****"
    return f"{c[:6]}...{c[-6:]}"

def get_vault_summary() -> list:
    vault = load_vault()
    summary = []
    for k, v in vault.items():
        summary.append({
            "id": v.get("id", k),
            "name": v.get("name", k),
            "platform": v.get("platform", "generic"),
            "masked": mask_cookie(v.get("cookie", "")),
            "has_cookie": bool(v.get("cookie")),
            "is_system": v.get("is_system", False),
            "description": v.get("description", ""),
            "updated_at": v.get("updated_at", "")
        })
    return summary

def resolve_effective_cookie(feed_meta: dict) -> str:
    mode = feed_meta.get("cookie_mode", "default")
    feed_type = feed_meta.get("type", "threads")
    custom_cookie = feed_meta.get("cookie", "").strip()
    profile_id = feed_meta.get("cookie_profile", "")

    if mode == "none":
        return ""
    
    if mode == "custom":
        return custom_cookie

    if mode == "profile" and profile_id:
        vault = load_vault()
        if profile_id in vault:
            return vault[profile_id].get("cookie", "").strip()
        return ""

    # Default mode
    if feed_type == "threads":
        vault = load_vault()
        if "threads_default" in vault and vault["threads_default"].get("cookie"):
            return vault["threads_default"]["cookie"].strip()
        return read_threads_session_cookie()

    # For other types without profile, return custom if given
    return custom_cookie
