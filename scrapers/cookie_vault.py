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

def read_initial_threads_cookie() -> str:
    if os.path.exists(COOKIE_FILE):
        try:
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                c = f.read().strip()
                if c: return c
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

    # Clean up any legacy system flags
    modified = False
    if "threads_default" in vault:
        # Migrate old threads_default to a normal editable profile
        val = vault.pop("threads_default")
        vault["threads_main"] = {
            "id": "threads_main",
            "name": "Tài khoản Threads",
            "website": "threads.net",
            "scraper_type": "threads",
            "cookie": val.get("cookie", ""),
            "description": "Cookie tài khoản Threads",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        modified = True

    for k, v in list(vault.items()):
        if v.get("is_system"):
            v.pop("is_system", None)
            modified = True
        if "website" not in v:
            v["website"] = "threads.net" if v.get("platform") == "threads" else "generic"
            modified = True
        if "scraper_type" not in v:
            v["scraper_type"] = v.get("platform", "web")
            modified = True

    if not vault:
        initial_cookie = read_initial_threads_cookie()
        if initial_cookie:
            vault["threads_main"] = {
                "id": "threads_main",
                "name": "Tài khoản Threads",
                "website": "threads.net",
                "scraper_type": "threads",
                "cookie": initial_cookie,
                "description": "Cookie tài khoản Threads",
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            modified = True

    if modified:
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

def mask_cookie(cookie: str) -> str:
    if not cookie: return "Chưa có dữ liệu"
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
            "website": v.get("website", "generic"),
            "scraper_type": v.get("scraper_type", "web"),
            "masked": mask_cookie(v.get("cookie", "")),
            "has_cookie": bool(v.get("cookie")),
            "description": v.get("description", ""),
            "updated_at": v.get("updated_at", "")
        })
    # Sort by website then name
    summary.sort(key=lambda x: (x["website"], x["name"]))
    return summary

def get_cookie_by_id(profile_id: str) -> str:
    vault = load_vault()
    if profile_id in vault:
        return vault[profile_id].get("cookie", "").strip()
    return ""

def resolve_effective_cookie(feed_meta: dict) -> str:
    mode = feed_meta.get("cookie_mode", "none")
    if mode == "custom":
        return feed_meta.get("cookie", "").strip()
    elif mode == "profile":
        profile_id = feed_meta.get("cookie_profile", "")
        if profile_id:
            return get_cookie_by_id(profile_id)
    return ""
