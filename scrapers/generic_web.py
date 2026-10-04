import os
import re
import json
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("rsshub.generic_web")

FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://host.docker.internal:8191/v1")

def format_cookies_for_httpx(cookie_str: str) -> dict:
    if not cookie_str:
        return {}
    cookie_str = cookie_str.strip()
    cookies = {}
    if cookie_str.startswith("[") and cookie_str.endswith("]"):
        try:
            arr = json.loads(cookie_str)
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, dict) and item.get("name"):
                        cookies[item["name"]] = str(item.get("value", ""))
                return cookies
        except Exception:
            pass

    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            cookies[k.strip()] = v.strip()
    return cookies

def fetch_via_flaresolverr(url: str, custom_cookie: str = None, max_timeout: int = 60000) -> str:
    logger.info(f"Querying FlareSolverr ({FLARESOLVERR_URL}) for {url}...")
    payload = {
        "cmd": "request.get",
        "url": url,
        "maxTimeout": max_timeout
    }
    if custom_cookie:
        cookies_dict = format_cookies_for_httpx(custom_cookie)
        if cookies_dict:
            domain = urlparse(url).netloc
            payload["cookies"] = [
                {"name": k, "value": v, "domain": domain} for k, v in cookies_dict.items()
            ]
    try:
        with httpx.Client(timeout=65.0) as client:
            resp = client.post(FLARESOLVERR_URL, json=payload)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "ok":
                    return data.get("solution", {}).get("response", "")
    except Exception as e:
        logger.error(f"FlareSolverr fetch error for {url}: {e}")
    return ""

def fetch_html(url: str, custom_cookie: str = None, use_flaresolverr: bool = False) -> str:
    if use_flaresolverr:
        return fetch_via_flaresolverr(url, custom_cookie=custom_cookie)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    cookies = format_cookies_for_httpx(custom_cookie)

    try:
        with httpx.Client(timeout=25.0, headers=headers, cookies=cookies, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                return resp.text
            elif resp.status_code in (403, 503):
                logger.warning(f"HTTP {resp.status_code} on {url}, attempting FlareSolverr bypass...")
                return fetch_via_flaresolverr(url, custom_cookie=custom_cookie)
            else:
                logger.warning(f"Fetch {url} returned HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"Direct fetch failed for {url}: {e}. Retrying via FlareSolverr...")
        return fetch_via_flaresolverr(url, custom_cookie=custom_cookie)
    return ""

def parse_html_to_posts(html_text: str, base_url: str, selectors: dict = None, max_items: int = 50) -> list:
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    # Remove unwanted scripts and styles
    for s in soup(["script", "style", "noscript", "svg"]):
        s.decompose()

    posts = []
    parsed_domain = urlparse(base_url).netloc
    now_ts = int(datetime.now(timezone.utc).timestamp())

    # 1. Custom CSS Selectors Mode
    if selectors and selectors.get("item_selector"):
        item_sel = selectors.get("item_selector", "").strip()
        title_sel = selectors.get("title_selector", "").strip()
        link_sel = selectors.get("link_selector", "").strip()
        desc_sel = selectors.get("desc_selector", "").strip()
        date_sel = selectors.get("date_selector", "").strip()

        items = soup.select(item_sel)
        for item in items[:max_items]:
            # Title
            title = ""
            if title_sel:
                t_elem = item.select_one(title_sel)
                if t_elem: title = t_elem.get_text(strip=True)
            if not title:
                title = item.get_text(strip=True)[:100]

            # Link
            link = ""
            if link_sel:
                l_elem = item.select_one(link_sel)
                if l_elem and l_elem.get("href"):
                    link = l_elem["href"].strip()
            if not link:
                l_elem = item.find("a")
                if l_elem and l_elem.get("href"):
                    link = l_elem["href"].strip()
            if not link:
                continue

            full_link = urljoin(base_url, link)

            # Description
            desc = ""
            if desc_sel:
                d_elem = item.select_one(desc_sel)
                if d_elem: desc = d_elem.get_text(strip=True)
            if not desc:
                desc = title

            # Images
            images = []
            img = item.find("img")
            if img:
                src = img.get("src") or img.get("data-src") or img.get("data-original")
                if src: images.append(urljoin(base_url, src))

            # Google Drive links in description
            gdrive_links = re.findall(r"https?://(?:drive\.google\.com/[^\s\"'<>]+|docs\.google\.com/[^\s\"'<>]+)", desc)

            posts.append({
                "id": full_link,
                "username": parsed_domain,
                "text": f"{title}\n{desc}" if desc and desc != title else title,
                "taken_at": now_ts,
                "url": full_link,
                "gdrive_links": list(set(gdrive_links)),
                "images": images,
                "preview_title": title
            })
        return posts

    # 2. Smart Auto-Detection Mode
    # Container selectors ordered by specificity
    container_selectors = [
        ".structItem--thread", # Xenforo/Voz
        "article",
        ".box-category-item", # Tuoi Tre
        ".item-news", # VnExpress
        ".story",
        ".post",
        ".thread",
        ".entry",
        ".news-item",
        ".card",
        "div[class*='item-']",
        "div[class*='post-']",
        "div[class*='article-']",
        "li[class*='item']",
        "li[class*='post']"
    ]

    selected_items = []
    for sel in container_selectors:
        candidates = soup.select(sel)
        if len(candidates) >= 2:
            selected_items = candidates
            break

    seen_links = set()

    if selected_items:
        for item in selected_items[:max_items * 2]:
            # Find Title & Link
            a_elem = None
            title = ""

            # Check headings first
            for h in item.select("h1, h2, h3, h4, .title, [class*='title']"):
                a_in_h = h.find("a")
                if a_in_h and a_in_h.get("href"):
                    a_elem = a_in_h
                    title = a_in_h.get_text(strip=True) or h.get_text(strip=True)
                    break
                elif h.get_text(strip=True):
                    title = h.get_text(strip=True)

            # If not in heading, search for first valid anchor
            if not a_elem:
                for a in item.find_all("a", href=True):
                    txt = a.get_text(strip=True)
                    if len(txt) >= 8 and not txt.startswith("#"):
                        a_elem = a
                        if not title: title = txt
                        break

            if not a_elem:
                continue

            href = a_elem.get("href", "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            full_link = urljoin(base_url, href)
            if full_link in seen_links:
                continue
            seen_links.add(full_link)

            if not title:
                title = a_elem.get("title", "").strip() or a_elem.get_text(strip=True)
            if len(title) < 4:
                continue

            # Description / Summary
            desc_elem = item.select_one("p, .desc, .summary, .description, .lead, .teaser, .snippet, [class*='desc']")
            desc = desc_elem.get_text(strip=True) if desc_elem else ""

            # Images
            images = []
            img = item.find("img")
            if img:
                src = img.get("src") or img.get("data-src") or img.get("data-original") or img.get("data-lazy-src")
                if src and not src.startswith("data:"):
                    images.append(urljoin(base_url, src))

            # Google Drive links
            full_text = f"{title}\n{desc}" if desc else title
            gdrive_links = re.findall(r"https?://(?:drive\.google\.com/[^\s\"'<>]+|docs\.google\.com/[^\s\"'<>]+)", full_text)

            posts.append({
                "id": full_link,
                "username": parsed_domain,
                "text": full_text,
                "taken_at": now_ts,
                "url": full_link,
                "gdrive_links": list(set(gdrive_links)),
                "images": images,
                "preview_title": title
            })
            if len(posts) >= max_items:
                break

    # Fallback: scan all prominent <a> tags
    if len(posts) < 3:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            txt = a.get_text(strip=True)
            if len(txt) < 15 or len(txt) > 250:
                continue
            if href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
                continue
            
            # Filter common boilerplate navigation texts
            lower_txt = txt.lower()
            if any(w in lower_txt for w in ["đăng nhập", "đăng ký", "chính sách", "điều khoản", "liên hệ", "trang chủ", "giới thiệu", "login", "register", "privacy", "terms", "about"]):
                continue

            full_link = urljoin(base_url, href)
            if full_link in seen_links:
                continue
            seen_links.add(full_link)

            posts.append({
                "id": full_link,
                "username": parsed_domain,
                "text": txt,
                "taken_at": now_ts,
                "url": full_link,
                "gdrive_links": [],
                "images": [],
                "preview_title": txt
            })
            if len(posts) >= max_items:
                break

    return posts

def scrape_generic_web(url: str, custom_cookie: str = None, selectors: dict = None, use_flaresolverr: bool = False, max_items: int = 50) -> list:
    logger.info(f"Scraping generic web page: {url} (flaresolverr={use_flaresolverr})")
    html_content = fetch_html(url, custom_cookie=custom_cookie, use_flaresolverr=use_flaresolverr)
    if not html_content:
        logger.warning(f"No HTML content fetched for {url}")
        return []
    posts = parse_html_to_posts(html_content, base_url=url, selectors=selectors, max_items=max_items)
    logger.info(f"Extracted {len(posts)} posts from {url}")
    return posts
