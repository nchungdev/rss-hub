# ClaraOS RSS Hub (Wrapper & Feed Engine)

A high-performance, multi-format RSS/JSON/Atom feed wrapper and scraper engine for ClaraOS.

## ✨ Features

- **Multi-Format Feeds**: Natively serves any feed as:
  - `.xml` / `.rss`: RSS 2.0 format (Feedly, NetNewsWire, Miniflux, Reeder).
  - `.json`: JSON Feed v1.1 format (Telegram bots, Discord webhooks, automations).
  - `.atom`: Atom 1.0 format.
- **Threads Engine**: Scrapes Threads communities and topic tags, loads session cookies, connects to FlareSolverr, and automatically extracts Google Drive Ebook download links.
- **RSSHub Upstream Integration**: Transparently proxies and serves thousands of standard RSSHub routes (`diygod/rsshub`).
- **Web Dashboard**: Clean, modern dashboard at root URL with 1-click feed copy buttons and manual refresh.

## 🚀 Deployment

```bash
docker compose up -d
```

## 🌐 Endpoints

- `GET /` -> Web Dashboard
- `GET /bookthreads.xml` -> RSS 2.0 feed for Book Threads
- `GET /bookthreads.json` -> JSON Feed v1.1 for Book Threads
- `GET /bookthreads.atom` -> Atom feed for Book Threads
- `GET /health` -> Healthcheck
