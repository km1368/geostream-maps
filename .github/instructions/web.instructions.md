---
description: "Use when working on the nginx-hosted MapLibre map, web routes, Martin tile integration, or browser-facing API proxies."
applyTo: "web/**"
---

- The web service is an nginx-hosted MapLibre GL JS map; keep browser-facing routes and API proxies compatible with the existing application.
- `/tiles/` proxies Martin vector tiles and `/clickhouse/` proxies ClickHouse HTTP; preserve the current prefix handling in nginx.
- Keep ClickHouse proxy authentication in nginx headers aligned with `.env`; never expose credentials in browser code.
- Martin's tile cache is disabled in `martin.yaml` so updated PostGIS traffic colors appear promptly; do not enable an unbounded cache.
- Nginx and Martin configuration files are bind-mounted; restart the owning service after changing its config.
- Map glyphs are served from the shared fonts directory; preserve fontstack names referenced by the map style.
