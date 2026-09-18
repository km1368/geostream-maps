# Fonts

Put your `.ttf`, `.otf`, or `.ttc` font files here (subfolders work too —
Martin scans this directory recursively).

This folder is mounted read-only into the `martin` container at `/fonts`
(see the `--font /fonts` flag in `docker-compose.yml`). Martin converts the
fonts to MapLibre glyph PBFs on the fly.

After adding a font, restart the stack:

    docker compose up -d

Then check the exact font name Martin discovered:

    http://localhost:8080/tiles/catalog

Use that name (case-sensitive, spaces included) in the `text-font` property
of the `street-labels` layer in `web/index.html`. The font currently
configured there is `IRANSansXFaNum Regular` — if you add a different font,
change it to match its catalog name.

Tip: for Persian/Arabic text, make sure the font actually contains those
glyphs (e.g. IRANSansX, Vazirmatn, Noto Sans Arabic).
