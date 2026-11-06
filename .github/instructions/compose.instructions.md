---
description: "Use when editing Docker Compose services, dependencies, networks, profiles, or startup orchestration."
applyTo: "docker-compose.yml"
---

# Compose Orchestration

- Keep every service on the explicit `mapnet` network; otherwise service DNS and broker access can fail silently.
- `import` and `sedona-jupyter` are opt-in profiles. Plain `docker compose up -d` does not start them.
- `docker compose down -v` only removes services in active profiles; include `--profile import` when removing import-profile containers.
- `docker compose up` does not support `--rm`; for one-shot import, run the importer first, then the column setup with `--no-deps`.
- Cold start: start `postgres-1`, run both import steps from the README, then start the stack.
- `minio-init` and `polaris-init` are idempotent one-shots on normal startup; `sedona` waits for both to complete successfully.
- For bind-mounted code/config changes, restart the owning service. Rebuild images when changing Dockerfiles, copied files, or baked dependencies.
- Diagnose startup loops with `docker inspect --format '{{json .State}}' <container>` and full logs; a short log tail can hide the original failure.
