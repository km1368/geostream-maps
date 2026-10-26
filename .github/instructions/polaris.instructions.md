---
description: "Use when configuring Apache Polaris Iceberg catalog, MinIO storage credentials, or lakehouse bootstrap scripts."
applyTo: "polaris/**"
---

- Polaris provides the Iceberg REST catalog; the server-side catalog is named `polaris` and its default table location is in MinIO.
- `polaris-init.sh` bootstraps the catalog and grants `CATALOG_MANAGE_CONTENT`; keep it idempotent because Compose runs it on normal startup.
- Use the unauthenticated health endpoint on port 8182 for readiness; catalog API endpoints may return 401 without credentials even when Polaris is healthy.
- Keep Polaris root credentials in `.env` and align them with `POLARIS_BOOTSTRAP_CREDENTIALS` and client credentials used by Spark and Airflow.
- MinIO storage configuration requires path-style access and `stsUnavailable: true`; do not enable credential vending for this MinIO deployment.
- The REST catalog's `warehouse` value is the catalog name, not an S3 URI; table locations come from `default-base-location`.
- Polaris has no web UI; use the catalog and management REST APIs for administration and verification.