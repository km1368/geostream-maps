#!/bin/sh
set -e

POLARIS=http://polaris-1:8181

# Probe the unauthenticated health endpoint; the catalog config endpoint returns 401.
i=0
until curl -sf -o /dev/null "http://polaris-1:8182/q/health"; do
  i=$((i+1))
  if [ "$i" -ge 30 ]; then
    echo "polaris-init: Polaris unreachable, giving up"
    exit 1
  fi
  echo "polaris-init: waiting for Polaris..."
  sleep 2
done

TOKEN=$(curl -sf -X POST "$POLARIS/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=$POLARIS_ROOT_CLIENT_ID&client_secret=$POLARIS_ROOT_CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
if [ -z "$TOKEN" ]; then
  echo "polaris-init: token mint failed"
  exit 1
fi

echo "polaris-init: creating catalog 'polaris' (MinIO storage)..."
# 409 means the catalog already exists, making reruns idempotent.
curl -s -o /tmp/resp.json -w "%{http_code}" -X POST "$POLARIS/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -H "Polaris-Realm: POLARIS" \
  -d '{"catalog":{"name":"polaris","type":"INTERNAL","readOnly":false,"properties":{"default-base-location":"s3://lakehouse/gps_processed"},"storageConfigInfo":{"storageType":"S3","endpoint":"http://minio-1:9000","endpointInternal":"http://minio-1:9000","stsUnavailable":true,"pathStyleAccess":true,"allowedLocations":["s3://lakehouse"]}}}' \
  | { code=$(cat); if [ "$code" != 201 ] && [ "$code" != 409 ]; then echo "polaris-init: catalog create failed ($code):" && cat /tmp/resp.json && exit 1; fi; }

echo "polaris-init: granting CATALOG_MANAGE_CONTENT to catalog_admin..."
# Polaris answers 201 (created) or 200 (updated) for this PUT.
curl -s -o /tmp/resp2.json -w "%{http_code}" -X PUT "$POLARIS/api/management/v1/catalogs/polaris/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -H "Polaris-Realm: POLARIS" \
  -d '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}' \
  | { code=$(cat); if [ "$code" != 200 ] && [ "$code" != 201 ] && [ "$code" != 409 ]; then echo "polaris-init: grant failed ($code):" && cat /tmp/resp2.json && exit 1; fi; }

echo "polaris-init: done."