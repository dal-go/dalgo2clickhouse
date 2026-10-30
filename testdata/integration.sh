#!/usr/bin/env bash
set -euo pipefail
if [[ "$(uname -s)" != Linux ]]; then
  echo 'ClickHouse integration requires the approved Linux environment.' >&2
  exit 1
fi
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
fixture="dalgo-clickhouse-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$"
cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [[ "$status" != 0 ]]; then docker logs "$fixture" >&2 || true; fi
  docker rm -f "$fixture" >/dev/null
  exit "$status"
}
trap cleanup EXIT INT TERM
# Exact published ClickHouse image repository, version and verified index digest.
docker run --detach --name "$fixture" --memory=2g --memory-swap=2g --cpus=2 --pids-limit=128 \
  --publish 127.0.0.1::9000 --publish 127.0.0.1::8123 --env CLICKHOUSE_SKIP_USER_SETUP=1 \
  --mount "type=bind,source=$root/testdata/init.sql,target=/docker-entrypoint-initdb.d/01-fixture.sql,readonly" \
  --mount "type=bind,source=$root/testdata/access.xml,target=/etc/clickhouse-server/config.d/adapter-access.xml,readonly" \
  clickhouse/clickhouse-server:26.10.3.7@sha256:739e099abd1157390960a59e4532d21781d8fde57e6e4151cb920ea02c30b1ce >/dev/null
ready=false
for attempt in {1..60}; do
  if docker exec "$fixture" clickhouse-client --query "SELECT count() FROM explore.events" 2>/dev/null | grep -qx 3; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { echo 'ClickHouse synthetic fixture did not initialize' >&2; exit 1; }
mapping=$(docker port "$fixture" 9000/tcp)
[[ "$mapping" =~ ^127\.0\.0\.1:[0-9]+$ ]] || exit 1
export CLICKHOUSE_TEST_ADDRESS="$mapping"
http_mapping=$(docker port "$fixture" 8123/tcp)
[[ "$http_mapping" =~ ^127\.0\.0\.1:[0-9]+$ ]] || exit 1
export CLICKHOUSE_TEST_HTTP_ADDRESS="$http_mapping"
go test -race -tags=integration -count=1 -timeout=3m ./...
