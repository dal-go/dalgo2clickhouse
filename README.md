# DALgo ClickHouse adapter

A read-only Go adapter for ClickHouse exploration using the official
`clickhouse-go/v2` driver's `database/sql` interface and the framework-owned
`dal.NewDB` wrapper.

Linux validation passed dependency resolution, formatting, vet and all nine
race-tested unit tests. Real-server integration, publication, CLI integration
and web integration remain separate gates; none is established by the unit
results or the presence of this code.

## Read contract

- Structured queries read one configured database/table into a DALgo
  `RecordsetReader`. Explicit projections/aliases, scalar comparisons, AND/OR,
  `IS NULL`/`IS NOT NULL`, field ordering and positive bounded LIMIT/OFFSET are
  supported. Tables and columns come from the configuration allowlist; filter
  values use driver parameters and never enter the generated identifier text.
- Caller-authored native SQL is an additional, explicitly enabled capability.
  It uses the ClickHouse credential's permissions, including any permitted
  functions, external sources and other databases. The structured table/column
  allowlist does **not** constrain native SQL. There is no pretend SQL sanitizer.
- Both paths retain UInt64 integers, `decimal.Decimal`, DateTime64 timestamps,
  nullable pointers, arrays, maps and other supported driver scalar/container
  values without JSON or floating-point conversion. `DbType` retains the native
  column type. Unsupported native structs fail explicitly instead of coercing.
- An operation has one deadline, one leased SQL connection and finite row/value
  budgets. It fully materializes the result, checks EOF and closes the SQL rows
  and lease before returning a reader. A stream error, row overflow, byte
  overflow, duplicate output column or unsupported value returns no partial
  reader. The byte budget accounts for copied native values and container slots;
  it is not a measurement of wire bytes. Server-side profile limits bound server
  work and driver buffering separately.
- MergeTree sorting/primary indexes do not imply unique records. Duplicate rows
  remain duplicate rows. Point Get/Exists/GetMulti, durable record keys,
  `RecordsReader`, write sessions, callback transactions and cursors are
  unsupported. Structured joins, grouping, aggregate expressions, subqueries,
  membership/array filters, NULL comparisons and wildcard exclusions are refused.
  Native SQL can express analytics under its separate credential scope.

## Credentials and server settings

Use a dedicated least-privilege ClickHouse user with `SELECT` only on admitted
data and required catalog tables. Every operation checks the effective
`readonly=1` setting on the same leased connection before its statement. A
writable account is refused, even for a SELECT. The adapter never implements a
transaction by calling ClickHouse's transaction-shaped driver stubs.

The administrator must enforce immutable `readonly=1` and throw-on-overflow
settings, plus finite read/CPU/memory ceilings. Allow `max_execution_time`,
`max_result_rows` and `max_result_bytes` to change in read-only mode **only**
inside finite administrator-owned MIN/MAX constraints. Never make `readonly`
changeable in read-only mode. `testdata/init.sql` shows the exact synthetic
profile prepared for the integration fixture. Production grants and ceilings
must be chosen for the application, independently of these synthetic fixtures.

The driver derives `max_execution_time` from the caller deadline with its own
grace period. The adapter also sends result limits and `result_overflow_mode`
`throw`; incompatible profiles fail closed. Native SQL can request settings
within the server profile's bounds, so a profile constraint is essential to
bound work even when caller-authored SQL specifies its own settings. Local row
and copied-value budgets and caller cancellation remain additional limits.

Caller context values are not passed into private driver query options, so a
context cannot smuggle authentication overrides, settings, external tables or
logging callbacks. Returned errors omit SQL, credentials, response bodies,
endpoint details and driver causes. Driver debug logging is not enabled.

TLS with certificate verification is the default. Unencrypted transport needs
an explicit `PlaintextLoopback` option and a literal loopback address.

## Example

This uses a synthetic `explore.events` table and the disposable fixture account:

```go
db, err := dalgo2clickhouse.NewDB(ctx, dalgo2clickhouse.Config{
    Address: "127.0.0.1:9000", Database: "explore",
    Username: "fixture_reader", Password: "synthetic-reader",
    PlaintextLoopback: true,
    Tables: map[string][]string{"events": {"id", "amount"}},
    MaxRows: 100, MaxBytes: 1 << 20, Timeout: 5 * time.Second,
})
if err != nil { return err }
defer db.(*dalgo2clickhouse.Database).Close()
query := dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().
    Limit(10).OrderBy(dal.AscendingField("id")).SelectIntoRecordset()
reader, err := db.ExecuteQueryToRecordsetReader(ctx, query)
if err != nil { return err }
defer reader.Close()
```

For native SQL, enable `Config.NativeSQL` deliberately and obtain
`dal.As[dalgo2clickhouse.NativeQuerier](db)`. `QueryNative(ctx, sql, args...)`
accepts SQL and scalar parameters under the server credential. Its result must
fit the configured limits; `SELECT ... LIMIT n` is useful, but unbounded SQL is
also subject to server and materialization limits and is never silently cut off.

`dal.As[dbschema.SchemaReader](db)` exposes permitted tables, ordered columns,
native type/default metadata and real non-unique data-skipping index names.
`ListDatabases` reports only the configured database. Constraint/referrer
introspection is explicitly unsupported. No primary-key uniqueness is invented.

## Prepared validation

Dependency pins are real versions: DALgo `v0.96.1`, the
[official Go driver `v2.49.0`](https://github.com/ClickHouse/clickhouse-go/releases/tag/v2.49.0),
and the [ClickHouse-published image `clickhouse/clickhouse-server:26.10.3.7`](https://hub.docker.com/r/clickhouse/clickhouse-server).
The fixture pins its published multi-platform index digest
`sha256:739e099abd1157390960a59e4532d21781d8fde57e6e4151cb920ea02c30b1ce`.
This is a verified Docker Hub image tag; no matching GitHub stable-release tag
is claimed.
The Go module checksum file was generated by normal Go tooling in the capped
Linux worker. To reproduce its checks there:

```sh
GOMAXPROCS=2 GOTOOLCHAIN=local go mod tidy
gofmt -w .
go vet ./...
go test -race -count=1 -coverprofile=coverage.out ./...
```

With Docker available in the separately admitted integration environment:

```sh
bash testdata/integration.sh
```

That Linux-only script owns one disposable container limited to 2 GiB memory,
zero swap, two CPUs and 128 PIDs, with random loopback ports. Its documented
global/background worker settings are reduced for this small fixture; actual
startup under those limits remains a mandatory CI check. It initializes
synthetic data and a read-only profile, runs the real
driver tests and removes only its own container. Missing server configuration
fails the integration tests. The required `real-clickhouse` CI job tests native
write/settings refusals, data grants, UInt64/Decimal/DateTime64/array/map/null
fidelity, duplicate rows, schema scope, injection, limits and cancellation.
Unit tests exercise SQL planning, native copying and controlled stream/error
lifetimes; they do not claim server behavior. Do not land while real-server CI
is red or has not run.

## Consumer integration is still required

DataTug CLI's `accesspolicies.Run` and `pkg/secureread` currently consume
`RecordsReader`. This adapter intentionally returns anonymous recordsets, so it
is not an automatic drop-in for those paths. A separate policy-aware anonymous
recordset/native-SQL execution path must be integrated under the existing
policy/server read refusal rules. Do not fabricate stable keys or bypass
`accesspolicies` to connect it. CLI and web wiring are not implemented here.

Upstream references: [Go client](https://clickhouse.com/docs/integrations/language-clients/go/index),
[HTTP interface](https://clickhouse.com/docs/concepts/features/interfaces/http),
[settings constraints](https://clickhouse.com/docs/concepts/features/configuration/settings/constraints-on-settings).
