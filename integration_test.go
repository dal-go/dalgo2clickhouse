//go:build integration

package dalgo2clickhouse

import (
	"context"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dal-go/dalgo/dal"
	"github.com/shopspring/decimal"
)

// Missing infrastructure is a failure, never a skipped integration success.
func realDatabase(t *testing.T, mutate func(*Config)) *Database {
	t.Helper()
	address := os.Getenv("CLICKHOUSE_TEST_ADDRESS")
	if address == "" {
		t.Fatal("run testdata/integration.sh: CLICKHOUSE_TEST_ADDRESS is required")
	}
	c := testConfig()
	c.Address = address
	c.Username = "fixture_reader"
	c.Password = "synthetic-reader"
	c.Timeout = 5 * time.Second
	c.MaxRows = 100
	c.MaxBytes = 1 << 20
	if mutate != nil {
		mutate(&c)
	}
	db, err := NewDB(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	d := db.(*Database)
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d
}

func TestRealPrecisionDuplicatesAndStructuredQuery(t *testing.T) {
	d := realDatabase(t, nil)
	q := dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(3).
		WhereField("id", dal.Equal, uint64(math.MaxUint64)).OrderBy(dal.AscendingField("id")).
		SelectColumns(dal.Column{Expression: dal.Field("id")}, dal.Column{Expression: dal.Field("amount")}, dal.Column{Expression: dal.Field("items")}, dal.Column{Expression: dal.Field("label")}, dal.Column{Expression: dal.Field("at")}, dal.Column{Expression: dal.Field("attrs")})
	r, err := d.ExecuteQueryToRecordsetReader(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Recordset().RowsCount() != 2 {
		t.Fatal("non-unique MergeTree rows lost")
	}
	row, rs, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	values, err := row.Data(rs)
	if err != nil {
		t.Fatal(err)
	}
	if values[0] != uint64(math.MaxUint64) {
		t.Fatalf("UInt64 rounded/coerced: %T %v", values[0], values[0])
	}
	amount, ok := values[1].(decimal.Decimal)
	if !ok || amount.String() != "12345678901234567890.123456789" || amount.Exponent() != -9 {
		t.Fatalf("Decimal coerced: %T %v", values[1], values[1])
	}
	if !reflect.DeepEqual(values[2], []uint64{math.MaxUint64}) || !dal.IsNullValue(values[3]) {
		t.Fatal("array/null changed", values)
	}
	if values[4].(time.Time).Nanosecond() != 123456789 {
		t.Fatal("DateTime64 precision lost")
	}
	if !reflect.DeepEqual(values[5], map[string]uint64{"maximum": math.MaxUint64}) {
		t.Fatalf("native Map changed: %T %v", values[5], values[5])
	}
	if d.pool.Stats().InUse != 0 {
		t.Fatal("read lease retained")
	}
	_, _, _ = r.Next()
	if _, _, err := r.Next(); err != dal.ErrNoMoreRecords {
		t.Fatal(err)
	}
}

func TestRealNativeSecurityAndBounds(t *testing.T) {
	d := realDatabase(t, nil)
	version, err := d.QueryNative(context.Background(), "SELECT version() AS version")
	if err != nil {
		t.Fatal(err)
	}
	versionRow, versionSet, err := version.Next()
	if err != nil {
		t.Fatal(err)
	}
	serverVersion, err := versionRow.GetValueByIndex(0, versionSet)
	if err != nil || serverVersion != "26.10.3.7" {
		t.Fatal("unexpected real server fixture", serverVersion, err)
	}
	_ = version.Close()
	for _, statement := range []string{
		"INSERT INTO explore.events (id) VALUES (42)",
		"CREATE TABLE explore.should_not_exist (id UInt64) ENGINE=Memory",
		"SELECT 1 SETTINGS readonly=0",
		"SELECT 1 SETTINGS max_result_rows=0",
		"SELECT 1 SETTINGS max_result_bytes=0",
		"SELECT 1 SETTINGS max_execution_time=0",
		"SELECT 1 SETTINGS result_overflow_mode='break'",
		"SELECT 1 SETTINGS read_overflow_mode='break'",
		"SELECT * FROM forbidden.secret",
	} {
		r, err := d.QueryNative(context.Background(), statement)
		if err == nil || r != nil {
			t.Fatalf("unsafe native statement admitted: %s", statement)
		}
		if strings.Contains(err.Error(), statement) || strings.Contains(err.Error(), "synthetic-reader") {
			t.Fatal("unsafe error reflection")
		}
	}
	r, err := d.QueryNative(context.Background(), "SELECT count() AS count FROM explore.events")
	if err != nil {
		t.Fatal(err)
	}
	row, rs, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	count, err := row.GetValueByIndex(0, rs)
	if err != nil || count != uint64(3) {
		t.Fatal("write attempt changed fixture", count, err)
	}
	_ = r.Close()
	// Caller driver options cannot replace the authenticated account/settings.
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"readonly": 0}))
	r, err = d.QueryNative(ctx, "SELECT 1")
	if err != nil {
		t.Fatal("caller driver settings were retained", err)
	}
	_ = r.Close()
	q := dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(3).WhereField("label", dal.Equal, "safe'; DROP TABLE explore.events; --").SelectIntoRecordset()
	r, err = d.ExecuteQueryToRecordsetReader(context.Background(), q)
	if err != nil || r.Recordset().RowsCount() != 0 {
		t.Fatal("parameter injection", err)
	}
	_ = r.Close()
	limited := realDatabase(t, func(c *Config) { c.MaxRows = 2 })
	if r, err := limited.QueryNative(context.Background(), "SELECT number FROM numbers(10)"); err == nil || r != nil {
		t.Fatal("row budget silently truncated")
	}
	if r, err := d.QueryNative(context.Background(), "SELECT repeat('x', 2000000)"); err == nil || r != nil {
		t.Fatal("byte budget silently truncated")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := d.QueryNative(ctx, "SELECT sleep(3)"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("deadline not propagated", err)
	}
	if d.pool.Stats().InUse != 0 {
		t.Fatal("canceled query leaked lease")
	}
	// A writable credential is refused before its requested statement, even if
	// that particular statement happens to be a SELECT.
	writable := realDatabase(t, func(c *Config) { c.Username = "default"; c.Password = "" })
	if _, err := writable.QueryNative(context.Background(), "SELECT 1"); err != ErrReadOnlyRequired {
		t.Fatal(err)
	}
}

func TestRealHTTPNativeValues(t *testing.T) {
	address := os.Getenv("CLICKHOUSE_TEST_HTTP_ADDRESS")
	if address == "" {
		t.Fatal("CLICKHOUSE_TEST_HTTP_ADDRESS is required")
	}
	d := realDatabase(t, func(c *Config) { c.HTTP = true; c.Address = address })
	r, err := d.QueryNative(context.Background(), "SELECT id, amount FROM explore.events ORDER BY id DESC LIMIT 1")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	row, rs, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	values, err := row.Data(rs)
	if err != nil || values[0] != uint64(math.MaxUint64) || values[1].(decimal.Decimal).String() != "12345678901234567890.123456789" {
		t.Fatal("HTTP native value loss", values, err)
	}
}

func TestRealPermittedCatalog(t *testing.T) {
	d := realDatabase(t, nil)
	databases, err := d.ListDatabases(context.Background())
	if err != nil || !reflect.DeepEqual(databases, []string{"explore"}) {
		t.Fatal(databases, err)
	}
	refs, err := d.ListCollections(context.Background(), nil)
	if err != nil || len(refs) != 1 || refs[0].Name() != "events" {
		t.Fatal(refs, err)
	}
	definition, err := d.DescribeCollection(context.Background(), &refs[0])
	if err != nil || len(definition.Fields) != 6 || len(definition.PrimaryKey) != 0 {
		t.Fatal(definition, err)
	}
	if !strings.HasPrefix(definition.SourceDefinition.Columns[1].DeclaredType, "Decimal") {
		t.Fatal("native decimal metadata lost")
	}
	indexes, err := d.ListIndexes(context.Background(), &refs[0])
	if err != nil || len(indexes) != 1 || indexes[0].Name != "label_skip" || indexes[0].Unique {
		t.Fatal(indexes, err)
	}
	filtered := realDatabase(t, func(c *Config) { c.Tables["events"] = []string{"id", "label"} })
	definition, err = filtered.DescribeCollection(context.Background(), &refs[0])
	if err != nil || len(definition.Fields) != 2 {
		t.Fatal(definition, err)
	}
	q := dal.From(refs[0]).NewQuery().Limit(3).SelectIntoRecordset()
	r, err := filtered.ExecuteQueryToRecordsetReader(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	values, err := r.Recordset().GetRow(0).Data(r.Recordset())
	if err != nil || len(values) != 2 {
		t.Fatal("forbidden columns leaked", err)
	}
	for _, unknown := range []dal.CollectionRef{dal.NewRootCollectionRef("secret", ""), dal.NewQualifiedRootCollectionRef("forbidden", "events", "")} {
		if _, err := d.DescribeCollection(context.Background(), &unknown); err != dal.ErrNotSupported {
			t.Fatal(err)
		}
	}
}
