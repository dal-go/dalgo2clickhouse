package dalgo2clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/shopspring/decimal"
)

// This database/sql stand-in controls stream/error lifetimes, not ClickHouse
// behavior. The mandatory integration job proves server/driver semantics.
type fakeSource struct {
	mu                sync.Mutex
	readonly          uint64
	queries           []string
	args              [][]driver.NamedValue
	columns           []string
	types             []reflect.Type
	rows              [][]driver.Value
	queryErr, lateErr error
	wait              bool
	closed            int
	connects          int
}
type fakeConnector struct{ source *fakeSource }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) {
	c.source.mu.Lock()
	c.source.connects++
	c.source.mu.Unlock()
	return &fakeConn{c.source}, nil
}
func (c fakeConnector) Driver() driver.Driver { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unused") }

type fakeConn struct{ source *fakeSource }

// Match the pinned ClickHouse driver's NamedValueChecker: retain exact values.
func (*fakeConn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (*fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("prepare forbidden") }
func (*fakeConn) Close() error                        { return nil }
func (*fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("transaction forbidden") }
func (c *fakeConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f := c.source
	if statement == "SELECT getSetting('readonly')" {
		return &fakeRows{columns: []string{"readonly"}, types: []reflect.Type{reflect.TypeOf(uint64(0))}, rows: [][]driver.Value{{f.readonly}}}, nil
	}
	f.mu.Lock()
	f.queries = append(f.queries, statement)
	f.args = append(f.args, args)
	f.mu.Unlock()
	if f.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return &fakeRows{source: f, columns: f.columns, types: f.types, rows: f.rows, late: f.lateErr}, nil
}

type fakeRows struct {
	source   *fakeSource
	columns  []string
	types    []reflect.Type
	rows     [][]driver.Value
	position int
	late     error
}

func (r *fakeRows) Columns() []string { return r.columns }
func (r *fakeRows) Close() error {
	if r.source != nil {
		r.source.mu.Lock()
		r.source.closed++
		r.source.mu.Unlock()
	}
	return nil
}
func (r *fakeRows) Next(values []driver.Value) error {
	if r.position >= len(r.rows) {
		if r.late != nil {
			return r.late
		}
		return io.EOF
	}
	copy(values, r.rows[r.position])
	r.position++
	return nil
}
func (r *fakeRows) ColumnTypeScanType(i int) reflect.Type { return r.types[i] }
func (*fakeRows) ColumnTypeDatabaseTypeName(int) string   { return "native" }

func testConfig() Config {
	return Config{Address: "127.0.0.1:9000", Database: "explore", Username: "reader", PlaintextLoopback: true,
		Tables: map[string][]string{"events": nil}, NativeSQL: true, Timeout: time.Second, MaxRows: 10, MaxBytes: 8192}
}
func fakeDatabase(t *testing.T, f *fakeSource) *Database {
	t.Helper()
	d := newDatabase(testConfig(), sql.OpenDB(fakeConnector{f}))
	t.Cleanup(func() { _ = d.Close() })
	return d
}
func structured(limit int) dal.StructuredQuery {
	return dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(limit).SelectIntoRecordset()
}

func TestCompileDoesNotInterpolateValues(t *testing.T) {
	value := "x'; DROP TABLE events; --"
	q := dal.From(dal.NewRootCollectionRef("events", "e")).NewQuery().Limit(2).
		Where(dal.NewGroupCondition(dal.Or, dal.WhereField("label", dal.Equal, value), dal.Field("label").IsNull())).
		OrderBy(dal.DescendingField("id")).SelectColumns(dal.Column{Expression: dal.NewFieldRef("e", "label"), Alias: "display"})
	p, err := compile(q, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT `events`.`label` AS `display` FROM `explore`.`events` WHERE (`events`.`label` = ? OR `events`.`label` IS NULL) ORDER BY `events`.`id` DESC LIMIT 2 OFFSET 0"
	if p.sql != want || !reflect.DeepEqual(p.args, []any{value}) || strings.Contains(p.sql, value) {
		t.Fatalf("unsafe plan: %#v", p)
	}
}

func TestCompileRefusesUnsupportedBeforeIO(t *testing.T) {
	c := testConfig()
	queries := []dal.Query{
		nil, structured(0), structured(11),
		dal.From(dal.NewRootCollectionRef("events;DROP", "")).NewQuery().Limit(1).SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("other", "")).NewQuery().Limit(1).SelectIntoRecordset(),
		dal.From(dal.NewQualifiedRootCollectionRef("forbidden", "events", "")).NewQuery().Limit(1).SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(1).WhereField("id", dal.In, []int{1}).SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(1).WhereField("id", dal.Equal, nil).SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(1).StartAfter("fake").SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(1).GroupBy(dal.Field("id")).SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(1).SelectColumns(dal.Column{Expression: dal.Field("id;DROP")}),
	}
	for i, q := range queries {
		if _, err := compile(q, c); err == nil {
			t.Fatalf("case %d admitted", i)
		}
	}
	c.Tables["events"] = []string{"label"}
	p, err := compile(structured(2), c)
	if err != nil || !strings.HasPrefix(p.sql, "SELECT `events`.`label` FROM") {
		t.Fatal(p, err)
	}
	q := dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(1).WhereField("id", dal.Equal, 1).SelectIntoRecordset()
	if _, err := compile(q, c); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatal(err)
	}
}

func TestConfigCopiesScopeAndRedacts(t *testing.T) {
	c := testConfig()
	c.Password = "never-print-secret"
	c.Tables["events"] = []string{"id"}
	got, err := resolveConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Tables["events"][0] = "changed"
	if got.Tables["events"][0] != "id" {
		t.Fatal("scope aliases caller memory")
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", c, c), c.Password) {
		t.Fatal("credentials reflected")
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.Address = "customer.example:9000" },
		func(c *Config) { c.Database = "db;DROP" },
		func(c *Config) { c.Timeout = -1 },
		func(c *Config) { c.MaxRows = 100001 },
		func(c *Config) { c.MaxBytes = -1 },
		func(c *Config) { c.Tables = map[string][]string{"events": {"id", "id"}} },
	} {
		bad := testConfig()
		change(&bad)
		if _, err := resolveConfig(bad); err == nil {
			t.Fatal("bad config admitted")
		}
	}
	for _, value := range []any{nil, math.Inf(1), math.NaN(), []int{1}, &struct{}{}} {
		if _, err := boundScalar(value); err == nil {
			t.Fatalf("unsupported bound value admitted: %T", value)
		}
	}
}

func TestNativeValuesAndDuplicateRowsSurvive(t *testing.T) {
	f := &fakeSource{readonly: 1, columns: []string{"id", "amount", "items", "label"},
		types: []reflect.Type{reflect.TypeOf(uint64(0)), reflect.TypeOf(decimal.Decimal{}), reflect.TypeOf([]uint64{}), reflect.TypeOf((*string)(nil))},
		rows:  [][]driver.Value{{uint64(math.MaxUint64), "12345678901234567890.123456789", []uint64{math.MaxUint64}, nil}, {uint64(math.MaxUint64), "12345678901234567890.123456789", []uint64{math.MaxUint64}, nil}}}
	d := fakeDatabase(t, f)
	r, err := d.QueryNative(context.Background(), "SELECT sensitive SQL", 7)
	if err != nil {
		t.Fatal(err)
	}
	if d.pool.Stats().InUse != 0 || f.closed != 1 {
		t.Fatal("rows/lease not released before handoff")
	}
	if r.Recordset().RowsCount() != 2 {
		t.Fatal("duplicate rows collapsed")
	}
	row, rs, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	values, err := row.Data(rs)
	if err != nil {
		t.Fatal(err)
	}
	if values[0] != uint64(math.MaxUint64) || values[1].(decimal.Decimal).String() != "12345678901234567890.123456789" {
		t.Fatal(values)
	}
	if !reflect.DeepEqual(values[2], []uint64{math.MaxUint64}) || !reflect.ValueOf(values[3]).IsNil() {
		t.Fatal(values)
	}
	f.rows[0][2].([]uint64)[0] = 0
	if values[2].([]uint64)[0] != math.MaxUint64 {
		t.Fatal("driver buffer alias")
	}
	_, _, _ = r.Next()
	for n := 0; n < 2; n++ {
		if _, _, err := r.Next(); err != dal.ErrNoMoreRecords {
			t.Fatal(err)
		}
	}
	if _, err := r.Cursor(); err != dal.ErrNotSupported {
		t.Fatal(err)
	}
	_ = r.Close()
	_ = r.Close()
	if _, _, err := r.Next(); err != dal.ErrReaderClosed {
		t.Fatal(err)
	}
}

func TestNoPartialResultsOrSecretErrors(t *testing.T) {
	for _, kind := range []string{"late", "overflow", "bytes", "query", "empty-column"} {
		t.Run(kind, func(t *testing.T) {
			f := &fakeSource{readonly: 1, columns: []string{"id"}, types: []reflect.Type{reflect.TypeOf(uint64(0))}, rows: [][]driver.Value{{uint64(1)}}}
			d := fakeDatabase(t, f)
			switch kind {
			case "late":
				f.lateErr = fmt.Errorf("credential=secret SELECT private: %w", io.EOF)
			case "overflow":
				d.cfg.MaxRows = 1
				f.rows = append(f.rows, []driver.Value{uint64(2)})
			case "bytes":
				d.cfg.MaxBytes = 1
			case "query":
				f.queryErr = errors.New("credential=secret SELECT private")
			case "empty-column":
				f.columns = []string{""}
			}
			r, err := d.QueryNative(context.Background(), "SELECT private")
			if err == nil || r != nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private") || errors.Is(err, io.EOF) {
				t.Fatalf("partial/reflected result: %v %v", r, err)
			}
			if d.pool.Stats().InUse != 0 {
				t.Fatal("connection lease leaked")
			}
		})
	}
}

func TestReadonlyAndUnsupportedCapabilities(t *testing.T) {
	f := &fakeSource{readonly: 0}
	d := fakeDatabase(t, f)
	if _, err := d.QueryNative(context.Background(), "DROP TABLE events"); err != ErrReadOnlyRequired || len(f.queries) != 0 {
		t.Fatal(err)
	}
	if _, ok := dal.As[NativeQuerier](d); !ok {
		t.Fatal("native capability absent")
	}
	if _, ok := dal.As[dbschema.SchemaReader](d); !ok {
		t.Fatal("schema capability absent")
	}
	if _, ok := any(d.DB).(dal.WriteSession); ok {
		t.Fatal("write capability advertised")
	}
	called := false
	if err := d.RunReadonlyTransaction(context.Background(), func(context.Context, dal.ReadTransaction) error { called = true; return nil }); err != dal.ErrNotSupported || called {
		t.Fatal(err)
	}
	if err := d.RunReadwriteTransaction(context.Background(), func(context.Context, dal.ReadwriteTransaction) error { called = true; return nil }); err != dal.ErrNotSupported || called {
		t.Fatal(err)
	}
	if err := d.Get(context.Background(), nil); err != dal.ErrNotSupported {
		t.Fatal(err)
	}
	if _, err := d.ExecuteQueryToRecordsReader(context.Background(), structured(1)); err != dal.ErrNotSupported {
		t.Fatal(err)
	}
	d.cfg.NativeSQL = false
	if _, err := d.QueryNative(context.Background(), "SELECT 1"); err != dal.ErrNotSupported {
		t.Fatal(err)
	}
}

func TestCancellationAndCloseOwnLeases(t *testing.T) {
	f := &fakeSource{readonly: 1, wait: true}
	d := fakeDatabase(t, f)
	d.cfg.Timeout = 10 * time.Millisecond
	if _, err := d.QueryNative(context.Background(), "SELECT sleep(10)"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if d.pool.Stats().InUse != 0 {
		t.Fatal("timed out lease leaked")
	}
	key := struct{}{}
	if (noValues{context.WithValue(context.Background(), key, "private driver override")}).Value(key) != nil {
		t.Fatal("driver options survived")
	}
	f.wait = false
	f.columns = []string{"id"}
	f.types = []reflect.Type{reflect.TypeOf(uint64(0))}
	f.rows = [][]driver.Value{{uint64(1)}}
	ctx, cancel := context.WithCancel(context.Background())
	r, err := d.QueryNative(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	for n := 0; n < 2; n++ {
		if _, _, err := r.Next(); err != context.Canceled {
			t.Fatal(err)
		}
	}
	_ = d.Close()
	_ = d.Close()
	if _, err := d.QueryNative(context.Background(), "SELECT 1"); err != ErrClosed {
		t.Fatal(err)
	}
}

func TestNativeCopyBudgets(t *testing.T) {
	value := map[string]any{"array": []any{uint64(math.MaxUint64), nil, decimal.RequireFromString("9.000000001")}}
	budget := int64(8192)
	copy, err := copyValue(reflect.ValueOf(value), &budget, 0)
	if err != nil || !reflect.DeepEqual(copy.Interface(), value) {
		t.Fatal(copy, err)
	}
	cycle := map[string]any{}
	cycle["self"] = cycle
	if _, err := copyValue(reflect.ValueOf(cycle), &budget, 0); err != ErrLimit {
		t.Fatal(err)
	}
}

func TestMetadataScopeAndNativeTypes(t *testing.T) {
	f := &fakeSource{readonly: 1, columns: []string{"name"}, types: []reflect.Type{reflect.TypeOf("")}, rows: [][]driver.Value{{"events"}}}
	d := fakeDatabase(t, f)
	refs, err := d.ListCollections(context.Background(), nil)
	if err != nil || len(refs) != 1 || refs[0].Schema() != "explore" || refs[0].Name() != "events" {
		t.Fatal(refs, err)
	}
	if len(f.args[0]) != 2 || f.args[0][0].Value != "explore" || f.args[0][1].Value != "events" {
		t.Fatal("catalog scope was not parameterized")
	}
	f.rows = [][]driver.Value{{"outside_scope"}}
	if _, err := d.ListCollections(context.Background(), nil); err != ErrBackend {
		t.Fatal("out-of-scope metadata admitted", err)
	}
	f.columns = []string{"name", "type", "default_kind", "default_expression"}
	f.types = []reflect.Type{reflect.TypeOf(""), reflect.TypeOf(""), reflect.TypeOf(""), reflect.TypeOf("")}
	f.rows = [][]driver.Value{{"amount", "Nullable(Decimal(38, 9))", "DEFAULT", "NULL"}}
	definition, err := d.DescribeCollection(context.Background(), &refs[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(definition.PrimaryKey) != 0 || len(definition.Fields) != 1 || definition.Fields[0].Type != dbschema.Decimal || !definition.Fields[0].Nullable {
		t.Fatal(definition)
	}
	column := definition.SourceDefinition.Columns[0]
	if column.DeclaredType != "Nullable(Decimal(38, 9))" || column.DefaultSQL == nil || *column.DefaultSQL != "NULL" {
		t.Fatal("source metadata changed", column)
	}
	before := len(f.queries)
	bad := dal.NewQualifiedRootCollectionRef("secret", "events", "")
	if _, err := d.DescribeCollection(context.Background(), &bad); err != dal.ErrNotSupported || len(f.queries) != before {
		t.Fatal("out-of-scope catalog request reached provider")
	}
	if _, err := d.ListConstraints(context.Background(), &refs[0]); err == nil {
		t.Fatal("constraints silently fabricated")
	}
	if _, err := d.ListReferrers(context.Background(), &refs[0]); err == nil {
		t.Fatal("referrers silently fabricated")
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if err := safeError(context.Background(), fmt.Errorf("secret SQL: %w", cause)); err != cause {
			t.Fatal("classification lost or wrapper retained", err)
		}
	}
}
