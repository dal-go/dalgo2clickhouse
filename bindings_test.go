package dalgo2clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/shopspring/decimal"
)

type nilStructuredQuery struct{ dal.StructuredQuery }
type forbiddenBinding struct{}

func (forbiddenBinding) String() string               { panic("custom Stringer must not run") }
func (forbiddenBinding) Value() (driver.Value, error) { panic("custom Valuer must not run") }

func TestValidateStructuredQueryIsPureAndSharesCompiler(t *testing.T) {
	f := &fakeSource{readonly: 0}
	d := fakeDatabase(t, f)
	validator, ok := dal.As[StructuredQueryValidator](d)
	if !ok {
		t.Fatal("pure validation capability absent")
	}
	parameter := dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(2).
		Where(dal.WhereField("id", dal.Equal, dal.NewParam("wanted"))).SelectIntoRecordset()
	if err := validator.ValidateStructuredQuery(parameter); err != nil {
		t.Fatal("original parameterized shape refused", err)
	}
	if _, err := compile(parameter, d.cfg); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatal("execution admitted an unresolved parameter", err)
	}
	effective := dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(2).
		WhereField("id", dal.Equal, dal.Constant{Value: json.Number("18446744073709551615")}).SelectIntoRecordset()
	if err := validator.ValidateStructuredQuery(effective); err != nil {
		t.Fatal(err)
	}
	plan, err := compile(effective, d.cfg)
	if err != nil || plan.args[0].(*big.Int).String() != "18446744073709551615" {
		t.Fatal("effective exact value changed", err)
	}
	var typedNil *nilStructuredQuery
	for _, query := range []dal.Query{
		nil, typedNil, dal.NewTextQuery("SELECT 1", nil), structured(11),
		dal.From(dal.NewRootCollectionRef("forbidden", "")).NewQuery().Limit(1).SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(1).SelectColumns(dal.Column{Expression: dal.NewParam("column")}),
		dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(1).WhereField("id", dal.Equal, dal.Constant{Value: []int{1}}).SelectIntoRecordset(),
	} {
		if err := validator.ValidateStructuredQuery(query); err == nil {
			t.Fatal("invalid original/effective AST admitted")
		}
	}
	if f.connects != 0 || len(f.queries) != 0 {
		t.Fatal("pure validation performed I/O")
	}
}

func TestNativeNamedAndExactBindingsRemainDriverValues(t *testing.T) {
	f := &fakeSource{readonly: 1, columns: []string{"ok"}, types: []reflect.Type{reflect.TypeOf(int64(0))}, rows: [][]driver.Value{{int64(1)}}}
	d := fakeDatabase(t, f)
	integer := new(big.Int).Lsh(big.NewInt(1), 200)
	amount := decimal.RequireFromString("12345678901234567890.123456789")
	text := "x'; DROP TABLE explore.events; --"
	statement := "SELECT {id:UInt256}, {amount:Decimal(38,9)}, {json:UInt64}, {fraction:Decimal(38,9)}, {text:String}"
	r, err := d.QueryNative(context.Background(), statement,
		sql.Named("id", integer), sql.Named("amount", amount), sql.Named("json", json.Number("18446744073709551615")),
		sql.Named("fraction", json.Number("12345678901234567890.123456789")), sql.Named("text", text))
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	if len(f.queries) != 1 || f.queries[0] != statement || len(f.args[0]) != 5 {
		t.Fatal("adapter rewrote SQL or binding count")
	}
	for i, name := range []string{"id", "amount", "json", "fraction", "text"} {
		if f.args[0][i].Name != name {
			t.Fatal("named parameter identity changed")
		}
	}
	boundInteger := f.args[0][0].Value.(*big.Int)
	integer.SetInt64(0)
	if boundInteger.Cmp(new(big.Int).Lsh(big.NewInt(1), 200)) != 0 || f.args[0][2].Value.(*big.Int).String() != "18446744073709551615" {
		t.Fatal("integer binding rounded or borrowed caller storage")
	}
	for _, i := range []int{1, 3} {
		got := f.args[0][i].Value.(decimal.Decimal)
		if !got.Equal(amount) || got.Exponent() != -9 {
			t.Fatal("decimal binding changed")
		}
	}
	if f.args[0][4].Value != text {
		t.Fatal("parameter value became SQL syntax")
	}
	bound, err := boundArguments([]any{json.Number("-1"), json.Number("1.25e2")})
	if err != nil || bound[0].(*big.Int).Int64() != -1 || bound[1].(decimal.Decimal).String() != "125" {
		t.Fatal("positional exact normalization", err)
	}
	for _, args := range [][]any{{nil}, {sql.Named("missing", nil)}} {
		if _, err := boundArguments(args); err != nil {
			t.Fatal("explicit native NULL refused", err)
		}
	}
}

func TestNativeUnsafeBindingsRefusedBeforeIO(t *testing.T) {
	cases := [][]any{
		{sql.NamedArg{Value: 1}}, {sql.Named("a;b", 1)}, {sql.Named("a", 1), sql.Named("a", 2)},
		{sql.Named("a", 1), 2}, {1, sql.Named("a", 2)}, {sql.Named("a", sql.Named("nested", 1))},
		{forbiddenBinding{}}, {sql.Named("a", forbiddenBinding{})}, {[]int{1}}, {map[string]int{"a": 1}},
		{(*big.Int)(nil)}, {new(big.Int).Lsh(big.NewInt(1), 256)}, {new(big.Int).Sub(minInt256, big.NewInt(1))},
		{json.Number("NaN")}, {json.Number("01")}, {json.Number("1;DROP")}, {json.Number("1e1000000000")},
		{json.Number("0." + strings.Repeat("0", 76) + "1")}, {json.Number(strings.Repeat("9", 257))},
		{decimal.New(1, -77)}, {decimal.New(1, 76)}, {decimal.NewFromBigInt(new(big.Int).Lsh(big.NewInt(1), 300), 0)},
		{math.Inf(1)}, {math.NaN()}, {float64(9007199254740992)}, {float32(16777216)},
	}
	for i, args := range cases {
		t.Run(strings.Join([]string{"case", big.NewInt(int64(i)).String()}, "-"), func(t *testing.T) {
			f := &fakeSource{readonly: 0}
			d := fakeDatabase(t, f)
			if r, err := d.QueryNative(context.Background(), "SELECT private", args...); !errors.Is(err, dal.ErrNotSupported) || r != nil || f.connects != 0 {
				t.Fatalf("invalid binding reached I/O or escaped refusal: %v", err)
			}
		})
	}
	for _, value := range []any{
		minInt256, new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)),
		decimal.New(1, -76), decimal.New(1, 75),
	} {
		if _, err := boundScalar(value); err != nil {
			t.Fatal("supported exact boundary refused", err)
		}
	}
}

func TestAnonymousDuplicateColumnLabelsKeepOrdinals(t *testing.T) {
	f := &fakeSource{readonly: 1, columns: []string{"same", "same"},
		types: []reflect.Type{reflect.TypeOf(uint64(0)), reflect.TypeOf(uint64(0))},
		rows:  [][]driver.Value{{uint64(math.MaxUint64), uint64(math.MaxUint64)}, {uint64(math.MaxUint64), uint64(math.MaxUint64)}}}
	d := fakeDatabase(t, f)
	r, err := d.QueryNative(context.Background(), "SELECT id AS same, id AS same")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rs := r.Recordset()
	if rs.ColumnsCount() != 2 || rs.RowsCount() != 2 || rs.Columns()[0].Name() != "same" || rs.Columns()[1].Name() != "same" {
		t.Fatal("duplicate column or row identity was rewritten")
	}
	row, rs, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	values, err := row.Data(rs)
	if err != nil || values[0] != uint64(math.MaxUint64) || values[1] != uint64(math.MaxUint64) {
		t.Fatal("duplicate column ordinal values changed", err)
	}
	q := dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(2).
		SelectColumns(dal.Column{Expression: dal.Field("id"), Alias: "same"}, dal.Column{Expression: dal.Field("id"), Alias: "same"})
	if err := d.ValidateStructuredQuery(q); err != nil {
		t.Fatal("duplicate structured labels refused", err)
	}
	p, err := compile(q, d.cfg)
	if err != nil || !strings.HasPrefix(p.sql, "SELECT `events`.`id` AS `same`, `events`.`id` AS `same` FROM") {
		t.Fatal("duplicate projection was renamed", err)
	}
	for _, columns := range [][]dal.Column{
		{{Expression: dal.Field("id"), Alias: "same"}, {Expression: dal.Field("amount"), Alias: "same"}},
		{{Expression: dal.Field("id")}, {Expression: dal.Field("amount"), Alias: "id"}},
	} {
		blockedSource := &fakeSource{readonly: 0}
		blockedDB := fakeDatabase(t, blockedSource)
		conflict := dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(1).SelectColumns(columns...)
		if err := blockedDB.ValidateStructuredQuery(conflict); !errors.Is(err, dal.ErrNotSupported) {
			t.Fatal("conflicting structured alias passed pure validation", err)
		}
		if result, err := blockedDB.ExecuteQueryToRecordsetReader(context.Background(), conflict); !errors.Is(err, dal.ErrNotSupported) || result != nil || blockedSource.connects != 0 {
			t.Fatal("conflicting structured alias reached I/O", err)
		}
	}
	invalid := "SELECT id AS same, amount AS same"
	f.queryErr = errors.New("MULTIPLE_EXPRESSIONS_FOR_ALIAS: " + invalid)
	if result, err := d.QueryNative(context.Background(), invalid); !errors.Is(err, ErrBackend) || result != nil || strings.Contains(err.Error(), invalid) {
		t.Fatal("native backend failure was not sealed", err)
	}
	if f.queries[len(f.queries)-1] != invalid {
		t.Fatal("adapter rewrote conflicting native SQL")
	}
}

func TestStructuredOutputAliasesCannotRewriteSourceFields(t *testing.T) {
	f := &fakeSource{readonly: 0}
	d := fakeDatabase(t, f)
	for _, right := range []any{dal.NewParam("ceiling"), uint64(15000000000000000000)} {
		query := dal.From(dal.NewRootCollectionRef("events", "logical")).NewQuery().Limit(3).
			Where(dal.WhereField("amount", dal.LessThen, right)).OrderBy(dal.DescendingField("amount")).
			SelectColumns(dal.Column{Expression: dal.NewFieldRef("logical", "id"), Alias: "amount"})
		if err := d.ValidateStructuredQuery(query); err != nil {
			t.Fatal("valid original/effective alias shape refused", err)
		}
		plan, err := compileWithParameters(query, d.cfg, true)
		want := "SELECT `events`.`id` AS `amount` FROM `explore`.`events` WHERE `events`.`amount` < ? ORDER BY `events`.`amount` DESC LIMIT 3 OFFSET 0"
		if err != nil || plan.sql != want {
			t.Fatal("output alias changed source field binding", err)
		}
	}
	if f.connects != 0 || len(f.queries) != 0 {
		t.Fatal("pure source-field validation performed I/O")
	}
}
