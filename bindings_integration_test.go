//go:build integration

package dalgo2clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/shopspring/decimal"
)

func TestRealNamedAndExactBindings(t *testing.T) {
	for _, protocol := range []string{"native", "http"} {
		t.Run(protocol, func(t *testing.T) {
			d := realDatabase(t, func(c *Config) {
				if protocol == "http" {
					c.HTTP = true
					c.Address = os.Getenv("CLICKHOUSE_TEST_HTTP_ADDRESS")
					if c.Address == "" {
						t.Fatal("CLICKHOUSE_TEST_HTTP_ADDRESS is required")
					}
				}
			})
			amount := decimal.RequireFromString("12345678901234567890.123456789")
			wide := new(big.Int).Lsh(big.NewInt(1), 200)
			text := "quoted'; DROP TABLE explore.events; --\n\\\x00"
			statement := "SELECT {id:UInt64} AS id, {amount:Decimal(38,9)} AS amount, {integer:UInt64} AS json_integer, {fraction:Decimal(38,9)} AS json_decimal, toString({wide:UInt256}) AS wide_integer, {text:String} AS text, {signed:Int64} AS signed, {missing:Nullable(UInt64)} AS missing"
			r, err := d.QueryNative(context.Background(), statement,
				sql.Named("id", uint64(math.MaxUint64)), sql.Named("amount", amount),
				sql.Named("integer", json.Number("18446744073709551615")),
				sql.Named("fraction", json.Number("12345678901234567890.123456789")),
				sql.Named("wide", wide), sql.Named("text", text), sql.Named("signed", json.Number("-1")), sql.Named("missing", nil))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			row, rs, err := r.Next()
			if err != nil {
				t.Fatal(err)
			}
			columns := rs.Columns()
			if columns[0].Name() != "id" || columns[1].Name() != "amount" || columns[0].DbType() != "UInt64" || strings.ReplaceAll(columns[1].DbType(), " ", "") != "Decimal(38,9)" {
				t.Fatal("exact numeric ordinal metadata changed")
			}
			values, err := row.Data(rs)
			if err != nil || len(values) != 8 || values[0] != uint64(math.MaxUint64) || values[2] != uint64(math.MaxUint64) || values[4] != wide.String() || values[5] != text || values[6] != int64(-1) || !dal.IsNullValue(values[7]) {
				t.Fatal("real named/exact binding changed a value", err)
			}
			for _, index := range []int{1, 3} {
				value, ok := values[index].(decimal.Decimal)
				if !ok || !value.Equal(amount) || value.Exponent() != -9 {
					t.Fatal("real exact decimal binding changed value or scale")
				}
			}
			positional, err := d.QueryNative(context.Background(), "SELECT toUInt64(?) AS id, toDecimal128(?,9) AS amount", json.Number("18446744073709551615"), amount)
			if err != nil {
				t.Fatal(err)
			}
			defer positional.Close()
			row, rs, err = positional.Next()
			if err != nil {
				t.Fatal(err)
			}
			values, err = row.Data(rs)
			if err != nil || values[0] != uint64(math.MaxUint64) || !values[1].(decimal.Decimal).Equal(amount) {
				t.Fatal("real positional exact binding changed value", err)
			}
		})
	}
}

func TestRealAnonymousDuplicateColumnOrdinals(t *testing.T) {
	d := realDatabase(t, nil)
	invalid := "SELECT id AS same, amount AS same FROM explore.events ORDER BY id DESC LIMIT 2"
	if rejected, err := d.QueryNative(context.Background(), invalid); !errors.Is(err, ErrBackend) || rejected != nil || strings.Contains(err.Error(), invalid) {
		t.Fatal("conflicting native aliases did not fail with a sealed backend error", err)
	}
	r, err := d.QueryNative(context.Background(), "SELECT id AS same, id AS same FROM explore.events ORDER BY id DESC LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Recordset().ColumnsCount() != 2 || r.Recordset().RowsCount() != 2 {
		t.Fatal("duplicate native columns or rows lost")
	}
	columns := r.Recordset().Columns()
	if columns[0].Name() != "same" || columns[1].Name() != "same" || columns[0].DbType() != "UInt64" || columns[1].DbType() != "UInt64" {
		t.Fatal("duplicate native column metadata was rewritten")
	}
	for index := 0; index < 2; index++ {
		row, rs, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		values, err := row.Data(rs)
		if err != nil || values[0] != uint64(math.MaxUint64) || values[1] != uint64(math.MaxUint64) {
			t.Fatal("native duplicate ordinals changed values", err)
		}
	}
	query := dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().Limit(2).
		OrderBy(dal.DescendingField("id")).SelectColumns(dal.Column{Expression: dal.Field("id")}, dal.Column{Expression: dal.Field("id")})
	if err := d.ValidateStructuredQuery(query); err != nil {
		t.Fatal(err)
	}
	structured, err := d.ExecuteQueryToRecordsetReader(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer structured.Close()
	if structured.Recordset().RowsCount() != 2 || structured.Recordset().ColumnsCount() != 2 || structured.Recordset().Columns()[0].Name() != "id" || structured.Recordset().Columns()[1].Name() != "id" {
		t.Fatal("repeated structured projection was collapsed or renamed")
	}
	row, rs, err := structured.Next()
	if err != nil {
		t.Fatal(err)
	}
	values, err := row.Data(rs)
	if err != nil || values[0] != uint64(math.MaxUint64) || values[1] != uint64(math.MaxUint64) {
		t.Fatal("repeated structured ordinal values changed", err)
	}
}

func TestRealStructuredOutputAliasesCannotRewriteSourceFields(t *testing.T) {
	d := realDatabase(t, nil)
	filtered := dal.From(dal.NewRootCollectionRef("events", "logical")).NewQuery().Limit(3).
		WhereField("amount", dal.LessThen, uint64(15000000000000000000)).OrderBy(dal.DescendingField("amount")).
		SelectColumns(dal.Column{Expression: dal.NewFieldRef("logical", "id"), Alias: "amount"})
	if err := d.ValidateStructuredQuery(filtered); err != nil {
		t.Fatal(err)
	}
	r, err := d.ExecuteQueryToRecordsetReader(context.Background(), filtered)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// All three physical amounts are below the bound. Substituting the output
	// alias (id) into WHERE would wrongly remove the two maximum-UInt64 rows.
	if r.Recordset().RowsCount() != 3 || r.Recordset().Columns()[0].Name() != "amount" || r.Recordset().Columns()[0].DbType() != "UInt64" {
		t.Fatal("SELECT alias changed the source amount predicate or output metadata")
	}
	ordered := dal.From(dal.NewRootCollectionRef("events", "logical")).NewQuery().Limit(3).
		OrderBy(dal.DescendingField("amount")).SelectColumns(
		dal.Column{Expression: dal.NewFieldRef("logical", "label"), Alias: "amount"}, dal.Column{Expression: dal.Field("id")})
	if err := d.ValidateStructuredQuery(ordered); err != nil {
		t.Fatal(err)
	}
	r, err = d.ExecuteQueryToRecordsetReader(context.Background(), ordered)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Recordset().RowsCount() != 3 || r.Recordset().Columns()[0].DbType() != "Nullable(String)" {
		t.Fatal("source amount ordering changed output cardinality or metadata")
	}
	for i := 0; i < 3; i++ {
		row, rs, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		values, err := row.Data(rs)
		if err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			if values[1] != uint64(math.MaxUint64) || !dal.IsNullValue(values[0]) {
				t.Fatal("ORDER BY resolved the nullable output alias instead of source Decimal amount")
			}
		} else {
			label, ok := values[0].(*string)
			if values[1] != uint64(7) || !ok || label == nil || *label != "safe" {
				t.Fatal("source amount order did not retain the final row")
			}
		}
	}
}
