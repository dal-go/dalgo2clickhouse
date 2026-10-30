package dalgo2clickhouse

import (
	"context"
	"database/sql"
	"math/big"
	"reflect"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/shopspring/decimal"
)

// read materializes one finite result and releases both rows and its leased
// connection before handing anything to a caller. Overflow/late errors return
// no reader, so a truncated result is never represented as successful EOF.
func (d *Database) read(ctx context.Context, statement string, args []any, name string) (dal.RecordsetReader, error) {
	op, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer op.done()
	rs, err := d.materialize(op, statement, args, name)
	if err != nil {
		return nil, err
	}
	return &reader{rs: rs, ctx: ctx, db: d}, nil
}

func (d *Database) materialize(op *operation, statement string, args []any, name string) (recordset.Recordset, error) {
	settings := clickhouse.Settings{
		"max_result_rows":      d.cfg.MaxRows + 1,
		"max_result_bytes":     d.cfg.MaxBytes,
		"result_overflow_mode": "throw",
	}
	ctx := clickhouse.Context(op.ctx, clickhouse.WithSettings(settings))
	rows, err := op.conn.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, safeError(op.ctx, err)
	}
	defer func() { _ = rows.Close() }()
	return collect(op.ctx, rows, name, d.cfg.MaxRows, d.cfg.MaxBytes)
}

func collect(ctx context.Context, rows *sql.Rows, name string, maxRows int, maxBytes int64) (recordset.Recordset, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, safeError(ctx, err)
	}
	if len(types) == 0 || len(types) > 256 {
		return nil, ErrLimit
	}
	columns := make([]recordset.Column[any], len(types))
	seen := map[string]bool{}
	budget := maxBytes
	for i, col := range types {
		if col.Name() == "" || seen[col.Name()] {
			return nil, ErrBackend
		}
		seen[col.Name()] = true
		budget -= int64(len(col.Name()) + len(col.DatabaseTypeName()))
		columns[i] = recordset.NewTypedColumn[any](col.Name(), nil, recordset.ColDbType(col.DatabaseTypeName()))
	}
	if budget < 0 {
		return nil, ErrLimit
	}
	rs := recordset.NewColumnarRecordset(name, columns...)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, safeError(ctx, err)
		}
		if rs.RowsCount() >= maxRows {
			return nil, ErrLimit
		}
		dest := make([]any, len(types))
		for i, col := range types {
			typ := col.ScanType()
			if typ == nil {
				return nil, ErrUnsupportedValue
			}
			dest[i] = reflect.New(typ).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, safeError(ctx, err)
		}
		values := make([]any, len(dest))
		for i, target := range dest {
			value, err := copyValue(reflect.ValueOf(target).Elem(), &budget, 0)
			if err != nil {
				return nil, err
			}
			values[i] = value.Interface()
		}
		row := rs.NewRow()
		for i, value := range values {
			if err := row.SetValueByIndex(i, value, rs); err != nil {
				return nil, ErrBackend
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, safeError(ctx, err)
	}
	if err := rows.Close(); err != nil {
		return nil, safeError(ctx, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, safeError(ctx, err)
	}
	return rs, nil
}

// copyValue detaches driver-owned slices/maps/pointers without JSON or float
// conversion. The finite budget also accounts for container slots and prevents
// deeply nested or cyclic values from allocating without bound.
func copyValue(v reflect.Value, budget *int64, depth int) (reflect.Value, error) {
	if depth > 16 {
		return reflect.Value{}, ErrLimit
	}
	*budget -= 16
	if *budget < 0 {
		return reflect.Value{}, ErrLimit
	}
	if !v.IsValid() {
		return reflect.Value{}, ErrUnsupportedValue
	}
	if v.CanInterface() {
		switch value := v.Interface().(type) {
		case time.Time:
			return v, nil
		case decimal.Decimal:
			*budget -= int64(len(value.Coefficient().Bytes()))
			if *budget < 0 {
				return reflect.Value{}, ErrLimit
			}
			return reflect.ValueOf(decimal.NewFromBigInt(value.Coefficient(), value.Exponent())), nil
		case big.Int:
			*budget -= int64(len(value.Bytes()))
			if *budget < 0 {
				return reflect.Value{}, ErrLimit
			}
			return reflect.ValueOf(*new(big.Int).Set(&value)), nil
		}
	}
	switch v.Kind() {
	case reflect.String:
		*budget -= int64(v.Len())
		if *budget < 0 {
			return reflect.Value{}, ErrLimit
		}
		return v, nil
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return v, nil
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type()), nil
		}
		child, err := copyValue(v.Elem(), budget, depth+1)
		if err != nil {
			return reflect.Value{}, err
		}
		out := reflect.New(v.Type()).Elem()
		if v.Kind() == reflect.Pointer {
			out.Set(reflect.New(v.Type().Elem()))
			out.Elem().Set(child)
		} else {
			out.Set(child)
		}
		return out, nil
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return reflect.Zero(v.Type()), nil
		}
		if int64(v.Len()) > *budget/16 {
			return reflect.Value{}, ErrLimit
		}
		var out reflect.Value
		if v.Kind() == reflect.Slice {
			out = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		} else {
			out = reflect.New(v.Type()).Elem()
		}
		for i := 0; i < v.Len(); i++ {
			child, err := copyValue(v.Index(i), budget, depth+1)
			if err != nil {
				return reflect.Value{}, err
			}
			out.Index(i).Set(child)
		}
		return out, nil
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type()), nil
		}
		if int64(v.Len()) > *budget/32 {
			return reflect.Value{}, ErrLimit
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			key, err := copyValue(iter.Key(), budget, depth+1)
			if err != nil {
				return reflect.Value{}, err
			}
			value, err := copyValue(iter.Value(), budget, depth+1)
			if err != nil {
				return reflect.Value{}, err
			}
			out.SetMapIndex(key, value)
		}
		return out, nil
	default:
		return reflect.Value{}, ErrUnsupportedValue
	}
}

type reader struct {
	mu       sync.Mutex
	rs       recordset.Recordset
	ctx      context.Context
	db       *Database
	position int
	closed   bool
	err      error
}

func (r *reader) Recordset() recordset.Recordset { return r.rs }
func (*reader) Cursor() (string, error)          { return "", dal.ErrNotSupported }
func (r *reader) Close() error                   { r.mu.Lock(); defer r.mu.Unlock(); r.closed = true; return nil }
func (r *reader) Next() (recordset.Row, recordset.Recordset, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, r.rs, dal.ErrReaderClosed
	}
	if r.err != nil {
		return nil, r.rs, r.err
	}
	if r.ctx.Err() != nil {
		r.err = safeError(r.ctx, r.ctx.Err())
	} else if r.db.closed.Load() {
		r.err = ErrClosed
	} else if r.position >= r.rs.RowsCount() {
		r.err = dal.ErrNoMoreRecords
	}
	if r.err != nil {
		return nil, r.rs, r.err
	}
	row := r.rs.GetRow(r.position)
	r.position++
	return row, r.rs, nil
}
