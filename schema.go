package dalgo2clickhouse

import (
	"context"
	"sort"
	"strings"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

var _ dbschema.SchemaReader = (*Database)(nil)

func (d *Database) metadata(ctx context.Context, statement string, args ...any) (recordset.Recordset, error) {
	op, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer op.done()
	return d.materialize(op, statement, args, "clickhouse-catalog")
}

// ListDatabases reports only the configured database, and only if the server
// returns it. It does not enumerate databases outside the structured scope.
func (d *Database) ListDatabases(ctx context.Context) ([]string, error) {
	rs, err := d.metadata(ctx, "SELECT name FROM system.databases WHERE name = ?", d.cfg.Database)
	if err != nil {
		return nil, err
	}
	return stringsFrom(rs, 0)
}

func placeholders(n int) string { return strings.TrimSuffix(strings.Repeat("?,", n), ",") }

func (d *Database) ListCollections(ctx context.Context, parent *record.Key) ([]dal.CollectionRef, error) {
	if parent != nil {
		return nil, dal.ErrNotSupported
	}
	names := make([]string, 0, len(d.cfg.Tables))
	for name := range d.cfg.Tables {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return []dal.CollectionRef{}, nil
	}
	args := []any{d.cfg.Database}
	for _, name := range names {
		args = append(args, name)
	}
	rs, err := d.metadata(ctx, "SELECT name FROM system.tables WHERE database = ? AND name IN ("+placeholders(len(names))+") ORDER BY name", args...)
	if err != nil {
		return nil, err
	}
	visible, err := stringsFrom(rs, 0)
	if err != nil {
		return nil, err
	}
	refs := make([]dal.CollectionRef, len(visible))
	for i, name := range visible {
		if _, allowed := d.cfg.Tables[name]; !allowed {
			return nil, ErrBackend
		}
		refs[i] = dal.NewQualifiedRootCollectionRef(d.cfg.Database, name, "")
	}
	return refs, nil
}

func (d *Database) checkRef(ref *dal.CollectionRef) error {
	if ref == nil || ref.Parent() != nil || ref.ScanLimit() != 0 || len(ref.ScanOrders()) != 0 || (ref.Schema() != "" && ref.Schema() != d.cfg.Database) || (ref.Database() != "" && ref.Database() != d.cfg.Database) {
		return dal.ErrNotSupported
	}
	if _, ok := d.cfg.Tables[ref.Name()]; !ok {
		return dal.ErrNotSupported
	}
	return nil
}

func (d *Database) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	if err := d.checkRef(ref); err != nil {
		return nil, err
	}
	args := []any{d.cfg.Database, ref.Name()}
	statement := "SELECT name, type, default_kind, default_expression FROM system.columns WHERE database = ? AND table = ?"
	if columns := d.cfg.Tables[ref.Name()]; len(columns) > 0 {
		statement += " AND name IN (" + placeholders(len(columns)) + ")"
		for _, name := range columns {
			args = append(args, name)
		}
	}
	statement += " ORDER BY position"
	rs, err := d.metadata(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	if rs.RowsCount() == 0 {
		return nil, ErrBackend
	}
	definition := &dbschema.CollectionDef{Name: ref.Name(), SourceDefinition: &dbschema.SourceDefinition{Dialect: "clickhouse"}}
	for i := 0; i < rs.RowsCount(); i++ {
		values, err := rowStrings(rs, i)
		if err != nil || len(values) != 4 {
			return nil, ErrBackend
		}
		name, typ := values[0], values[1]
		if !identifier.MatchString(name) {
			return nil, ErrBackend
		}
		allowed := d.cfg.Tables[ref.Name()]
		if len(allowed) > 0 {
			found := false
			for _, column := range allowed {
				if column == name {
					found = true
				}
			}
			if !found {
				return nil, ErrBackend
			}
		}
		field := dbschema.FieldDef{Name: dal.FieldName(name), Type: portableType(typ), Nullable: strings.Contains(typ, "Nullable(")}
		definition.Fields = append(definition.Fields, field)
		source := dbschema.SourceColumnDef{Name: name, DeclaredType: typ, NotNull: !field.Nullable}
		if values[2] != "" {
			value := values[3]
			source.DefaultSQL = &value
		}
		definition.SourceDefinition.Columns = append(definition.SourceDefinition.Columns, source)
	}
	// MergeTree's PRIMARY KEY is a sparse sorting index, not uniqueness. Never
	// manufacture a portable unique key from it or from sampled rows.
	return definition, nil
}

func portableType(native string) dbschema.Type {
	for strings.HasPrefix(native, "Nullable(") || strings.HasPrefix(native, "LowCardinality(") {
		at := strings.IndexByte(native, '(')
		native = strings.TrimSuffix(native[at+1:], ")")
	}
	switch {
	case native == "Bool":
		return dbschema.Bool
	case strings.HasPrefix(native, "UInt"), strings.HasPrefix(native, "Int"):
		return dbschema.Int
	case strings.HasPrefix(native, "Float"):
		return dbschema.Float
	case strings.HasPrefix(native, "Decimal"):
		return dbschema.Decimal
	case strings.HasPrefix(native, "Date"):
		return dbschema.Time
	case native == "String", strings.HasPrefix(native, "FixedString"), strings.HasPrefix(native, "Enum"):
		return dbschema.String
	default:
		return dbschema.Null // Native type remains exact in SourceDefinition.
	}
}

// ListIndexes reports real data-skipping index names as non-unique. Its native
// expressions are not guessed into portable field lists.
func (d *Database) ListIndexes(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	if err := d.checkRef(ref); err != nil {
		return nil, err
	}
	// Index expressions may name forbidden columns. Avoid exposing metadata for
	// them when a column allowlist is active.
	if len(d.cfg.Tables[ref.Name()]) > 0 {
		return nil, schemaUnsupported("ListIndexes")
	}
	rs, err := d.metadata(ctx, "SELECT name FROM system.data_skipping_indices WHERE database = ? AND table = ? ORDER BY name", d.cfg.Database, ref.Name())
	if err != nil {
		return nil, err
	}
	names, err := stringsFrom(rs, 0)
	if err != nil {
		return nil, err
	}
	indexes := make([]dbschema.IndexDef, len(names))
	for i, name := range names {
		indexes[i] = dbschema.IndexDef{Name: name, Collection: ref.Name(), Unique: false}
	}
	return indexes, nil
}

func (d *Database) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	if err := d.checkRef(ref); err != nil {
		return nil, err
	}
	return nil, schemaUnsupported("ListConstraints")
}
func (d *Database) ListReferrers(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.Referrer, error) {
	if err := d.checkRef(ref); err != nil {
		return nil, err
	}
	return nil, schemaUnsupported("ListReferrers")
}
func schemaUnsupported(op string) error {
	return &dbschema.NotSupportedError{Op: op, Backend: "clickhouse", Reason: "portable constraint or index-column semantics are not exposed"}
}

func stringsFrom(rs recordset.Recordset, column int) ([]string, error) {
	values := make([]string, rs.RowsCount())
	for i := range values {
		value, err := rs.GetRow(i).GetValueByIndex(column, rs)
		if err != nil {
			return nil, ErrBackend
		}
		text, ok := value.(string)
		if !ok {
			return nil, ErrBackend
		}
		values[i] = text
	}
	return values, nil
}
func rowStrings(rs recordset.Recordset, i int) ([]string, error) {
	data, err := rs.GetRow(i).Data(rs)
	if err != nil {
		return nil, ErrBackend
	}
	values := make([]string, len(data))
	for n, value := range data {
		text, ok := value.(string)
		if !ok {
			return nil, ErrBackend
		}
		values[n] = text
	}
	return values, nil
}
