package dalgo2clickhouse

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/dal-go/dalgo/dal"
)

type plan struct {
	sql  string
	args []any
}

// StructuredQueryValidator is an optional, pure preflight capability. A policy
// preparer must validate both the original AST and the effective AST with the
// same method before executing either. It does not authorize native SQL.
type StructuredQueryValidator interface {
	ValidateStructuredQuery(dal.Query) error
}

// ValidateStructuredQuery runs the execution compiler against this database's
// immutable relation/column scope and bounds. A valid dal.Param may occur only
// in a supported comparison's scalar value position, so policy preparers can
// guard the original AST before substitution as well as the effective AST.
// Execution still refuses unresolved parameters before I/O. This method acquires
// no connection, changes no query, and neither authorizes nor executes a policy.
func (d *Database) ValidateStructuredQuery(query dal.Query) error {
	_, err := compileWithParameters(query, d.cfg, true)
	return err
}

func quote(name string) string { return "`" + name + "`" }

func tableRef(q dal.StructuredQuery, c Config) (dal.CollectionRef, error) {
	from := q.From()
	if from == nil || from.Base() == nil || len(from.Joins()) != 0 {
		return dal.CollectionRef{}, dal.ErrNotSupported
	}
	var ref dal.CollectionRef
	switch v := from.Base().(type) {
	case dal.CollectionRef:
		ref = v
	case *dal.CollectionRef:
		if v == nil {
			return ref, dal.ErrNotSupported
		}
		ref = *v
	default:
		return ref, dal.ErrNotSupported
	}
	if ref.Parent() != nil || ref.ScanLimit() != 0 || len(ref.ScanOrders()) != 0 || (ref.Database() != "" && ref.Database() != c.Database) || (ref.Schema() != "" && ref.Schema() != c.Database) {
		return ref, dal.ErrNotSupported
	}
	if _, ok := c.Tables[ref.Name()]; !ok || !identifier.MatchString(ref.Name()) {
		return ref, dal.ErrNotSupported
	}
	return ref, nil
}

type compiler struct {
	cfg             Config
	table, alias    string
	args            []any
	conditions      int
	allowParameters bool
}

func (b *compiler) field(expr dal.Expression) (string, error) {
	f, ok := expr.(dal.FieldRef)
	if !ok || f.IsID() || !identifier.MatchString(f.Name()) || (f.Source() != "" && f.Source() != b.table && f.Source() != b.alias) {
		return "", dal.ErrNotSupported
	}
	allowed := b.cfg.Tables[b.table]
	if len(allowed) > 0 {
		found := false
		for _, name := range allowed {
			if name == f.Name() {
				found = true
				break
			}
		}
		if !found {
			return "", dal.ErrNotSupported
		}
	}
	// The emitted FROM has no SQL alias. Qualify physical source columns by its
	// table name so ClickHouse's global SELECT aliases cannot rewrite a predicate
	// or ordering expression into a different column (including a policy filter).
	return quote(b.table) + "." + quote(f.Name()), nil
}

func compile(query dal.Query, c Config) (plan, error) {
	return compileWithParameters(query, c, false)
}

func compileWithParameters(query dal.Query, c Config, allowParameters bool) (plan, error) {
	q, ok := query.(dal.StructuredQuery)
	if !ok || q == nil || reflect.ValueOf(q).Kind() == reflect.Pointer && reflect.ValueOf(q).IsNil() {
		return plan{}, dal.ErrNotSupported
	}
	if dal.HasAggregation(q) || dal.HasSubquery(q) || len(q.GroupBy()) != 0 || q.Having() != nil || q.StartFrom() != "" || q.StartAfter() != "" || q.IDKind() != reflect.Invalid {
		return plan{}, dal.ErrNotSupported
	}
	if q.Limit() <= 0 || q.Limit() > c.MaxRows || q.Offset() < 0 || q.Offset() > 100000 {
		return plan{}, ErrLimit
	}
	ref, err := tableRef(q, c)
	if err != nil {
		return plan{}, err
	}
	b := compiler{cfg: c, table: ref.Name(), alias: ref.Alias(), allowParameters: allowParameters}
	columns := q.Columns()
	if len(columns) > 256 || len(q.OrderBy()) > 16 {
		return plan{}, ErrLimit
	}
	selected := []string{}
	labels := make(map[string]string)
	for _, col := range columns {
		if col.Wildcard != nil {
			return plan{}, dal.ErrNotSupported
		}
		field, err := b.field(col.Expression)
		if err != nil {
			return plan{}, err
		}
		expression := field
		name := col.Expression.(dal.FieldRef).Name()
		if col.Alias != "" {
			if !identifier.MatchString(col.Alias) {
				return plan{}, dal.ErrNotSupported
			}
			name = col.Alias
			field += " AS " + quote(name)
		}
		if previous, exists := labels[name]; exists && previous != expression {
			return plan{}, dal.ErrNotSupported
		}
		labels[name] = expression
		selected = append(selected, field)
	}
	if len(selected) == 0 {
		if allowed := c.Tables[ref.Name()]; len(allowed) > 0 {
			for _, name := range allowed {
				selected = append(selected, quote(ref.Name())+"."+quote(name))
			}
		} else {
			selected = []string{quote(ref.Name()) + ".*"}
		}
	}
	statement := "SELECT " + strings.Join(selected, ", ") + " FROM " + quote(c.Database) + "." + quote(ref.Name())
	if q.Where() != nil {
		where, err := b.condition(q.Where(), 0)
		if err != nil {
			return plan{}, err
		}
		statement += " WHERE " + where
	}
	orders := []string{}
	for _, order := range q.OrderBy() {
		if order == nil {
			return plan{}, dal.ErrNotSupported
		}
		field, err := b.field(order.Expression())
		if err != nil {
			return plan{}, err
		}
		if order.Descending() {
			field += " DESC"
		} else {
			field += " ASC"
		}
		orders = append(orders, field)
	}
	if len(orders) > 0 {
		statement += " ORDER BY " + strings.Join(orders, ", ")
	}
	statement += " LIMIT " + strconv.Itoa(q.Limit()) + " OFFSET " + strconv.Itoa(q.Offset())
	return plan{sql: statement, args: b.args}, nil
}

func (b *compiler) condition(c dal.Condition, depth int) (string, error) {
	b.conditions++
	if c == nil || depth > 8 || b.conditions > 64 {
		return "", dal.ErrNotSupported
	}
	switch v := c.(type) {
	case dal.GroupCondition:
		if (v.Operator() != dal.And && v.Operator() != dal.Or) || len(v.Conditions()) == 0 {
			return "", dal.ErrNotSupported
		}
		parts := []string{}
		for _, child := range v.Conditions() {
			part, err := b.condition(child, depth+1)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
		return "(" + strings.Join(parts, " "+string(v.Operator())+" ") + ")", nil
	case *dal.GroupCondition:
		if v == nil {
			return "", dal.ErrNotSupported
		}
		return b.condition(*v, depth+1)
	case dal.IsNullCondition:
		field, err := b.field(v.Operand())
		if err != nil {
			return "", err
		}
		if v.Negated() {
			return field + " IS NOT NULL", nil
		}
		return field + " IS NULL", nil
	case *dal.IsNullCondition:
		if v == nil {
			return "", dal.ErrNotSupported
		}
		return b.condition(*v, depth+1)
	case dal.Comparison:
		field, err := b.field(v.Left)
		if err != nil {
			return "", err
		}
		var value any
		switch right := v.Right.(type) {
		case dal.Constant:
			value, err = boundScalar(right.Value)
			if err != nil {
				return "", err
			}
		case dal.Param:
			if !b.allowParameters || len(right.Name) > 128 || !dal.ValidParamName(right.Name) {
				return "", dal.ErrNotSupported
			}
		default:
			return "", dal.ErrNotSupported
		}
		operator := ""
		switch v.Operator {
		case dal.Equal:
			operator = "="
		case dal.GreaterThen:
			operator = ">"
		case dal.GreaterOrEqual:
			operator = ">="
		case dal.LessThen:
			operator = "<"
		case dal.LessOrEqual:
			operator = "<="
		default:
			return "", dal.ErrNotSupported
		}
		b.args = append(b.args, value)
		return field + " " + operator + " ?", nil
	default:
		return "", dal.ErrNotSupported
	}
}
