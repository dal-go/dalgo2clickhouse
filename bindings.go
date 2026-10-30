package dalgo2clickhouse

import (
	"database/sql"
	"encoding/json"
	"math"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/shopspring/decimal"
)

var (
	parameterName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,127}$`)
	jsonNumber    = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)
	minInt256     = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 255))
)

// boundArguments preserves database/sql names and lets the pinned driver bind
// them; it never scans, rewrites or substitutes into caller SQL. Mixed styles,
// nested names, custom Valuers/Stringers and duplicate names are refused before
// the read-only probe or any connection acquisition.
func boundArguments(args []any) ([]any, error) {
	bound := make([]any, len(args))
	seen := make(map[string]bool)
	named, positional := false, false
	for i, arg := range args {
		if value, ok := arg.(sql.NamedArg); ok {
			if !parameterName.MatchString(value.Name) || seen[value.Name] {
				return nil, dal.ErrNotSupported
			}
			seen[value.Name] = true
			named = true
			scalar, err := boundNativeScalar(value.Value)
			if err != nil {
				return nil, err
			}
			bound[i] = sql.Named(value.Name, scalar)
		} else {
			positional = true
			value, err := boundNativeScalar(arg)
			if err != nil {
				return nil, err
			}
			bound[i] = value
		}
		if named && positional {
			return nil, dal.ErrNotSupported
		}
	}
	return bound, nil
}

// Native SQL can bind an explicit NULL; structured comparisons instead require
// an IS NULL condition and must not silently compile a nil constant as "= NULL".
func boundNativeScalar(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	return boundScalar(value)
}

// Only known, bounded scalars reach the driver. Exact numeric values are copied
// without float conversion. Structured NULL comparisons remain unsupported.
func boundScalar(value any) (any, error) {
	switch v := value.(type) {
	case string:
		if len(v) <= 4096 {
			return v, nil
		}
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, time.Time:
		return v, nil
	case float32:
		if !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0) && (math.Trunc(float64(v)) != float64(v) || math.Abs(float64(v)) <= 16777215) {
			return v, nil
		}
	case float64:
		if !math.IsNaN(v) && !math.IsInf(v, 0) && (math.Trunc(v) != v || math.Abs(v) <= 9007199254740991) {
			return v, nil
		}
	case *big.Int:
		if v != nil && v.BitLen() <= 256 && v.Cmp(minInt256) >= 0 {
			return new(big.Int).Set(v), nil
		}
	case decimal.Decimal:
		return boundDecimal(v)
	case json.Number:
		text := string(v)
		if len(text) > 256 || !jsonNumber.MatchString(text) {
			break
		}
		if !strings.ContainsAny(text, ".eE") {
			integer, ok := new(big.Int).SetString(text, 10)
			if ok {
				return boundScalar(integer)
			}
			break
		}
		number, err := decimal.NewFromString(text)
		if err == nil {
			return boundDecimal(number)
		}
	}
	return nil, dal.ErrNotSupported
}

// ClickHouse Decimal supports at most 76 digits and a scale up to 76. Bound the
// exponent before formatting, so hostile exponents cannot allocate huge text.
func boundDecimal(value decimal.Decimal) (any, error) {
	exponent := value.Exponent()
	if exponent < -76 || exponent > 76 {
		return nil, dal.ErrNotSupported
	}
	coefficient := value.Coefficient()
	if coefficient.BitLen() > 256 {
		return nil, dal.ErrNotSupported
	}
	digits := len(strings.TrimPrefix(coefficient.String(), "-"))
	if digits > 76 || exponent > 0 && int64(digits)+int64(exponent) > 76 {
		return nil, dal.ErrNotSupported
	}
	return decimal.NewFromBigInt(coefficient, exponent), nil
}
