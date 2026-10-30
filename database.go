// Package dalgo2clickhouse provides bounded, read-only ClickHouse exploration.
package dalgo2clickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"net"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

var (
	ErrConfig           = errors.New("clickhouse: invalid configuration")
	ErrClosed           = errors.New("clickhouse: database closed")
	ErrBackend          = errors.New("clickhouse: read failed")
	ErrLimit            = errors.New("clickhouse: operation budget exceeded")
	ErrReadOnlyRequired = errors.New("clickhouse: server account must enforce readonly=1")
	ErrUnsupportedValue = errors.New("clickhouse: unsupported native value")
)

// Config contains no DSN, SQL settings, debug callbacks or arbitrary transports.
// Tables is the structured-query/catalog allowlist in Database; each value is
// an allowed column list (empty means all columns visible to the credential).
// NativeSQL is a separate, credential-scoped permission and is disabled by default.
type Config struct {
	Address   string
	Database  string
	Username  string
	Password  string
	Tables    map[string][]string
	NativeSQL bool
	HTTP      bool
	// PlaintextLoopback permits unencrypted transport only to a literal loopback IP.
	PlaintextLoopback bool
	Timeout           time.Duration
	MaxRows           int
	MaxBytes          int64
}

func (Config) String() string     { return "clickhouse.Config{redacted}" }
func (c Config) GoString() string { return c.String() }

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

func resolveConfig(c Config) (Config, error) {
	host, port, err := net.SplitHostPort(c.Address)
	if err != nil || host == "" || port == "" || !identifier.MatchString(c.Database) || c.Username == "" {
		return Config{}, ErrConfig
	}
	if c.PlaintextLoopback {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return Config{}, ErrConfig
		}
	}
	if c.Timeout == 0 {
		c.Timeout = 10 * time.Second
	}
	if c.MaxRows == 0 {
		c.MaxRows = 1000
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 8 << 20
	}
	if c.Timeout < time.Millisecond || c.Timeout > time.Minute || c.MaxRows < 1 || c.MaxRows > 100000 || c.MaxBytes < 1 || c.MaxBytes > 64<<20 || len(c.Tables) > 256 || (len(c.Tables) == 0 && !c.NativeSQL) {
		return Config{}, ErrConfig
	}
	tables := make(map[string][]string, len(c.Tables))
	for table, columns := range c.Tables {
		if !identifier.MatchString(table) || len(columns) > 256 {
			return Config{}, ErrConfig
		}
		seen := map[string]bool{}
		for _, column := range columns {
			if !identifier.MatchString(column) || seen[column] {
				return Config{}, ErrConfig
			}
			seen[column] = true
		}
		tables[table] = append([]string(nil), columns...)
	}
	c.Tables = tables
	return c, nil
}

// Database embeds the framework-owned sealed DB. Close owns its private pool;
// callers never lend a pool whose transaction/write capabilities could leak.
type Database struct {
	dal.DB
	cfg      Config
	pool     *sql.DB
	closed   atomic.Bool
	life     context.Context
	stop     context.CancelFunc
	once     sync.Once
	closeErr error
}

// NewDB validates configuration without network I/O. Every operation subsequently
// verifies readonly=1 on its own leased connection before sending its statement.
func NewDB(ctx context.Context, cfg Config) (dal.DB, error) {
	if ctx == nil {
		return nil, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, safeError(ctx, err)
	}
	c, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}
	protocol := clickhouse.Native
	if c.HTTP {
		protocol = clickhouse.HTTP
	}
	var tlsConfig *tls.Config
	if !c.PlaintextLoopback {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	pool := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{c.Address}, Protocol: protocol, TLS: tlsConfig,
		Auth:        clickhouse.Auth{Database: c.Database, Username: c.Username, Password: c.Password},
		DialTimeout: c.Timeout, ReadTimeout: c.Timeout, MaxOpenConns: 4, MaxIdleConns: 2,
		BlockBufferSize: 1, FreeBufOnConnRelease: true,
	})
	d := newDatabase(c, pool)
	return d, nil
}

func newDatabase(c Config, pool *sql.DB) *Database {
	life, stop := context.WithCancel(context.Background())
	d := &Database{cfg: c, pool: pool, life: life, stop: stop}
	d.DB = dal.NewDB(&backend{db: d})
	return d
}

func (d *Database) Close() error {
	d.once.Do(func() {
		d.closed.Store(true)
		d.stop()
		if err := d.pool.Close(); err != nil {
			d.closeErr = ErrBackend
		}
	})
	return d.closeErr
}

// noValues retains caller cancellation/deadlines but not private driver options
// (settings, JWT, external tables, callbacks) smuggled through a caller context.
type noValues struct{ context.Context }

func (noValues) Value(any) any { return nil }

type operation struct {
	ctx    context.Context
	conn   *sql.Conn
	cancel context.CancelFunc
	stop   func() bool
}

func (d *Database) begin(ctx context.Context) (*operation, error) {
	if ctx == nil {
		return nil, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, safeError(ctx, err)
	}
	if d.closed.Load() {
		return nil, ErrClosed
	}
	opCtx, cancel := context.WithTimeout(noValues{ctx}, d.cfg.Timeout)
	stop := context.AfterFunc(d.life, cancel)
	conn, err := d.pool.Conn(opCtx)
	if err != nil {
		stop()
		cancel()
		return nil, safeError(ctx, err)
	}
	op := &operation{ctx: opCtx, conn: conn, cancel: cancel, stop: stop}
	// clickhouse-go derives max_execution_time from deadlines, including this
	// probe. Profiles must allow that setting within an administrator-owned cap.
	var readonly uint64
	if err = conn.QueryRowContext(opCtx, "SELECT getSetting('readonly')").Scan(&readonly); err != nil {
		err = safeError(opCtx, err)
		op.done()
		return nil, err
	}
	if readonly != 1 {
		op.done()
		return nil, ErrReadOnlyRequired
	}
	return op, nil
}

func (o *operation) done() {
	_ = o.conn.Close()
	o.stop()
	o.cancel()
}

func safeError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return context.Canceled
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return context.DeadlineExceeded
		}
		return ErrBackend
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, ErrLimit) {
		return ErrLimit
	}
	return ErrBackend // Never retain a driver/server error or its untrusted cause.
}

type backend struct {
	dal.ConcurrencyAvailable
	db *Database
}

var _ dal.Backend = (*backend)(nil)

func (b *backend) ID() string         { return "clickhouse:" + b.db.cfg.Database }
func (*backend) Adapter() dal.Adapter { return dal.NewAdapter("dalgo2clickhouse", "0.1.0") }
func (*backend) Schema() dal.Schema   { return nil }
func (*backend) RunReadonlyTransaction(context.Context, dal.ROTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*backend) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*backend) Get(context.Context, record.Record) error          { return dal.ErrNotSupported }
func (*backend) Exists(context.Context, *record.Key) (bool, error) { return false, dal.ErrNotSupported }
func (*backend) GetMulti(context.Context, []record.Record) error   { return dal.ErrNotSupported }
func (*backend) ExecuteQueryToRecordsReader(context.Context, dal.Query) (dal.RecordsReader, error) {
	return nil, dal.ErrNotSupported
}
func (b *backend) ExecuteQueryToRecordsetReader(ctx context.Context, q dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	plan, err := compile(q, b.db.cfg)
	if err != nil {
		return nil, err
	}
	return b.db.read(ctx, plan.sql, plan.args, recordset.NewOptions(options...).Name())
}

// NativeQuerier is deliberately separate from structured table/column allowlists.
// SQL is authored by the caller, not sanitized by this adapter. The configured
// server credential/profile is its authority boundary, including table functions.
type NativeQuerier interface {
	QueryNative(context.Context, string, ...any) (dal.RecordsetReader, error)
}

func (d *Database) QueryNative(ctx context.Context, statement string, args ...any) (dal.RecordsetReader, error) {
	if !d.cfg.NativeSQL {
		return nil, dal.ErrNotSupported
	}
	if len(statement) == 0 || len(statement) > 64<<10 || len(args) > 256 {
		return nil, ErrLimit
	}
	bound, err := boundArguments(args)
	if err != nil {
		return nil, err
	}
	return d.read(ctx, statement, bound, "clickhouse-native")
}
