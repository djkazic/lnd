package sqldb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/lightningnetwork/lnd/sqldb/sqlc"
)

// BulkCopier bulk-loads rows into a table using the Postgres COPY protocol,
// running within the current migration transaction. It is passed to custom
// migrations that opt in via MigrationConfig.MigrationFnWithCopier.
//
// It is nil for non-Postgres backends (e.g. SQLite), which have no COPY
// protocol; callers must fall back to multi-row INSERT in that case.
type BulkCopier interface {
	// CopyInto bulk-loads rows (row-major, one []any per row matching cols)
	// into table and returns the number of rows loaded. The load runs
	// within the migration transaction, so it is atomic with the rest of
	// the migration.
	CopyInto(ctx context.Context, table string, cols []string,
		rows [][]any) (int64, error)
}

// pgxCopier implements BulkCopier using pgx CopyFrom on the migration's
// *sql.Conn. Running via the same conn that owns the migration transaction
// makes the COPY participate in that transaction.
type pgxCopier struct {
	conn *sql.Conn
}

// newBulkCopier returns a BulkCopier for conn if its driver is pgx (Postgres),
// otherwise nil (signalling callers to fall back to multi-row INSERT).
func newBulkCopier(conn *sql.Conn) BulkCopier {
	var isPgx bool
	_ = conn.Raw(func(driverConn any) error {
		_, isPgx = driverConn.(*stdlib.Conn)
		return nil
	})
	if !isPgx {
		return nil
	}

	return &pgxCopier{conn: conn}
}

// CopyInto implements BulkCopier.
func (p *pgxCopier) CopyInto(ctx context.Context, table string, cols []string,
	rows [][]any) (int64, error) {

	var n int64
	err := p.conn.Raw(func(driverConn any) error {
		pc, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("driver conn is %T, not "+
				"pgx/v5/stdlib.Conn", driverConn)
		}

		var e error
		n, e = pc.Conn().CopyFrom(
			ctx, pgx.Identifier{table}, cols, pgx.CopyFromRows(rows),
		)

		return e
	})

	return n, err
}

// execConnMigrationTx runs body inside a transaction on the given dedicated
// *sql.Conn, with the same serialization-retry behavior as the standard
// migration executor. Running on an explicit conn (rather than a pooled tx)
// is what lets a BulkCopier reach the pgx connection for COPY within the
// transaction.
func execConnMigrationTx(ctx context.Context, db *BaseDB, conn *sql.Conn,
	opts TxOptions, body func(*sqlc.Queries) error) error {

	makeTx := func() (Tx, error) {
		return conn.BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.LevelSerializable,
			ReadOnly:  opts.ReadOnly(),
		})
	}

	execTxBody := func(tx Tx) error {
		sqlTx, ok := tx.(*sql.Tx)
		if !ok {
			return fmt.Errorf("expected *sql.Tx, got %T", tx)
		}

		return body(db.WithTx(sqlTx))
	}

	rollbackTx := func(tx Tx) error {
		sqlTx, ok := tx.(*sql.Tx)
		if !ok {
			return fmt.Errorf("expected *sql.Tx, got %T", tx)
		}

		_ = sqlTx.Rollback()

		return nil
	}

	onBackoff := func(retry int, delay time.Duration) {
		log.Tracef("Retrying migration transaction due to tx "+
			"serialization error, attempt_number=%v, delay=%v",
			retry, delay)
	}

	return ExecuteSQLTransactionWithRetry(
		ctx, makeTx, rollbackTx, execTxBody, onBackoff,
		DefaultNumTxRetries,
	)
}
