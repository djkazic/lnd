//go:build test_db_postgres

package migration1

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/lightningnetwork/lnd/payments/db/migration1/sqlc"
	"github.com/stretchr/testify/require"
)

// testPgxCopier implements BulkCopier using the pgx v5 COPY protocol, running
// within the migration transaction via the shared *sql.Conn. It mirrors the
// production copier and lets us verify COPY encodes rows identically to the
// multi-row INSERT path.
type testPgxCopier struct {
	conn  *sql.Conn
	calls int
	rows  int64
}

func (p *testPgxCopier) CopyInto(ctx context.Context, table string,
	cols []string, rows [][]any) (int64, error) {

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
	p.calls++
	p.rows += n

	return n, err
}

// TestMigrationCopyPathDataIntegrity runs the migration with the pgx COPY
// copier enabled and deep-compares every migrated payment against its KV
// source. This verifies COPY encodes the HTLC attempt columns byte-for-byte
// the same as the multi-row INSERT path — structural validation alone only
// checks counts, not contents.
func TestMigrationCopyPathDataIntegrity(t *testing.T) {
	ctx := context.Background()

	kvDB := setupTestKVDB(t)
	const numPayments = 40
	populateTestPayments(t, kvDB, numPayments)

	sqlStore := setupTestSQLDB(t)

	// Run the migration on a *sql.Conn-based transaction with the COPY
	// copier injected, mirroring the intended production wiring.
	rawDB := sqlStore.db.(*testBatchedSQLQueries).db
	conn, err := rawDB.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	require.NoError(t, err)

	copier := &testPgxCopier{conn: conn}
	migErr := MigratePaymentsKVToSQL(
		ctx, kvDB, sqlc.New(tx), &SQLStoreConfig{
			QueryCfg: sqlStore.cfg.QueryCfg,
			Copier:   copier,
		},
	)
	require.NoError(t, migErr)
	require.NoError(t, tx.Commit())

	// The copier must actually have been used (guards against a silent
	// fallback that would make this test meaningless).
	require.Positive(t, copier.calls, "COPY path was not exercised")
	require.Positive(t, copier.rows, "no attempts were COPY'd")

	// Deep-compare every payment: KV source vs SQL via SQLStore.FetchPayment.
	for i := 0; i < numPayments; i++ {
		hash := createTestPaymentHash(t, i)
		assertPaymentDataMatches(t, ctx, kvDB, sqlStore, hash)
	}
}
