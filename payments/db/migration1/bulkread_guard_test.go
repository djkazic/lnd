//go:build kvdb_sqlite && (test_db_sqlite || test_db_postgres)

package migration1

import (
	"context"
	"testing"

	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/stretchr/testify/require"
)

// TestBulkReaderPathExercised asserts that a SQL-backed KV test backend
// actually satisfies the kvBulkReader interface, so the migration takes the
// bulk-read path rather than silently falling back to the per-bucket path.
// This guards against the module-replace/interface-mismatch trap where the
// bulk path compiles but is never executed.
func TestBulkReaderPathExercised(t *testing.T) {
	kvDB := setupTestKVDB(t)

	_, ok := kvDB.(kvBulkReader)
	require.True(t, ok, "SQL-backed KV backend must implement "+
		"kvBulkReader so the migration uses the bulk-read path")
}

// TestBulkReadEmptyLeaf verifies that the bulk-read subtree reconstruction
// distinguishes a bucket (SQL NULL value) from a leaf with an empty value.
// Some SQL drivers return a nil Go slice for an empty blob, so classifying on
// the scanned []byte would misread an empty leaf as a bucket. The
// reconstruction must instead rely on the DB's own IS NULL result.
func TestBulkReadEmptyLeaf(t *testing.T) {
	ctx := context.Background()

	kvDB := setupTestKVDB(t)
	reader, ok := kvDB.(kvBulkReader)
	require.True(t, ok)

	var (
		hash        [32]byte
		normalKey   = []byte("normal")
		normalVal   = []byte("value")
		emptyKey    = []byte("empty")
		subKey      = []byte("sub")
		subChildKey = []byte("child")
	)
	hash[0] = 0x7e

	// Build: payments-root -> <hash> bucket -> {normal leaf, empty leaf,
	// nested sub-bucket}.
	require.NoError(t, kvdb.Update(kvDB, func(tx kvdb.RwTx) error {
		root, err := tx.CreateTopLevelBucket(paymentsRootBucket)
		if err != nil {
			return err
		}

		pmt, err := root.CreateBucketIfNotExists(hash[:])
		if err != nil {
			return err
		}
		if err := pmt.Put(normalKey, normalVal); err != nil {
			return err
		}
		if err := pmt.Put(emptyKey, []byte{}); err != nil {
			return err
		}

		sub, err := pmt.CreateBucketIfNotExists(subKey)
		if err != nil {
			return err
		}

		return sub.Put(subChildKey, []byte("x"))
	}, func() {}))

	rootID, err := bulkTopLevelBucketID(
		ctx, reader, reader.KVTableName(), paymentsRootBucket,
	)
	require.NoError(t, err)
	require.NotNil(t, rootID)

	root, err := bulkReadPaymentSubtrees(
		ctx, reader, reader.KVTableName(), *rootID, [][]byte{hash[:]},
	)
	require.NoError(t, err)

	pmt := root.NestedReadBucket(hash[:])
	require.NotNil(t, pmt, "payment bucket missing")

	// Normal leaf round-trips.
	require.Equal(t, normalVal, pmt.Get(normalKey))

	// Empty leaf must be a present, non-nil, zero-length value — not
	// misclassified as a bucket or reported as absent.
	emptyVal := pmt.Get(emptyKey)
	require.NotNil(t, emptyVal, "empty leaf misread (nil = absent/bucket)")
	require.Len(t, emptyVal, 0)
	require.Nil(t, pmt.NestedReadBucket(emptyKey),
		"empty leaf must not be a bucket")

	// The real nested bucket is still a bucket, not a value.
	sub := pmt.NestedReadBucket(subKey)
	require.NotNil(t, sub, "nested bucket missing")
	require.Nil(t, pmt.Get(subKey), "bucket must not be a value")
	require.Equal(t, []byte("x"), sub.Get(subChildKey))
}
