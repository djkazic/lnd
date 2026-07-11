package migration1

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightningnetwork/lnd/kvdb"
	"golang.org/x/time/rate"
)

// migratePaymentsBulk runs the payment migration reading the source KV data in
// bulk from the underlying SQL kv table. It mirrors the per-bucket path in
// MigratePaymentsKVToSQL but replaces the millions of per-bucket round-trips
// with a handful of queries per batch, then reuses the exact same parsing,
// bulk-insert and validation code.
func migratePaymentsBulk(ctx context.Context, reader kvBulkReader,
	kvBackend kvdb.Backend, sqlDB SQLMigrationQueries, cfg *SQLStoreConfig,
	stats *MigrationStats, startTime time.Time) error {

	table := reader.KVTableName()

	// Read the switch sequencer horizon for legacy attempt ID allocation.
	switchSeq, err := bulkSwitchSequence(ctx, reader, table)
	if err != nil {
		return fmt.Errorf("read switch sequence: %w", err)
	}
	attemptIDAllocator := newAttemptIDAllocator(switchSeq)

	// Resolve the top-level payments and index bucket ids.
	rootID, err := bulkTopLevelBucketID(ctx, reader, table, paymentsRootBucket)
	if err != nil {
		return fmt.Errorf("resolve payments bucket: %w", err)
	}
	if rootID == nil {
		log.Infof("No payments bucket found - database is empty")

		return finishMigration(
			ctx, kvBackend, sqlDB, stats, startTime,
			attemptIDAllocator,
		)
	}

	indexID, err := bulkTopLevelBucketID(
		ctx, reader, table, paymentsIndexBucket,
	)
	if err != nil {
		return fmt.Errorf("resolve index bucket: %w", err)
	}
	if indexID == nil {
		return fmt.Errorf("index bucket does not exist")
	}

	// Load the entire payment index (sequence -> hash) in order with a
	// single query.
	indexEntries, err := bulkLoadIndex(ctx, reader, table, *indexID)
	if err != nil {
		return fmt.Errorf("bulk load index: %w", err)
	}

	indexedPayments := int64(len(indexEntries))
	log.Infof("Found ~%d index entries to migrate (includes duplicates)",
		indexedPayments)

	// Set up a progress reporter with rolling-window ETA.
	reporter := &migrationProgressReporter{
		startTime:          startTime,
		stats:              stats,
		indexedPayments:    indexedPayments,
		rateWindowDuration: defaultRateWindowDuration,
		windowStart:        startTime,
	}
	reportInterval := rate.Sometimes{Interval: 5 * time.Second}

	batchSize := int(cfg.QueryCfg.MaxBatchSize)
	for start := 0; start < len(indexEntries); start += batchSize {
		end := min(start+batchSize, len(indexEntries))
		chunk := indexEntries[start:end]

		// Deserialize the payment hashes for this batch so we can bulk
		// read their subtrees.
		hashes := make([][]byte, 0, len(chunk))
		for i := range chunk {
			hash, err := deserializePaymentIndex(
				bytes.NewReader(chunk[i].indexVal),
			)
			if err != nil {
				return err
			}

			hashes = append(hashes, cloneBytes(hash[:]))
		}

		// Bulk read the subtrees for this batch into an in-memory
		// payments bucket.
		paymentsBucket, err := bulkReadPaymentSubtrees(
			ctx, reader, table, *rootID, hashes,
		)
		if err != nil {
			return fmt.Errorf("bulk read payment subtrees: %w", err)
		}

		// Parse each payment, reusing the exact per-bucket parsing code
		// against the in-memory bucket.
		batch := make([]*preparedPayment, 0, len(chunk))
		for i := range chunk {
			reportInterval.Do(reporter.report)

			prep, err := prepareIndexEntry(
				chunk[i].seqKey, chunk[i].indexVal,
				paymentsBucket, stats, attemptIDAllocator,
			)
			if err != nil {
				return err
			}
			if prep == nil {
				continue
			}

			batch = append(batch, prep)
		}

		if len(batch) > 0 {
			if err := flushAndValidateBatch(
				ctx, kvBackend, sqlDB, cfg, batch,
			); err != nil {
				return err
			}
		}
	}

	return finishMigration(
		ctx, kvBackend, sqlDB, stats, startTime, attemptIDAllocator,
	)
}

// kvBulkReader is implemented by SQL-backed kv stores (postgres_kvdb and
// sqlite_kvdb, via the sqlbase package) to allow the migration to bulk-read the
// payment subtree directly from the underlying kv table. Traversing the bucket
// hierarchy through the normal kvdb API costs one SQL round-trip per bucket
// lookup, Get and cursor step; for a large payments database that is millions
// of round-trips. When the KV backend implements this interface the migration
// reads whole batches with a handful of queries instead.
//
// bbolt backends do not implement this interface and fall back to the regular
// per-bucket traversal, which is cheap for a memory-mapped local file anyway.
type kvBulkReader interface {
	// KVTableName returns the name of the underlying kv table.
	KVTableName() string

	// KVBulkQuery runs a read-only query against the kv table connection.
	// The caller must close the returned rows.
	KVBulkQuery(ctx context.Context, query string,
		args ...interface{}) (*sql.Rows, error)
}

// memBucket is an in-memory implementation of kvdb.RBucket (walletdb.ReadBucket)
// reconstructed from a bulk read of the underlying kv table. It lets the bulk
// read path reuse the exact same payment deserialization/parsing code as the
// per-bucket path.
type memBucket struct {
	// values holds the leaf key/value pairs of this bucket. A present but
	// empty value is stored as a non-nil empty slice, mirroring the kv
	// store's own Get semantics.
	values map[string][]byte

	// buckets holds the nested sub-buckets of this bucket.
	buckets map[string]*memBucket

	// keys is the sorted union of value and bucket keys, used to make
	// ForEach iterate in the same key order as the real backend.
	keys []string
}

func newMemBucket() *memBucket {
	return &memBucket{
		values:  make(map[string][]byte),
		buckets: make(map[string]*memBucket),
	}
}

// finalize computes the sorted key order for this bucket and all of its nested
// buckets, so ForEach matches the ascending-key iteration order of the real
// backend.
func (m *memBucket) finalize() {
	m.keys = make([]string, 0, len(m.values)+len(m.buckets))
	for k := range m.values {
		m.keys = append(m.keys, k)
	}
	for k := range m.buckets {
		m.keys = append(m.keys, k)
	}
	sort.Strings(m.keys)

	for _, b := range m.buckets {
		b.finalize()
	}
}

// Get returns the value for the given key, or nil if it does not exist (or the
// key names a nested bucket).
func (m *memBucket) Get(key []byte) []byte {
	if v, ok := m.values[string(key)]; ok {
		return v
	}

	return nil
}

// NestedReadBucket returns the nested bucket for the given key, or nil.
func (m *memBucket) NestedReadBucket(key []byte) walletdb.ReadBucket {
	if b, ok := m.buckets[string(key)]; ok {
		return b
	}

	return nil
}

// ForEach invokes cb for every key in this bucket in ascending key order.
// Nested buckets are reported with a nil value, matching the real backend.
func (m *memBucket) ForEach(cb func(k, v []byte) error) error {
	for _, k := range m.keys {
		var v []byte
		if val, ok := m.values[k]; ok {
			v = val
		}

		if err := cb([]byte(k), v); err != nil {
			return err
		}
	}

	return nil
}

// ReadCursor is not used by the migration parsing path and is therefore not
// implemented for the in-memory bucket.
func (m *memBucket) ReadCursor() walletdb.ReadCursor {
	panic("memBucket.ReadCursor: not supported in migration bulk read path")
}

// Sequence returns the bucket's sequence number. The migration parsing path
// reads per-payment sequence numbers from a dedicated key (not the bucket
// sequence), so this always returns 0 for the in-memory bucket.
func (m *memBucket) Sequence() uint64 {
	return 0
}

// bulkIndexEntry is a single (sequence key, index value) pair read from the
// payments index bucket.
type bulkIndexEntry struct {
	seqKey   []byte
	indexVal []byte
}

// bulkTopLevelBucketID looks up the id of a top-level (parent_id IS NULL)
// bucket by key. It returns (nil, nil) if the bucket does not exist.
func bulkTopLevelBucketID(ctx context.Context, reader kvBulkReader,
	table string, key []byte) (*int64, error) {

	query := "SELECT id FROM " + table +
		" WHERE parent_id IS NULL AND key = $1 AND value IS NULL"

	rows, err := reader.KVBulkQuery(ctx, query, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, rows.Err()
	}

	var id int64
	if err := rows.Scan(&id); err != nil {
		return nil, err
	}

	return &id, rows.Err()
}

// bulkSwitchSequence reads the switch sequencer horizon (the Sequence() of the
// top-level switchNextPaymentIDKey bucket) with a single query. It returns 0 if
// the bucket or sequence is absent.
func bulkSwitchSequence(ctx context.Context, reader kvBulkReader,
	table string) (uint64, error) {

	query := "SELECT sequence FROM " + table +
		" WHERE parent_id IS NULL AND key = $1"

	rows, err := reader.KVBulkQuery(ctx, query, switchNextPaymentIDKey)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, rows.Err()
	}

	var seq sql.NullInt64
	if err := rows.Scan(&seq); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	if !seq.Valid || seq.Int64 < 0 {
		return 0, nil
	}

	return uint64(seq.Int64), nil
}

// bulkLoadIndex reads the entire payments index bucket in ascending key
// (sequence) order in a single query.
func bulkLoadIndex(ctx context.Context, reader kvBulkReader, table string,
	indexID int64) ([]bulkIndexEntry, error) {

	query := "SELECT key, value FROM " + table +
		" WHERE parent_id = $1 ORDER BY key"

	rows, err := reader.KVBulkQuery(ctx, query, indexID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []bulkIndexEntry
	for rows.Next() {
		var key, value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}

		entries = append(entries, bulkIndexEntry{
			seqKey:   cloneBytes(key),
			indexVal: cloneBytes(value),
		})
	}

	return entries, rows.Err()
}

// bulkReadPaymentSubtrees bulk-reads the full kv subtree for the given payment
// hashes and reconstructs an in-memory payments bucket that maps each hash to
// its payment bucket. Hashes without a corresponding payment bucket are simply
// absent from the result (the caller handles them the same way it handles a
// missing bucket in the per-bucket path).
func bulkReadPaymentSubtrees(ctx context.Context, reader kvBulkReader,
	table string, rootID int64, hashes [][]byte) (*memBucket, error) {

	root := newMemBucket()
	if len(hashes) == 0 {
		root.finalize()
		return root, nil
	}

	// Resolve the payment bucket id for each hash.
	byID := make(map[int64]*memBucket)
	idArgs := make([]interface{}, 0, len(hashes)+1)
	idArgs = append(idArgs, rootID)
	for _, h := range hashes {
		idArgs = append(idArgs, h)
	}

	// $1 is the root id; $2.. are the hashes.
	idQuery := "SELECT id, key FROM " + table +
		" WHERE parent_id = $1 AND value IS NULL AND key IN (" +
		placeholders(2, len(hashes)) + ")"

	bucketIDs := make([]int64, 0, len(hashes))
	idRows, err := reader.KVBulkQuery(ctx, idQuery, idArgs...)
	if err != nil {
		return nil, err
	}
	err = func() error {
		defer idRows.Close()
		for idRows.Next() {
			var id int64
			var key []byte
			if err := idRows.Scan(&id, &key); err != nil {
				return err
			}

			mb := newMemBucket()
			byID[id] = mb
			bucketIDs = append(bucketIDs, id)
			root.buckets[string(key)] = mb
		}

		return idRows.Err()
	}()
	if err != nil {
		return nil, err
	}

	if len(bucketIDs) == 0 {
		root.finalize()
		return root, nil
	}

	// Read the whole subtree rooted at those payment buckets in one
	// recursive query.
	args := make([]interface{}, 0, len(bucketIDs))
	for _, id := range bucketIDs {
		args = append(args, id)
	}

	// A row is a bucket iff its value column is SQL NULL; a leaf has a
	// (possibly empty) value. We must not infer this from the scanned Go
	// []byte, because some drivers (e.g. SQLite) return a nil slice for an
	// empty blob, which would make an empty leaf indistinguishable from a
	// bucket. Instead we let the database report it via an explicit
	// CASE ... value IS NULL column (1 = bucket), which is integer-typed on
	// both Postgres and SQLite.
	cteQuery := "WITH RECURSIVE subtree(id, parent_id, key, value) AS (" +
		"SELECT id, parent_id, key, value FROM " + table +
		" WHERE parent_id IN (" + placeholders(1, len(bucketIDs)) + ") " +
		"UNION ALL " +
		"SELECT k.id, k.parent_id, k.key, k.value FROM " + table + " k " +
		"JOIN subtree s ON k.parent_id = s.id) " +
		"SELECT id, parent_id, key, value, " +
		"CASE WHEN value IS NULL THEN 1 ELSE 0 END FROM subtree"

	rows, err := reader.KVBulkQuery(ctx, cteQuery, args...)
	if err != nil {
		return nil, err
	}

	type rawRow struct {
		id       int64
		parentID int64
		key      []byte
		value    []byte
		isBucket bool
	}

	var raws []rawRow
	err = func() error {
		defer rows.Close()
		for rows.Next() {
			var (
				id, parentID int64
				key, value   []byte
				bucketFlag   int
			)
			if err := rows.Scan(
				&id, &parentID, &key, &value, &bucketFlag,
			); err != nil {
				return err
			}

			isBucket := bucketFlag == 1

			// For a leaf, normalize an empty value to a non-nil
			// empty slice so Get mirrors the backend (which returns
			// an empty, non-nil slice for an empty value and nil
			// only when the key is absent).
			leafValue := cloneBytes(value)
			if !isBucket && leafValue == nil {
				leafValue = []byte{}
			}

			raws = append(raws, rawRow{
				id:       id,
				parentID: parentID,
				key:      cloneBytes(key),
				value:    leafValue,
				isBucket: isBucket,
			})
		}

		return rows.Err()
	}()
	if err != nil {
		return nil, err
	}

	// Pass 1: create an in-memory bucket for every bucket row.
	for _, r := range raws {
		if r.isBucket {
			if _, ok := byID[r.id]; !ok {
				byID[r.id] = newMemBucket()
			}
		}
	}

	// Pass 2: attach every row to its parent bucket.
	for _, r := range raws {
		parent, ok := byID[r.parentID]
		if !ok {
			return nil, fmt.Errorf("bulk read: missing parent "+
				"bucket for row id=%d parent_id=%d", r.id,
				r.parentID)
		}

		if r.isBucket {
			parent.buckets[string(r.key)] = byID[r.id]
		} else {
			parent.values[string(r.key)] = r.value
		}
	}

	root.finalize()

	return root, nil
}

// placeholders returns a comma-separated list of n numbered SQL placeholders
// ($start, $start+1, ...).
func placeholders(start, n int) string {
	var b strings.Builder
	b.Grow(n * 6)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "$%d", start+i)
	}

	return b.String()
}

// cloneBytes returns a copy of b (nil for nil).
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}

	out := make([]byte, len(b))
	copy(out, b)

	return out
}
