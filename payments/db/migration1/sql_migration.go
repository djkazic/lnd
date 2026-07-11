package migration1

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/payments/db/migration1/sqlc"
	"golang.org/x/time/rate"
)

const (
	// defaultRateWindowDuration is the rolling window used for ETA
	// calculation during migration progress reporting.
	defaultRateWindowDuration = 300 * time.Second
)

var (
	// switchNextPaymentIDKey is the switch sequencer bucket key. This is
	// intentionally kept in sync with htlcswitch.nextPaymentIDKey without
	// importing htlcswitch into the migration package.
	switchNextPaymentIDKey = []byte("next-payment-id-key")
)

// MigrationStats tracks migration progress.
type MigrationStats struct {
	TotalPayments      int64
	SuccessfulPayments int64
	FailedPayments     int64
	InFlightPayments   int64
	InitiatedPayments  int64
	TotalAttempts      int64
	SettledAttempts    int64
	FailedAttempts     int64
	InFlightAttempts   int64
	TotalHops          int64
	DuplicatePayments  int64
	DuplicateEntries   int64
	SkippedPayments    int64
	MigrationDuration  time.Duration
}

// migrationProgressReporter tracks rolling-window ETA state and logs
// periodic progress lines during the payment migration.
type migrationProgressReporter struct {
	startTime          time.Time
	stats              *MigrationStats
	indexedPayments    int64
	rateWindowDuration time.Duration
	windowStart        time.Time
	windowPayments     int64
	prevWindowRate     float64
}

// report logs a progress line showing the current migration rate and ETA.
func (p *migrationProgressReporter) report() {
	elapsed := time.Since(p.startTime)
	if elapsed <= 0 || p.stats.TotalPayments == 0 {
		return
	}

	paymentRate := float64(p.stats.TotalPayments) / elapsed.Seconds()
	attemptRate := float64(p.stats.TotalAttempts) / elapsed.Seconds()

	var pctStr string
	if p.indexedPayments > 0 {
		pct := float64(p.stats.TotalPayments) /
			float64(p.indexedPayments) * 100
		pctStr = fmt.Sprintf(" (~%.1f%%)", pct)
	}

	// Compute ETA using the rolling window rate so it responds to
	// recent throughput changes. When the window expires we save
	// the previous rate as a fallback for the reset tick.
	windowElapsed := time.Since(p.windowStart)
	if windowElapsed >= p.rateWindowDuration {
		n := p.stats.TotalPayments - p.windowPayments
		p.prevWindowRate = float64(n) / windowElapsed.Seconds()
		p.windowPayments = p.stats.TotalPayments
		p.windowStart = time.Now()
		windowElapsed = 0
	}

	var etaStr string
	if p.indexedPayments > 0 {
		windowRate := p.prevWindowRate
		if windowElapsed > 0 {
			n := p.stats.TotalPayments - p.windowPayments
			windowRate = float64(n) / windowElapsed.Seconds()
		}

		if windowRate > 0 {
			remaining := p.indexedPayments - p.stats.TotalPayments
			secs := float64(remaining) / windowRate
			eta := time.Duration(secs) * time.Second
			etaStr = fmt.Sprintf(
				" | ETA: ~%v", eta.Round(time.Second),
			)
		}
	}

	log.Infof("Progress: %d payments%s, %d attempts, %d hops | Rate: %.1f "+
		"pmt/s, %.1f att/s | Elapsed: %v%s", p.stats.TotalPayments,
		pctStr, p.stats.TotalAttempts, p.stats.TotalHops,
		paymentRate, attemptRate, elapsed.Round(time.Second), etaStr)
}

// MigratePaymentsKVToSQL migrates payments from KV to SQL and validates
// migrated data in batches. Callers are responsible for executing this within
// a single SQL transaction if atomicity is required.
func MigratePaymentsKVToSQL(ctx context.Context, kvBackend kvdb.Backend,
	sqlDB SQLMigrationQueries, cfg *SQLStoreConfig) error {

	if cfg == nil {
		return fmt.Errorf("missing SQL store config for migration")
	}

	if cfg.QueryCfg == nil {
		return fmt.Errorf("missing SQL store config for validation")
	}

	if cfg.QueryCfg.MaxBatchSize == 0 {
		return fmt.Errorf("invalid max batch size for validation")
	}

	stats := &MigrationStats{}
	startTime := time.Now()

	log.Infof("Starting payment migration from KV to SQL...")

	// If the KV backend is SQL-backed (postgres_kvdb / sqlite_kvdb), read
	// payments in bulk directly from the underlying table instead of doing
	// one round-trip per bucket/Get/cursor step. bbolt backends fall back to
	// the per-bucket traversal below, which is cheap for a local mmap'd file.
	if reader, ok := kvBackend.(kvBulkReader); ok {
		return migratePaymentsBulk(
			ctx, reader, kvBackend, sqlDB, cfg, stats, startTime,
		)
	}

	var (
		batch []*preparedPayment

		reportInterval = rate.Sometimes{Interval: 5 * time.Second}
	)

	indexedPayments, nextSwitchPaymentID, err := collectMigrationState(
		kvBackend,
	)
	if err != nil {
		return fmt.Errorf("collect payment migration state: %w", err)
	}

	attemptIDAllocator := newAttemptIDAllocator(nextSwitchPaymentID)

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

	// Open the KV backend in read-only mode.
	err = kvBackend.View(func(kvTx kvdb.RTx) error {
		// In case we start with an empty database, there are no
		// payments to migrate.
		paymentsBucket := kvTx.ReadBucket(paymentsRootBucket)
		if paymentsBucket == nil {
			log.Infof("No payments bucket found - database is " +
				"empty")

			return nil
		}

		// The index bucket maps sequence number -> payment hash.
		indexes := kvTx.ReadBucket(paymentsIndexBucket)
		if indexes == nil {
			return fmt.Errorf("index bucket does not exist")
		}

		// We iterate over all sequence numbers in the index bucket to
		// make sure we have the correct order of payments. Otherwise,
		// if we just loop over the payments bucket, we might get the
		// payments not in the chronological order but rather the
		// lexicographical order of the payment hashes.
		return indexes.ForEach(func(seqKey, indexVal []byte) error {
			reportInterval.Do(reporter.report)

			prep, err := prepareIndexEntry(
				seqKey, indexVal, paymentsBucket, stats,
				attemptIDAllocator,
			)
			if err != nil {
				return err
			}
			if prep == nil {
				return nil
			}

			batch = append(batch, prep)
			if uint32(len(batch)) < cfg.QueryCfg.MaxBatchSize {
				return nil
			}

			if err := flushAndValidateBatch(
				ctx, kvBackend, sqlDB, cfg, batch,
			); err != nil {
				return err
			}
			batch = batch[:0]

			return nil
		})
	}, func() {})

	if err != nil {
		return fmt.Errorf("migrate payments: %w", err)
	}

	// Flush and validate any remaining payments in the batch.
	if len(batch) > 0 {
		if err := flushAndValidateBatch(
			ctx, kvBackend, sqlDB, cfg, batch,
		); err != nil {
			return err
		}
	}

	return finishMigration(
		ctx, kvBackend, sqlDB, stats, startTime, attemptIDAllocator,
	)
}

// finishMigration performs the shared post-migration steps: a final payment
// count sanity check, advancing the switch payment ID sequencer past any
// synthetic legacy attempt IDs, recording the duration and printing the
// summary.
func finishMigration(ctx context.Context, kvBackend kvdb.Backend,
	sqlDB SQLMigrationQueries, stats *MigrationStats, startTime time.Time,
	attemptIDAllocator *attemptIDAllocator) error {

	// Validate the total number of payments as an additional sanity check.
	if err := validatePaymentCounts(
		ctx, sqlDB, stats.TotalPayments,
	); err != nil {
		return err
	}

	if err := advanceSwitchPaymentIDSequence(
		kvBackend, attemptIDAllocator.nextID,
	); err != nil {
		return fmt.Errorf("advance switch payment ID sequence: %w", err)
	}

	stats.MigrationDuration = time.Since(startTime)

	printMigrationSummary(stats)

	return nil
}

// normalizeTimeForSQL converts a timestamp into the representation we persist
// and compare against in SQL:
// - drops any monotonic clock reading (SQL can't store it),
// - forces UTC for deterministic comparisons across environments.
//
// A zero time is returned unchanged.
func normalizeTimeForSQL(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}

	return time.Unix(0, t.UnixNano()).UTC()
}

// collectMigrationState scans the payment index once to gather progress
// information and reads the switch sequencer horizon used for legacy attempt
// ID allocation.
func collectMigrationState(kvBackend kvdb.Backend) (int64, uint64, error) {
	var (
		indexedPayments     int64
		nextSwitchPaymentID uint64
	)
	err := kvBackend.View(func(kvTx kvdb.RTx) error {
		// Read the switch sequencer horizon that legacy zero-ID
		// attempts will allocate from if needed.
		seqBucket := kvTx.ReadBucket(switchNextPaymentIDKey)
		if seqBucket != nil {
			nextSwitchPaymentID = seqBucket.Sequence()
		}

		// If there are no payments, there is nothing to count or
		// migrate.
		paymentsBucket := kvTx.ReadBucket(paymentsRootBucket)
		if paymentsBucket == nil {
			log.Infof("No payments bucket found - database is " +
				"empty")

			return nil
		}

		// Count index entries for approximate progress reporting. The
		// main migration still streams over this index in order.
		indexes := kvTx.ReadBucket(paymentsIndexBucket)
		if indexes == nil {
			return fmt.Errorf("index bucket does not exist")
		}

		return indexes.ForEach(func(_, _ []byte) error {
			indexedPayments++
			return nil
		})
	}, func() {
		indexedPayments = 0
		nextSwitchPaymentID = 0
	})
	if err != nil {
		return 0, 0, err
	}

	return indexedPayments, nextSwitchPaymentID, nil
}

// attemptIDAllocator tracks the next switch payment ID that is safe to hand
// out after migration.
type attemptIDAllocator struct {
	// nextID is the in-memory counter used for the next synthetic attempt
	// ID and the final switch sequencer horizon to persist.
	nextID uint64
}

// newAttemptIDAllocator creates a new attempt ID allocator.
//
// The SQL schema requires payment_htlc_attempts.attempt_index to be globally
// unique because attempt-related rows use it as their stable identifier. Very
// old KV payments can contain attempt ID zero, which represented an unknown
// legacy value and cannot be preserved in SQL without colliding with every
// other such legacy attempt.
//
// Non-zero attempt IDs were allocated from the switch sequencer. The sequencer
// persists a horizon: it reads Sequence() as the next ID to hand out, then
// writes a higher value before returning that ID. This means the value stored
// in switchNextPaymentIDKey is already beyond all IDs it handed out. We
// therefore allocate replacement IDs for legacy zero attempts from that horizon
// and persist the final next-unused value once migration succeeds. These old
// attempts may receive high attempt_index values, which means that within a
// payment that mixes remapped and non-remapped attempts the remapped ones will
// sort after the originals when SQL queries order by attempt_index. This is
// acceptable because it only affects very old payments whose attempt IDs were
// already unknown, and intra-payment attempt ordering is not a load-bearing
// user-visible invariant; uniqueness and future non-collision with the switch
// sequencer are the actual invariants we need to preserve.
func newAttemptIDAllocator(nextSwitchPaymentID uint64) *attemptIDAllocator {
	return &attemptIDAllocator{
		nextID: nextSwitchPaymentID,
	}
}

// allocateLegacyAttemptID returns a new unique attempt ID for a legacy payment.
// It uses the in-memory counter initialized from the switch payment ID
// sequencer horizon.
func (a *attemptIDAllocator) allocateLegacyAttemptID() (uint64, error) {
	// The runtime switch sequencer never hands out ID zero: when its
	// persisted sequence is zero, it starts by issuing ID one. Mirror
	// that behavior for legacy attempts on otherwise idle nodes whose
	// switch sequencer bucket has not allocated a batch yet.
	if a.nextID == 0 {
		a.nextID = 1
	}

	if a.nextID == ^uint64(0) {
		return 0, fmt.Errorf("cannot allocate legacy attempt ID: "+
			"switch payment ID sequence is %d", a.nextID)
	}

	attemptID := a.nextID
	a.nextID++

	return attemptID, nil
}

// advanceSwitchPaymentIDSequence makes sure the switch sequencer cannot later
// hand out an ID that was already present in a migrated payment attempt.
func advanceSwitchPaymentIDSequence(kvBackend kvdb.Backend,
	nextID uint64) error {

	return kvdb.Update(kvBackend, func(tx kvdb.RwTx) error {
		seqBucket := tx.ReadWriteBucket(switchNextPaymentIDKey)
		if seqBucket == nil {
			if nextID <= 1 {
				return nil
			}

			var err error
			seqBucket, err = tx.CreateTopLevelBucket(
				switchNextPaymentIDKey,
			)
			if err != nil {
				return err
			}
		}

		currentSeq := seqBucket.Sequence()
		if currentSeq == nextID {
			// No synthetic IDs were allocated, so the sequencer is
			// already at the migration cursor.
			return nil
		}
		if currentSeq > nextID {
			// Migration runs exclusively, so the sequencer should
			// not move beyond the cursor computed by migration.
			return fmt.Errorf("switch payment ID sequence above "+
				"migration horizon: current=%d, expected=%d",
				currentSeq, nextID)
		}

		// Synthetic IDs were allocated, so advance the sequencer to
		// the final next-unused ID.
		return seqBucket.SetSequence(nextID)
	}, func() {})
}

// prepareIndexEntry processes a single entry from the payments index bucket,
// parsing the corresponding payment (and any duplicates) into a preparedPayment
// ready for bulk insertion. It returns nil (with a nil error) for entries that
// should be skipped: a missing payment bucket, or a sequence pointer belonging
// to a duplicate payment.
func prepareIndexEntry(seqKey, indexVal []byte, paymentsBucket kvdb.RBucket,
	stats *MigrationStats,
	attemptIDAllocator *attemptIDAllocator) (*preparedPayment, error) {

	r := bytes.NewReader(indexVal)
	paymentHash, err := deserializePaymentIndex(r)
	if err != nil {
		return nil, err
	}

	paymentBucket := paymentsBucket.NestedReadBucket(paymentHash[:])
	if paymentBucket == nil {
		// We skip the entry in case this sequence number does not
		// have a corresponding payment bucket. But aborting would
		// not help either because it is just a db inconsistency.
		log.Warnf("Missing bucket for payment %x", paymentHash[:8])
		stats.SkippedPayments++

		return nil, nil
	}

	// Every payment bucket should have a sequence number which is
	// also important to check for duplicates.
	seqBytes := paymentBucket.Get(paymentSequenceKey)
	if seqBytes == nil {
		return nil, ErrNoSequenceNumber
	}

	// Skip duplicates. They are migrated into the payment_duplicates
	// table when the primary payment is processed.
	if !bytes.Equal(seqBytes, seqKey) {
		return nil, nil
	}

	// Fetch the payment from the kv store.
	payment, err := fetchPayment(paymentBucket)
	if err != nil {
		return nil, fmt.Errorf("fetch payment %x: %w", paymentHash[:8],
			err)
	}

	// Parse the payment into bulk-insertable params.
	prep, err := preparePayment(
		payment, paymentHash, stats, attemptIDAllocator,
	)
	if err != nil {
		return nil, fmt.Errorf("prepare payment %x: %w",
			paymentHash[:8], err)
	}

	// Parse any duplicate payments for this hash so they can be bulk
	// inserted alongside the primary payment.
	dupBucket := paymentBucket.NestedReadBucket(duplicatePaymentsBucket)
	if dupBucket != nil {
		prep.hasDuplicates = true
		duplicates, err := prepareDuplicatePayments(
			dupBucket, paymentHash, stats,
		)
		if err != nil {
			return nil, fmt.Errorf("prepare duplicates %x: %w",
				paymentHash[:8], err)
		}
		prep.duplicates = duplicates
	}

	return prep, nil
}

// flushAndValidateBatch bulk-inserts a whole batch of prepared payments and
// then validates the migrated batch against the source KV data.
func flushAndValidateBatch(ctx context.Context, kvBackend kvdb.Backend,
	sqlDB SQLMigrationQueries, cfg *SQLStoreConfig,
	batch []*preparedPayment) error {

	if err := flushPaymentBatch(
		ctx, sqlDB, cfg.Copier, batch,
	); err != nil {
		return err
	}

	refs := make([]migratedPaymentRef, 0, len(batch))
	for _, p := range batch {
		refs = append(refs, migratedPaymentRef{
			Hash:          p.hash,
			PaymentID:     p.paymentID,
			Payment:       p.payment,
			HasDuplicates: p.hasDuplicates,
		})
	}

	return validateMigratedPaymentBatch(ctx, kvBackend, sqlDB, cfg, refs)
}

// terminalizeUnresolvedLegacyZeroAttempts marks unresolved legacy zero-ID HTLC
// attempts failed and fails the parent payment if no other resolved or
// recoverable in-flight HTLC keeps it active.
//
// Attempt ID zero was written by an old KV migration as an unknown legacy
// value. If such an attempt has no settle/fail resolution, it cannot be safely
// resumed after SQL migration because the live switch state would not know the
// synthetic attempt ID assigned below.
//
// Callers only need this for in-flight payments: any unresolved HTLC makes the
// parent payment in-flight, while terminal historical payments can use the
// regular zero-ID remap path.
func terminalizeUnresolvedLegacyZeroAttempts(payment *MPPayment) (int, error) {
	var (
		terminalizedLegacyAttempts int
		hasSettled                 bool
		hasNonZeroInFlight         bool
	)

	for i := range payment.HTLCs {
		htlc := &payment.HTLCs[i]
		switch {
		case htlc.Settle != nil:
			hasSettled = true

		case htlc.Failure != nil:

		case htlc.AttemptID == 0:
			htlc.Failure = &HTLCFailInfo{
				Reason: HTLCFailUnknown,
			}
			terminalizedLegacyAttempts++

		default:
			hasNonZeroInFlight = true
		}
	}

	if terminalizedLegacyAttempts == 0 {
		return 0, nil
	}

	if !hasSettled && !hasNonZeroInFlight && payment.FailureReason == nil {
		reason := FailureReasonError
		payment.FailureReason = &reason
	}

	if err := payment.setState(); err != nil {
		return 0, err
	}

	return terminalizedLegacyAttempts, nil
}

// prepareDuplicatePayments parses duplicate payments from the KV duplicates
// bucket into bulk-insertable params (payment_id is assigned at flush time).
func prepareDuplicatePayments(dupBucket kvdb.RBucket, hash [32]byte,
	stats *MigrationStats) ([]preparedDuplicate, error) {

	var (
		duplicates     []preparedDuplicate
		duplicateCount int
	)

	err := dupBucket.ForEach(func(seqBytes, _ []byte) error {
		// The duplicates bucket should only contain nested buckets
		// keyed by 8-byte sequence numbers. Skip any unexpected keys
		// (defensive check for corrupted or malformed data).
		if len(seqBytes) != 8 {
			log.Warnf("Skipping unexpected key in duplicates "+
				"bucket for payment %x: key length %d, "+
				"expected 8", hash[:8], len(seqBytes))

			return nil
		}

		seqNum := byteOrder.Uint64(seqBytes)
		subBucket := dupBucket.NestedReadBucket(seqBytes)
		if subBucket == nil {
			return nil
		}

		duplicateCount++
		log.Infof("Migrating duplicate payment seq=%d for "+
			"payment %x", seqNum, hash[:8])

		params, err := parseSingleDuplicatePayment(
			subBucket, hash, seqNum,
		)
		if err != nil {
			return fmt.Errorf("prepare duplicate payment "+
				"seq=%d: %w", seqNum, err)
		}

		duplicates = append(duplicates, preparedDuplicate{
			params: params,
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	if duplicateCount > 0 {
		stats.DuplicatePayments++
		stats.DuplicateEntries += int64(duplicateCount)

		log.Infof("Payment %x had %d duplicate(s) migrated", hash[:8],
			duplicateCount)
	}

	return duplicates, nil
}

// parseSingleDuplicatePayment parses a duplicate payment record for the given
// payment hash into bulk-insertable params (without the primary payment_id,
// which is set at flush time).
func parseSingleDuplicatePayment(dupBucket kvdb.RBucket, hash [32]byte,
	duplicateSeq uint64) (sqlc.InsertPaymentDuplicateMigParams, error) {

	var zero sqlc.InsertPaymentDuplicateMigParams

	creationData := dupBucket.Get(duplicatePaymentCreationInfoKey)
	if creationData == nil {
		return zero, fmt.Errorf("duplicate payment seq=%d missing "+
			"creation info (payment=%x)", duplicateSeq, hash[:8])
	}

	creationInfo, err := deserializeDuplicatePaymentCreationInfo(
		bytes.NewReader(creationData),
	)
	if err != nil {
		return zero, fmt.Errorf("deserialize duplicate creation "+
			"info: %w", err)
	}

	settleData := dupBucket.Get(duplicatePaymentSettleInfoKey)
	failReasonData := dupBucket.Get(duplicatePaymentFailInfoKey)
	attemptData := dupBucket.Get(duplicatePaymentAttemptInfoKey)

	if settleData != nil && len(failReasonData) > 0 {
		return zero, fmt.Errorf("duplicate payment seq=%d has both "+
			"settle and fail info (payment=%x)", duplicateSeq,
			hash[:8])
	}

	var (
		failReason     sql.NullInt32
		settlePreimage []byte
		settleTime     sql.NullTime
	)

	switch {
	case settleData != nil:
		settlePreimage, settleTime, err = parseDuplicateSettleData(
			settleData,
		)
		if err != nil {
			return zero, err
		}

	case len(failReasonData) > 0:
		failReason = sql.NullInt32{
			Int32: int32(failReasonData[0]),
			Valid: true,
		}

	default:
		// If the duplicate payment has no settle or fail info,
		// we mark it as failed during the migration. Duplicate
		// payments were a bug in older versions of LND, so we can be
		// sure if a duplicate payment has no failure reason or
		// settlement data, the corresponding HTLC for this payment
		// has been failed (resolved).
		if attemptData == nil {
			log.Warnf("Duplicate payment seq=%d has no "+
				"attempt info and no resolution (payment=%x); "+
				"marking failed", duplicateSeq, hash[:8])
		} else {
			log.Warnf("Duplicate payment seq=%d has attempt "+
				"info but no resolution (payment=%x); "+
				"marking failed", duplicateSeq, hash[:8])
		}

		failReason = sql.NullInt32{
			Int32: int32(FailureReasonError),
			Valid: true,
		}
	}

	return sqlc.InsertPaymentDuplicateMigParams{
		AmountMsat: int64(creationInfo.Value),
		CreatedAt: normalizeTimeForSQL(
			creationInfo.CreationTime,
		),
		FailReason:     failReason,
		SettlePreimage: settlePreimage,
		SettleTime:     settleTime,
	}, nil
}

// parseDuplicateSettleData extracts settle data from either legacy or modern
// duplicate formats.
func parseDuplicateSettleData(settleData []byte) ([]byte, sql.NullTime, error) {
	if len(settleData) == lntypes.PreimageSize {
		return append([]byte(nil), settleData...), sql.NullTime{}, nil
	}

	settleInfo, err := deserializeHTLCSettleInfo(
		bytes.NewReader(settleData),
	)
	if err != nil {
		return nil, sql.NullTime{},
			fmt.Errorf("deserialize duplicate settle: %w", err)
	}

	settleTime := normalizeTimeForSQL(settleInfo.SettleTime)

	return settleInfo.Preimage[:], sql.NullTime{
		Time:  settleTime,
		Valid: !settleTime.IsZero(),
	}, nil
}

// printMigrationSummary prints a summary of the migration.
func printMigrationSummary(stats *MigrationStats) {
	if stats.TotalPayments == 0 {
		log.Infof("No payments migrated - database is empty")

		return
	}

	log.Infof("========================================")
	log.Infof("   Payment Migration Summary")
	log.Infof("========================================")
	log.Infof("Total Payments:        %d", stats.TotalPayments)
	log.Infof("  Successful:       %d", stats.SuccessfulPayments)
	log.Infof("  Failed:           %d", stats.FailedPayments)
	log.Infof("  In-Flight:        %d", stats.InFlightPayments)
	log.Infof("  Initiated:        %d", stats.InitiatedPayments)
	log.Infof("")
	log.Infof("Total HTLC Attempts:   %d", stats.TotalAttempts)
	log.Infof("  Settled:          %d", stats.SettledAttempts)
	log.Infof("  Failed:           %d", stats.FailedAttempts)
	log.Infof("  In-Flight:        %d", stats.InFlightAttempts)
	log.Infof("")
	log.Infof("Total Route Hops:      %d", stats.TotalHops)

	if stats.SkippedPayments > 0 {
		log.Infof("")
		log.Warnf("SKIPPED PAYMENTS:")
		log.Warnf("  Indexed payments with missing buckets: %d",
			stats.SkippedPayments)
		log.Warnf("  These indicate minor DB inconsistencies.")
	}

	if stats.DuplicatePayments > 0 {
		log.Infof("")
		log.Warnf("DUPLICATE PAYMENTS DETECTED:")
		log.Warnf("  Unique payment hashes with duplicates: %d",
			stats.DuplicatePayments)
		log.Warnf("  Total duplicate entries migrated:      %d",
			stats.DuplicateEntries)
		log.Warnf("  These were caused by an old LND bug.")
	}

	log.Infof("")
	log.Infof("Migration Duration:    %v", stats.MigrationDuration)
	log.Infof("========================================")
}
