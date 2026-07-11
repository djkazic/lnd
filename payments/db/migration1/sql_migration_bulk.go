package migration1

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"

	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/payments/db/migration1/lnwire"
	"github.com/lightningnetwork/lnd/payments/db/migration1/sqlc"
)

// customRecordKV is a pending custom record whose owner ID (payment_id or
// hop_id) is only known once the parent row has been bulk-inserted.
type customRecordKV struct {
	key   int64
	value []byte
}

// preparedHop holds all parsed data for a single route hop. The parent
// hop_id is filled in at flush time after the route hops are bulk-inserted.
type preparedHop struct {
	params  sqlc.InsertRouteHopParams
	blinded *sqlc.InsertRouteHopBlindedParams
	mpp     *sqlc.InsertRouteHopMppParams
	amp     *sqlc.InsertRouteHopAmpParams
	custom  []customRecordKV
}

// preparedAttempt holds all parsed data for a single HTLC attempt. The
// payment_id is filled in at flush time; the attempt_index is already known.
type preparedAttempt struct {
	attemptIndex   int64
	params         sqlc.InsertHtlcAttemptParams
	firstHopCustom []customRecordKV
	hops           []preparedHop
	settle         *sqlc.SettleAttemptParams
	fail           *sqlc.FailAttemptParams
}

// preparedDuplicate holds a parsed duplicate payment record. The payment_id is
// filled in at flush time.
type preparedDuplicate struct {
	params sqlc.InsertPaymentDuplicateMigParams
}

// preparedPayment holds all parsed data for a single primary payment, ready to
// be bulk-inserted. It is produced without issuing any SQL so that a whole
// batch of payments can be flushed together.
type preparedPayment struct {
	hash          lntypes.Hash
	payment       *MPPayment
	hasDuplicates bool

	// paymentID is assigned during flush once the payment row is inserted.
	paymentID int64

	paymentParams  sqlc.InsertPaymentMigParams
	intent         *sqlc.InsertPaymentIntentParams
	firstHopCustom []customRecordKV
	attempts       []preparedAttempt
	duplicates     []preparedDuplicate
}

// preparePayment parses a single KV payment into a preparedPayment without
// issuing any SQL. It mirrors the field-by-field construction previously done
// inline while inserting, and updates the migration stats as it goes.
func preparePayment(payment *MPPayment, hash lntypes.Hash,
	stats *MigrationStats,
	attemptIDAllocator *attemptIDAllocator) (*preparedPayment, error) {

	if payment.Status == StatusInFlight {
		terminalizedLegacyAttempts, err :=
			terminalizeUnresolvedLegacyZeroAttempts(payment)
		if err != nil {
			return nil, err
		}
		if terminalizedLegacyAttempts > 0 {
			log.Warnf("Terminalized %d unresolved legacy HTLC "+
				"attempt(s) with unknown attempt ID zero for "+
				"payment %x; the parent payment was failed if "+
				"no other settled or in-flight HTLC kept "+
				"it active", terminalizedLegacyAttempts,
				hash[:8])
		}
	}

	// Update migration stats based on payment status.
	switch payment.Status {
	case StatusSucceeded:
		stats.SuccessfulPayments++

	case StatusFailed:
		stats.FailedPayments++

	case StatusInFlight:
		stats.InFlightPayments++

	case StatusInitiated:
		stats.InitiatedPayments++
	}

	// Prepare fail reason for SQL insert.
	var failReason sql.NullInt32
	if payment.FailureReason != nil {
		failReason = sql.NullInt32{
			Int32: int32(*payment.FailureReason),
			Valid: true,
		}
	}

	prep := &preparedPayment{
		hash:    hash,
		payment: payment,
		paymentParams: sqlc.InsertPaymentMigParams{
			AmountMsat: int64(payment.Info.Value),
			CreatedAt: normalizeTimeForSQL(
				payment.Info.CreationTime,
			),
			PaymentIdentifier: hash[:],
			FailReason:        failReason,
		},
	}

	// Only include an intent row if we have an actual intent payload. For
	// legacy hash-only/keysend-style payments, the intent may be absent.
	if len(payment.Info.PaymentRequest) > 0 {
		prep.intent = &sqlc.InsertPaymentIntentParams{
			IntentType:    int16(PaymentIntentTypeBolt11),
			IntentPayload: payment.Info.PaymentRequest,
		}
	}

	// First hop custom records (payment level).
	for key, value := range payment.Info.FirstHopCustomRecords {
		prep.firstHopCustom = append(prep.firstHopCustom, customRecordKV{
			key:   int64(key),
			value: value,
		})
	}

	// HTLC attempts.
	for i := range payment.HTLCs {
		htlc := &payment.HTLCs[i]
		attempt, err := prepareHTLCAttempt(
			hash, htlc, stats, attemptIDAllocator,
		)
		if err != nil {
			return nil, fmt.Errorf("prepare attempt %d: %w",
				htlc.AttemptID, err)
		}

		prep.attempts = append(prep.attempts, attempt)
	}

	stats.TotalPayments++

	return prep, nil
}

// prepareHTLCAttempt parses a single HTLC attempt into a preparedAttempt.
func prepareHTLCAttempt(parentPaymentHash lntypes.Hash, htlc *HTLCAttempt,
	stats *MigrationStats,
	attemptIDAllocator *attemptIDAllocator) (preparedAttempt, error) {

	// Determine the payment hash for this HTLC attempt. For AMP payments,
	// each HTLC has its own unique hash. For non-AMP payments all HTLCs use
	// the parent payment hash; older attempts may not store it explicitly,
	// in which case we fall back to the parent payment hash.
	var paymentHash []byte
	switch {
	case htlc.Hash != nil:
		paymentHash = (*htlc.Hash)[:]

	default:
		paymentHash = parentPaymentHash[:]
	}

	firstHopAmountMsat := int64(htlc.Route.FirstHopAmount.Val.Int())

	sessionKey := htlc.SessionKey()
	if sessionKey == nil {
		return preparedAttempt{}, fmt.Errorf("HTLC attempt %d for "+
			"payment %x is missing session key", htlc.AttemptID,
			parentPaymentHash[:8])
	}

	sessionKeyBytes := sessionKey.Serialize()

	attemptID := htlc.AttemptID
	if attemptID == 0 {
		var err error
		attemptID, err = attemptIDAllocator.allocateLegacyAttemptID()
		if err != nil {
			return preparedAttempt{}, fmt.Errorf("allocate legacy "+
				"attempt ID: %w", err)
		}

		log.Warnf("Allocated HTLC attempt index %d from switch "+
			"sequencer for legacy payment %x with unknown "+
			"attempt ID", attemptID, parentPaymentHash[:8])
	}

	if attemptID > math.MaxInt64 {
		return preparedAttempt{}, fmt.Errorf("unable to convert HTLC "+
			"attempt ID to SQL attempt index: attempt_id=%d "+
			"payment=%x max=%d", attemptID, parentPaymentHash[:8],
			uint64(math.MaxInt64))
	}

	attemptIndex := int64(attemptID)

	attempt := preparedAttempt{
		attemptIndex: attemptIndex,
		params: sqlc.InsertHtlcAttemptParams{
			AttemptIndex:       attemptIndex,
			SessionKey:         sessionKeyBytes,
			AttemptTime:        normalizeTimeForSQL(htlc.AttemptTime),
			PaymentHash:        paymentHash,
			FirstHopAmountMsat: firstHopAmountMsat,
			RouteTotalTimeLock: int32(htlc.Route.TotalTimeLock),
			RouteTotalAmount:   int64(htlc.Route.TotalAmount),
			RouteSourceKey:     htlc.Route.SourcePubKey[:],
		},
	}

	// Route-level first hop custom records.
	for key, value := range htlc.Route.FirstHopWireCustomRecords {
		attempt.firstHopCustom = append(
			attempt.firstHopCustom, customRecordKV{
				key:   int64(key),
				value: value,
			},
		)
	}

	// Route hops.
	for hopIndex := range htlc.Route.Hops {
		hop := htlc.Route.Hops[hopIndex]
		prepHop, err := prepareRouteHop(attemptIndex, hopIndex, hop)
		if err != nil {
			return preparedAttempt{}, fmt.Errorf(
				"prepare hop %d: %w", hopIndex, err,
			)
		}

		attempt.hops = append(attempt.hops, prepHop)
		stats.TotalHops++
	}

	// Resolution (settle or fail).
	switch {
	case htlc.Settle != nil:
		attempt.settle = &sqlc.SettleAttemptParams{
			AttemptIndex: attemptIndex,
			ResolutionTime: normalizeTimeForSQL(
				htlc.Settle.SettleTime,
			),
			ResolutionType: int32(HTLCAttemptResolutionSettled),
			SettlePreimage: htlc.Settle.Preimage[:],
		}

		stats.SettledAttempts++

	case htlc.Failure != nil:
		var failureMsg bytes.Buffer
		if htlc.Failure.Message != nil {
			err := lnwire.EncodeFailureMessage(
				&failureMsg, htlc.Failure.Message, 0,
			)
			if err != nil {
				return preparedAttempt{}, fmt.Errorf("failed "+
					"to encode failure message: %w", err)
			}
		}

		attempt.fail = &sqlc.FailAttemptParams{
			AttemptIndex: attemptIndex,
			ResolutionTime: normalizeTimeForSQL(
				htlc.Failure.FailTime,
			),
			ResolutionType: int32(HTLCAttemptResolutionFailed),
			FailureSourceIndex: sql.NullInt32{
				Int32: int32(htlc.Failure.FailureSourceIndex),
				Valid: true,
			},
			HtlcFailReason: sql.NullInt32{
				Int32: int32(htlc.Failure.Reason),
				Valid: true,
			},
			FailureMsg: failureMsg.Bytes(),
		}

		stats.FailedAttempts++

	default:
		// If the attempt is not settled or failed, it is in flight.
		stats.InFlightAttempts++
	}

	stats.TotalAttempts++

	return attempt, nil
}

// prepareRouteHop parses a single route hop into a preparedHop.
func prepareRouteHop(attemptIndex int64, hopIndex int,
	hop *Hop) (preparedHop, error) {

	// Convert channel ID to string representation of uint64. The SCID is
	// stored as a decimal string to match the converter expectations
	// (sql_converters.go:173).
	scidStr := strconv.FormatUint(hop.ChannelID, 10)

	prepHop := preparedHop{
		params: sqlc.InsertRouteHopParams{
			HtlcAttemptIndex: attemptIndex,
			HopIndex:         int32(hopIndex),
			PubKey:           hop.PubKeyBytes[:],
			Scid:             scidStr,
			OutgoingTimeLock: int32(hop.OutgoingTimeLock),
			AmtToForward:     int64(hop.AmtToForward),
			MetaData:         hop.Metadata,
		},
	}

	// Blinded route data (route blinding).
	if len(hop.EncryptedData) > 0 || hop.BlindingPoint != nil ||
		hop.TotalAmtMsat != 0 {

		var blindingPoint []byte
		if hop.BlindingPoint != nil {
			blindingPoint = hop.BlindingPoint.SerializeCompressed()
		}

		var totalAmt sql.NullInt64
		if hop.TotalAmtMsat != 0 {
			totalAmt = sql.NullInt64{
				Int64: int64(hop.TotalAmtMsat),
				Valid: true,
			}
		}

		prepHop.blinded = &sqlc.InsertRouteHopBlindedParams{
			EncryptedData:       hop.EncryptedData,
			BlindingPoint:       blindingPoint,
			BlindedPathTotalAmt: totalAmt,
		}
	}

	// MPP record.
	if hop.MPP != nil {
		paymentAddr := hop.MPP.PaymentAddr()
		prepHop.mpp = &sqlc.InsertRouteHopMppParams{
			PaymentAddr: paymentAddr[:],
			TotalMsat:   int64(hop.MPP.TotalMsat()),
		}
	}

	// AMP record.
	if hop.AMP != nil {
		rootShare := hop.AMP.RootShare()
		setID := hop.AMP.SetID()
		prepHop.amp = &sqlc.InsertRouteHopAmpParams{
			RootShare:  rootShare[:],
			SetID:      setID[:],
			ChildIndex: int32(hop.AMP.ChildIndex()),
		}
	}

	// Custom records.
	if hop.CustomRecords != nil {
		for tlvType, value := range hop.CustomRecords {
			prepHop.custom = append(prepHop.custom, customRecordKV{
				key:   int64(tlvType),
				value: value,
			})
		}
	}

	return prepHop, nil
}

// htlcAttemptCols is the column order used to bulk-load HTLC attempts. It must
// stay in sync with the value order produced in insertHtlcAttempts and with the
// sqlc BulkInsertHtlcAttempts column list.
var htlcAttemptCols = []string{
	"payment_id", "attempt_index", "session_key", "attempt_time",
	"payment_hash", "first_hop_amount_msat", "route_total_time_lock",
	"route_total_amount", "route_source_key",
}

// insertHtlcAttempts writes the batch's HTLC attempts using the COPY protocol
// when a copier is available (Postgres), otherwise falling back to chunked
// multi-row INSERT (SQLite / non-pgx). Attempts carry a client-assigned
// attempt_index and their generated id is not used downstream, so no RETURNING
// is required and COPY is a clean fit.
func insertHtlcAttempts(ctx context.Context, sqlDB SQLMigrationQueries,
	copier BulkCopier, attempts []sqlc.InsertHtlcAttemptParams) error {

	if len(attempts) == 0 {
		return nil
	}

	if copier == nil {
		return sqlDB.BulkInsertHtlcAttempts(ctx, attempts)
	}

	rows := make([][]any, len(attempts))
	for i := range attempts {
		a := &attempts[i]
		rows[i] = []any{
			a.PaymentID, a.AttemptIndex, a.SessionKey, a.AttemptTime,
			a.PaymentHash, a.FirstHopAmountMsat,
			a.RouteTotalTimeLock, a.RouteTotalAmount,
			a.RouteSourceKey,
		}
	}

	_, err := copier.CopyInto(
		ctx, "payment_htlc_attempts", htlcAttemptCols, rows,
	)

	return err
}

// flushPaymentBatch bulk-inserts an entire batch of prepared payments using a
// small, fixed number of chunked multi-row INSERT (or COPY) statements, and
// assigns the generated payment IDs back onto the prepared payments.
func flushPaymentBatch(ctx context.Context, sqlDB SQLMigrationQueries,
	copier BulkCopier, batch []*preparedPayment) error {

	if len(batch) == 0 {
		return nil
	}

	// Stage 1: insert the payments and learn their generated IDs.
	paymentParams := make([]sqlc.InsertPaymentMigParams, 0, len(batch))
	for _, p := range batch {
		paymentParams = append(paymentParams, p.paymentParams)
	}

	ids, err := sqlDB.BulkInsertPaymentsMig(ctx, paymentParams)
	if err != nil {
		return fmt.Errorf("bulk insert payments: %w", err)
	}
	if len(ids) != len(batch) {
		return fmt.Errorf("payment bulk insert count mismatch: "+
			"got=%d want=%d", len(ids), len(batch))
	}

	// Map payment identifier -> generated ID. Payment identifiers are unique
	// within a batch (duplicates are migrated separately), so this pairs
	// results back independent of RETURNING row order.
	idByIdent := make(map[string]int64, len(ids))
	for _, row := range ids {
		idByIdent[string(row.PaymentIdentifier)] = row.ID
	}

	for _, p := range batch {
		id, ok := idByIdent[string(p.hash[:])]
		if !ok {
			return fmt.Errorf("missing inserted ID for payment %x",
				p.hash[:8])
		}
		p.paymentID = id
	}

	// Stage 2: intents, payment-level first hop custom records, and HTLC
	// attempts (all reference payment_id).
	var (
		intents        []sqlc.InsertPaymentIntentParams
		payFirstHopRec []sqlc.InsertPaymentFirstHopCustomRecordParams
		attempts       []sqlc.InsertHtlcAttemptParams
		attemptFHRec   []sqlc.InsertPaymentAttemptFirstHopCustomRecordParams
		settles        []sqlc.SettleAttemptParams
		fails          []sqlc.FailAttemptParams
		hops           []sqlc.InsertRouteHopParams
		duplicates     []sqlc.InsertPaymentDuplicateMigParams
	)

	for _, p := range batch {
		if p.intent != nil {
			intent := *p.intent
			intent.PaymentID = p.paymentID
			intents = append(intents, intent)
		}

		for _, rec := range p.firstHopCustom {
			payFirstHopRec = append(
				payFirstHopRec,
				sqlc.InsertPaymentFirstHopCustomRecordParams{
					PaymentID: p.paymentID,
					Key:       rec.key,
					Value:     rec.value,
				},
			)
		}

		for i := range p.attempts {
			a := &p.attempts[i]
			a.params.PaymentID = p.paymentID
			attempts = append(attempts, a.params)

			for _, rec := range a.firstHopCustom {
				attemptFHRec = append(
					attemptFHRec,
					//nolint:ll
					sqlc.InsertPaymentAttemptFirstHopCustomRecordParams{
						HtlcAttemptIndex: a.attemptIndex,
						Key:              rec.key,
						Value:            rec.value,
					},
				)
			}

			if a.settle != nil {
				settles = append(settles, *a.settle)
			}
			if a.fail != nil {
				fails = append(fails, *a.fail)
			}

			for j := range a.hops {
				hops = append(hops, a.hops[j].params)
			}
		}

		for _, dup := range p.duplicates {
			d := dup.params
			d.PaymentID = p.paymentID
			duplicates = append(duplicates, d)
		}
	}

	if err := sqlDB.BulkInsertPaymentIntents(ctx, intents); err != nil {
		return fmt.Errorf("bulk insert intents: %w", err)
	}
	if err := sqlDB.BulkInsertPaymentFirstHopCustomRecords(
		ctx, payFirstHopRec,
	); err != nil {
		return fmt.Errorf("bulk insert payment custom records: %w", err)
	}
	if err := insertHtlcAttempts(
		ctx, sqlDB, copier, attempts,
	); err != nil {
		return fmt.Errorf("insert attempts: %w", err)
	}
	if err := sqlDB.BulkInsertPaymentAttemptFirstHopCustomRecords(
		ctx, attemptFHRec,
	); err != nil {
		return fmt.Errorf("bulk insert attempt custom records: %w", err)
	}

	// Stage 3: insert route hops and learn their generated IDs, keyed by
	// (attempt_index, hop_index).
	hopIDs, err := sqlDB.BulkInsertRouteHops(ctx, hops)
	if err != nil {
		return fmt.Errorf("bulk insert route hops: %w", err)
	}
	if len(hopIDs) != len(hops) {
		return fmt.Errorf("route hop bulk insert count mismatch: "+
			"got=%d want=%d", len(hopIDs), len(hops))
	}

	type hopKey struct {
		attemptIndex int64
		hopIndex     int32
	}
	hopIDByKey := make(map[hopKey]int64, len(hopIDs))
	for _, row := range hopIDs {
		hopIDByKey[hopKey{row.HtlcAttemptIndex, row.HopIndex}] = row.ID
	}

	// Stage 4: hop children (blinded/mpp/amp/custom) and attempt
	// resolutions.
	var (
		blinded   []sqlc.InsertRouteHopBlindedParams
		mpps      []sqlc.InsertRouteHopMppParams
		amps      []sqlc.InsertRouteHopAmpParams
		hopCustom []sqlc.InsertPaymentHopCustomRecordParams
	)

	for _, p := range batch {
		for i := range p.attempts {
			a := &p.attempts[i]
			for j := range a.hops {
				h := &a.hops[j]
				key := hopKey{
					a.attemptIndex, h.params.HopIndex,
				}
				hopID, ok := hopIDByKey[key]
				if !ok {
					return fmt.Errorf("missing inserted ID "+
						"for hop (attempt=%d index=%d)",
						a.attemptIndex, h.params.HopIndex)
				}

				if h.blinded != nil {
					b := *h.blinded
					b.HopID = hopID
					blinded = append(blinded, b)
				}
				if h.mpp != nil {
					m := *h.mpp
					m.HopID = hopID
					mpps = append(mpps, m)
				}
				if h.amp != nil {
					am := *h.amp
					am.HopID = hopID
					amps = append(amps, am)
				}
				for _, rec := range h.custom {
					hopCustom = append(
						hopCustom,
						//nolint:ll
						sqlc.InsertPaymentHopCustomRecordParams{
							HopID: hopID,
							Key:   rec.key,
							Value: rec.value,
						},
					)
				}
			}
		}
	}

	if err := sqlDB.BulkInsertRouteHopBlinded(ctx, blinded); err != nil {
		return fmt.Errorf("bulk insert blinded hops: %w", err)
	}
	if err := sqlDB.BulkInsertRouteHopMpp(ctx, mpps); err != nil {
		return fmt.Errorf("bulk insert mpp hops: %w", err)
	}
	if err := sqlDB.BulkInsertRouteHopAmp(ctx, amps); err != nil {
		return fmt.Errorf("bulk insert amp hops: %w", err)
	}
	if err := sqlDB.BulkInsertPaymentHopCustomRecords(
		ctx, hopCustom,
	); err != nil {
		return fmt.Errorf("bulk insert hop custom records: %w", err)
	}

	if err := sqlDB.BulkSettleAttempts(ctx, settles); err != nil {
		return fmt.Errorf("bulk settle attempts: %w", err)
	}
	if err := sqlDB.BulkFailAttempts(ctx, fails); err != nil {
		return fmt.Errorf("bulk fail attempts: %w", err)
	}

	// Stage 5: duplicate payments (rare).
	if err := sqlDB.BulkInsertPaymentDuplicatesMig(
		ctx, duplicates,
	); err != nil {
		return fmt.Errorf("bulk insert duplicates: %w", err)
	}

	return nil
}
