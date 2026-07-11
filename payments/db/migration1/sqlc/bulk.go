package sqlc

import (
	"context"
	"fmt"
	"strings"
)

// maxBulkParams is the largest number of SQL placeholder parameters we put in a
// single multi-row INSERT statement. It is the smaller of the SQLite (32766)
// and Postgres (65535) per-statement variable limits, so chunking by this
// budget is safe on both backends.
const maxBulkParams = 32766

// buildBulkInsert builds an "INSERT INTO <table> (<cols>) VALUES (..),(..)"
// statement with $N placeholders for rowCount rows of len(cols) columns each,
// optionally followed by "RETURNING <returning>".
func buildBulkInsert(table string, cols []string, rowCount int,
	returning string) string {

	colCount := len(cols)

	var b strings.Builder
	b.Grow(len(table) + rowCount*colCount*6 + 64)

	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ", "))
	b.WriteString(") VALUES ")

	n := 1
	for r := 0; r < rowCount; r++ {
		if r > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('(')
		for c := 0; c < colCount; c++ {
			if c > 0 {
				b.WriteByte(',')
			}
			_, _ = fmt.Fprintf(&b, "$%d", n)
			n++
		}
		b.WriteByte(')')
	}

	if returning != "" {
		b.WriteString(" RETURNING ")
		b.WriteString(returning)
	}

	return b.String()
}

// rowsPerChunk returns how many rows of the given column count fit within the
// parameter budget of a single statement.
func rowsPerChunk(colCount int) int {
	n := maxBulkParams / colCount
	if n == 0 {
		return 1
	}

	return n
}

// bulkExec runs chunked multi-row INSERT statements (no RETURNING). rowArgs
// must contain len(cols) values per row, laid out row-major.
func (q *Queries) bulkExec(ctx context.Context, table string, cols []string,
	rowArgs []interface{}) error {

	colCount := len(cols)
	if colCount == 0 || len(rowArgs) == 0 {
		return nil
	}

	total := len(rowArgs) / colCount
	perChunk := rowsPerChunk(colCount)

	for start := 0; start < total; start += perChunk {
		end := start + perChunk
		if end > total {
			end = total
		}

		query := buildBulkInsert(table, cols, end-start, "")
		args := rowArgs[start*colCount : end*colCount]
		if _, err := q.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("bulk insert into %s (%d rows): %w",
				table, end-start, err)
		}
	}

	return nil
}

// BulkPaymentIDMig pairs an inserted payment's generated ID with its (unique)
// payment identifier so callers can map results back regardless of the order
// the database returns RETURNING rows in.
type BulkPaymentIDMig struct {
	ID                int64
	PaymentIdentifier []byte
}

// BulkInsertPaymentsMig bulk-inserts payments and returns their generated IDs
// paired with their payment identifiers.
func (q *Queries) BulkInsertPaymentsMig(ctx context.Context,
	params []InsertPaymentMigParams) ([]BulkPaymentIDMig, error) {

	cols := []string{
		"amount_msat", "created_at", "payment_identifier", "fail_reason",
	}
	colCount := len(cols)
	total := len(params)
	if total == 0 {
		return nil, nil
	}

	perChunk := rowsPerChunk(colCount)
	out := make([]BulkPaymentIDMig, 0, total)

	for start := 0; start < total; start += perChunk {
		end := start + perChunk
		if end > total {
			end = total
		}
		chunk := params[start:end]

		args := make([]interface{}, 0, len(chunk)*colCount)
		for _, p := range chunk {
			args = append(args, p.AmountMsat, p.CreatedAt,
				p.PaymentIdentifier, p.FailReason)
		}

		query := buildBulkInsert(
			"payments", cols, len(chunk),
			"id, payment_identifier",
		)
		rows, err := q.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("bulk insert payments "+
				"(%d rows): %w", len(chunk), err)
		}

		for rows.Next() {
			var r BulkPaymentIDMig
			if err := rows.Scan(
				&r.ID, &r.PaymentIdentifier,
			); err != nil {
				_ = rows.Close()
				return nil, err
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}

	return out, nil
}

// BulkInsertPaymentIntents bulk-inserts payment intents.
func (q *Queries) BulkInsertPaymentIntents(ctx context.Context,
	params []InsertPaymentIntentParams) error {

	cols := []string{"payment_id", "intent_type", "intent_payload"}
	args := make([]interface{}, 0, len(params)*len(cols))
	for _, p := range params {
		args = append(args, p.PaymentID, p.IntentType, p.IntentPayload)
	}

	return q.bulkExec(ctx, "payment_intents", cols, args)
}

// BulkInsertPaymentFirstHopCustomRecords bulk-inserts payment-level first hop
// custom records.
func (q *Queries) BulkInsertPaymentFirstHopCustomRecords(ctx context.Context,
	params []InsertPaymentFirstHopCustomRecordParams) error {

	cols := []string{"payment_id", "key", "value"}
	args := make([]interface{}, 0, len(params)*len(cols))
	for _, p := range params {
		args = append(args, p.PaymentID, p.Key, p.Value)
	}

	return q.bulkExec(
		ctx, "payment_first_hop_custom_records", cols, args,
	)
}

// BulkInsertHtlcAttempts bulk-inserts HTLC attempts.
func (q *Queries) BulkInsertHtlcAttempts(ctx context.Context,
	params []InsertHtlcAttemptParams) error {

	cols := []string{
		"payment_id", "attempt_index", "session_key", "attempt_time",
		"payment_hash", "first_hop_amount_msat", "route_total_time_lock",
		"route_total_amount", "route_source_key",
	}
	args := make([]interface{}, 0, len(params)*len(cols))
	for _, p := range params {
		args = append(args, p.PaymentID, p.AttemptIndex, p.SessionKey,
			p.AttemptTime, p.PaymentHash, p.FirstHopAmountMsat,
			p.RouteTotalTimeLock, p.RouteTotalAmount,
			p.RouteSourceKey)
	}

	return q.bulkExec(ctx, "payment_htlc_attempts", cols, args)
}

// BulkInsertPaymentAttemptFirstHopCustomRecords bulk-inserts route-level first
// hop custom records.
func (q *Queries) BulkInsertPaymentAttemptFirstHopCustomRecords(
	ctx context.Context,
	params []InsertPaymentAttemptFirstHopCustomRecordParams) error {

	cols := []string{"htlc_attempt_index", "key", "value"}
	args := make([]interface{}, 0, len(params)*len(cols))
	for _, p := range params {
		args = append(args, p.HtlcAttemptIndex, p.Key, p.Value)
	}

	return q.bulkExec(
		ctx, "payment_attempt_first_hop_custom_records", cols, args,
	)
}

// BulkHopID pairs an inserted route hop's generated ID with its natural key
// (htlc_attempt_index, hop_index) so callers can map results back regardless
// of RETURNING row order.
type BulkHopID struct {
	ID               int64
	HtlcAttemptIndex int64
	HopIndex         int32
}

// BulkInsertRouteHops bulk-inserts route hops and returns their generated IDs
// paired with their (htlc_attempt_index, hop_index) natural keys.
func (q *Queries) BulkInsertRouteHops(ctx context.Context,
	params []InsertRouteHopParams) ([]BulkHopID, error) {

	cols := []string{
		"htlc_attempt_index", "hop_index", "pub_key", "scid",
		"outgoing_time_lock", "amt_to_forward", "meta_data",
	}
	colCount := len(cols)
	total := len(params)
	if total == 0 {
		return nil, nil
	}

	perChunk := rowsPerChunk(colCount)
	out := make([]BulkHopID, 0, total)

	for start := 0; start < total; start += perChunk {
		end := start + perChunk
		if end > total {
			end = total
		}
		chunk := params[start:end]

		args := make([]interface{}, 0, len(chunk)*colCount)
		for _, p := range chunk {
			args = append(args, p.HtlcAttemptIndex, p.HopIndex,
				p.PubKey, p.Scid, p.OutgoingTimeLock,
				p.AmtToForward, p.MetaData)
		}

		query := buildBulkInsert(
			"payment_route_hops", cols, len(chunk),
			"id, htlc_attempt_index, hop_index",
		)
		rows, err := q.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("bulk insert route hops "+
				"(%d rows): %w", len(chunk), err)
		}

		for rows.Next() {
			var r BulkHopID
			if err := rows.Scan(
				&r.ID, &r.HtlcAttemptIndex, &r.HopIndex,
			); err != nil {
				_ = rows.Close()
				return nil, err
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}

	return out, nil
}

// BulkInsertRouteHopBlinded bulk-inserts blinded route hop records.
func (q *Queries) BulkInsertRouteHopBlinded(ctx context.Context,
	params []InsertRouteHopBlindedParams) error {

	cols := []string{
		"hop_id", "encrypted_data", "blinding_point",
		"blinded_path_total_amt",
	}
	args := make([]interface{}, 0, len(params)*len(cols))
	for _, p := range params {
		args = append(args, p.HopID, p.EncryptedData, p.BlindingPoint,
			p.BlindedPathTotalAmt)
	}

	return q.bulkExec(ctx, "payment_route_hop_blinded", cols, args)
}

// BulkInsertRouteHopMpp bulk-inserts MPP route hop records.
func (q *Queries) BulkInsertRouteHopMpp(ctx context.Context,
	params []InsertRouteHopMppParams) error {

	cols := []string{"hop_id", "payment_addr", "total_msat"}
	args := make([]interface{}, 0, len(params)*len(cols))
	for _, p := range params {
		args = append(args, p.HopID, p.PaymentAddr, p.TotalMsat)
	}

	return q.bulkExec(ctx, "payment_route_hop_mpp", cols, args)
}

// BulkInsertRouteHopAmp bulk-inserts AMP route hop records.
func (q *Queries) BulkInsertRouteHopAmp(ctx context.Context,
	params []InsertRouteHopAmpParams) error {

	cols := []string{"hop_id", "root_share", "set_id", "child_index"}
	args := make([]interface{}, 0, len(params)*len(cols))
	for _, p := range params {
		args = append(args, p.HopID, p.RootShare, p.SetID, p.ChildIndex)
	}

	return q.bulkExec(ctx, "payment_route_hop_amp", cols, args)
}

// BulkInsertPaymentHopCustomRecords bulk-inserts hop-level custom records.
func (q *Queries) BulkInsertPaymentHopCustomRecords(ctx context.Context,
	params []InsertPaymentHopCustomRecordParams) error {

	cols := []string{"hop_id", "key", "value"}
	args := make([]interface{}, 0, len(params)*len(cols))
	for _, p := range params {
		args = append(args, p.HopID, p.Key, p.Value)
	}

	return q.bulkExec(ctx, "payment_hop_custom_records", cols, args)
}

// BulkSettleAttempts bulk-inserts settle resolutions.
func (q *Queries) BulkSettleAttempts(ctx context.Context,
	params []SettleAttemptParams) error {

	cols := []string{
		"attempt_index", "resolution_time", "resolution_type",
		"settle_preimage",
	}
	args := make([]interface{}, 0, len(params)*len(cols))
	for _, p := range params {
		args = append(args, p.AttemptIndex, p.ResolutionTime,
			p.ResolutionType, p.SettlePreimage)
	}

	return q.bulkExec(
		ctx, "payment_htlc_attempt_resolutions", cols, args,
	)
}

// BulkFailAttempts bulk-inserts fail resolutions.
func (q *Queries) BulkFailAttempts(ctx context.Context,
	params []FailAttemptParams) error {

	cols := []string{
		"attempt_index", "resolution_time", "resolution_type",
		"failure_source_index", "htlc_fail_reason", "failure_msg",
	}
	args := make([]interface{}, 0, len(params)*len(cols))
	for _, p := range params {
		args = append(args, p.AttemptIndex, p.ResolutionTime,
			p.ResolutionType, p.FailureSourceIndex,
			p.HtlcFailReason, p.FailureMsg)
	}

	return q.bulkExec(
		ctx, "payment_htlc_attempt_resolutions", cols, args,
	)
}

// BulkInsertPaymentDuplicatesMig bulk-inserts duplicate payment records.
func (q *Queries) BulkInsertPaymentDuplicatesMig(ctx context.Context,
	params []InsertPaymentDuplicateMigParams) error {

	cols := []string{
		"payment_id", "amount_msat", "created_at", "fail_reason",
		"settle_preimage", "settle_time",
	}
	args := make([]interface{}, 0, len(params)*len(cols))
	for _, p := range params {
		args = append(args, p.PaymentID, p.AmountMsat, p.CreatedAt,
			p.FailReason, p.SettlePreimage, p.SettleTime)
	}

	return q.bulkExec(ctx, "payment_duplicates", cols, args)
}
