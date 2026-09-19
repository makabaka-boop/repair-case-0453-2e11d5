package store_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"batchseal/internal/store"

	"github.com/jackc/pgx/v5"
)

// These tests deterministically reproduce the cross-instance race in which
// one API instance commits while the other's transaction is already queued
// on the batch row lock. Once the lock is granted, the waiting transaction
// must adjudicate against everything the lock holder committed — a verdict
// based on a pre-lock snapshot turns retransmissions into duplicate-key
// errors (HTTP 500) and complete batches into spurious 409 INCOMPLETE.

// lockTestStore opens a store on its own isolated schema and returns the
// schema name so the test can drive a second connection as a rival instance.
func lockTestStore(ctx context.Context, t *testing.T) (*store.Store, string) {
	t.Helper()
	if !ensureBasePool() {
		t.Skipf("real PostgreSQL not available at %q: %v", testURL(), basePoolErr)
	}
	schema := fmt.Sprintf("t_%d", time.Now().UnixNano())
	if _, err := basePool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = basePool.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
	})
	s, err := store.New(ctx, schemaURL(schema))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)
	return s, schema
}

// rivalLock takes the batch row lock on a dedicated connection, modelling a
// second API instance mid-transaction. The returned transaction holds the
// lock until the caller commits (or the test cleans up with a rollback).
func rivalLock(ctx context.Context, t *testing.T, schema, batchID string) pgx.Tx {
	t.Helper()
	conn, err := pgx.Connect(ctx, schemaURL(schema))
	if err != nil {
		t.Fatalf("connect rival: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin rival tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	var expected int
	var status string
	if err := tx.QueryRow(ctx,
		`SELECT expected_chunks, status FROM batches WHERE id = $1 FOR UPDATE`,
		batchID,
	).Scan(&expected, &status); err != nil {
		t.Fatalf("rival lock batch row: %v", err)
	}
	return tx
}

// waitForLockWaiter blocks until the store under test has a backend queued
// on a PostgreSQL lock — proof that its transaction issued SELECT … FOR
// UPDATE and is now waiting behind the rival's row lock, so whatever the
// rival commits next happens strictly after the waiter's transaction began.
func waitForLockWaiter(ctx context.Context, t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := basePool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND query LIKE '%FOR UPDATE%'
			  AND pid <> pg_backend_pid()`).Scan(&n); err != nil {
			t.Fatalf("poll pg_stat_activity: %v", err)
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the store transaction to queue on the batch row lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type submitOutcome struct {
	res store.SubmitResult
	err error
}

// TestSubmitSeesChunkCommittedWhileWaitingForLock: instance A holds the
// batch lock and commits a chunk while instance B's identical submit is
// queued on the lock. Once B acquires the lock it must answer with the
// original acknowledgement — not misread the committed chunk as missing and
// die on a duplicate-key error (HTTP 500 for the caller).
func TestSubmitSeesChunkCommittedWhileWaitingForLock(t *testing.T) {
	ctx := context.Background()
	s, schema := lockTestStore(ctx, t)
	b := mustBatch(ctx, t, s, 1)

	rival := rivalLock(ctx, t, schema, b.ID)

	done := make(chan submitOutcome, 1)
	go func() {
		res, err := s.SubmitChunk(ctx, b.ID, 1, []byte("gamma"))
		done <- submitOutcome{res, err}
	}()
	waitForLockWaiter(ctx, t)

	// The lock holder commits the same content the waiter is retransmitting.
	var receivedAt time.Time
	if err := rival.QueryRow(ctx,
		`INSERT INTO chunks (batch_id, seq, payload) VALUES ($1, 1, $2)
		 RETURNING received_at`,
		b.ID, []byte("gamma"),
	).Scan(&receivedAt); err != nil {
		t.Fatalf("rival insert chunk: %v", err)
	}
	if err := rival.Commit(ctx); err != nil {
		t.Fatalf("rival commit: %v", err)
	}

	out := <-done
	if out.err != nil {
		t.Fatalf("queued retransmission must return the original ack, got error: %v", out.err)
	}
	if out.res.Created {
		t.Fatalf("queued retransmission must report the stored write, not a new one: %+v", out.res)
	}
	if !out.res.ReceivedAt.Equal(receivedAt) {
		t.Fatalf("ack time changed: got %v, original %v", out.res.ReceivedAt, receivedAt)
	}
	if out.res.Size != len("gamma") {
		t.Fatalf("ack size wrong: %+v", out.res)
	}
}

// TestSubmitConflictCommittedWhileWaitingForLock is the same race with
// different bytes: the queued submit must be adjudicated CHUNK_CONFLICT
// (HTTP 409), not a duplicate-key 500.
func TestSubmitConflictCommittedWhileWaitingForLock(t *testing.T) {
	ctx := context.Background()
	s, schema := lockTestStore(ctx, t)
	b := mustBatch(ctx, t, s, 1)

	rival := rivalLock(ctx, t, schema, b.ID)

	done := make(chan submitOutcome, 1)
	go func() {
		res, err := s.SubmitChunk(ctx, b.ID, 1, []byte("omega"))
		done <- submitOutcome{res, err}
	}()
	waitForLockWaiter(ctx, t)

	if _, err := rival.Exec(ctx,
		`INSERT INTO chunks (batch_id, seq, payload) VALUES ($1, 1, $2)`,
		b.ID, []byte("alpha")); err != nil {
		t.Fatalf("rival insert chunk: %v", err)
	}
	if err := rival.Commit(ctx); err != nil {
		t.Fatalf("rival commit: %v", err)
	}

	out := <-done
	if !errors.Is(out.err, store.ErrConflict) {
		t.Fatalf("want ErrConflict, got res=%+v err=%v", out.res, out.err)
	}
}

// TestSealSeesFinalChunkCommittedWhileWaitingForLock: with expectedChunks=1
// the final chunk commits while the seal request is queued on the batch
// lock. Once the seal acquires the lock it must see the complete set and
// seal — not answer 409 INCOMPLETE with received=0, gaps=[1] and leave the
// batch OPEN.
func TestSealSeesFinalChunkCommittedWhileWaitingForLock(t *testing.T) {
	ctx := context.Background()
	s, schema := lockTestStore(ctx, t)
	b := mustBatch(ctx, t, s, 1)

	rival := rivalLock(ctx, t, schema, b.ID)

	type sealOutcome struct {
		snap *store.Snapshot
		err  error
	}
	done := make(chan sealOutcome, 1)
	go func() {
		snap, err := s.SealBatch(ctx, b.ID)
		done <- sealOutcome{snap, err}
	}()
	waitForLockWaiter(ctx, t)

	if _, err := rival.Exec(ctx,
		`INSERT INTO chunks (batch_id, seq, payload) VALUES ($1, 1, $2)`,
		b.ID, []byte("final")); err != nil {
		t.Fatalf("rival insert final chunk: %v", err)
	}
	if err := rival.Commit(ctx); err != nil {
		t.Fatalf("rival commit: %v", err)
	}

	out := <-done
	if out.err != nil {
		t.Fatalf("seal after the final chunk must succeed, got %v (snap=%+v)", out.err, out.snap)
	}
	if out.snap.Status != store.StatusSealed || out.snap.SealedAt == nil {
		t.Fatalf("batch not sealed: %+v", out.snap)
	}
	if out.snap.Received != 1 || len(out.snap.Gaps) != 0 {
		t.Fatalf("sealed snapshot inconsistent: %+v", out.snap)
	}
}

// TestSubmitSeesSealCommittedWhileWaitingForLock is the reverse ordering:
// the lock holder writes the chunk and seals the batch while the submit is
// queued. After acquiring the lock the submit must observe the sealed state:
// an identical retransmission returns the original ack, changed content is
// rejected BATCH_SEALED.
func TestSubmitSeesSealCommittedWhileWaitingForLock(t *testing.T) {
	ctx := context.Background()
	s, schema := lockTestStore(ctx, t)

	for _, tc := range []struct {
		name    string
		sent    string
		wantErr error
	}{
		{"identical retransmission returns original ack", "held", nil},
		{"changed content rejected BATCH_SEALED", "changed", store.ErrSealed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := mustBatch(ctx, t, s, 1)
			rival := rivalLock(ctx, t, schema, b.ID)

			done := make(chan submitOutcome, 1)
			go func() {
				res, err := s.SubmitChunk(ctx, b.ID, 1, []byte(tc.sent))
				done <- submitOutcome{res, err}
			}()
			waitForLockWaiter(ctx, t)

			var receivedAt time.Time
			if err := rival.QueryRow(ctx,
				`INSERT INTO chunks (batch_id, seq, payload) VALUES ($1, 1, $2)
				 RETURNING received_at`,
				b.ID, []byte("held")).Scan(&receivedAt); err != nil {
				t.Fatalf("rival insert chunk: %v", err)
			}
			if _, err := rival.Exec(ctx,
				`UPDATE batches SET status = 'SEALED', sealed_at = now() WHERE id = $1`,
				b.ID); err != nil {
				t.Fatalf("rival seal: %v", err)
			}
			if err := rival.Commit(ctx); err != nil {
				t.Fatalf("rival commit: %v", err)
			}

			out := <-done
			if tc.wantErr != nil {
				if !errors.Is(out.err, tc.wantErr) {
					t.Fatalf("want %v, got res=%+v err=%v", tc.wantErr, out.res, out.err)
				}
				return
			}
			if out.err != nil || out.res.Created || !out.res.ReceivedAt.Equal(receivedAt) {
				t.Fatalf("identical retransmission after seal wrong: res=%+v err=%v want receivedAt=%v",
					out.res, out.err, receivedAt)
			}
		})
	}
}
