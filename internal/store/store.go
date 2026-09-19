// Package store persists batches and chunks in PostgreSQL.
//
// All coordination between API instances happens through database row locks,
// so any number of processes sharing one database reach the same verdict for
// a batch: duplicate chunks are idempotent, conflicting payloads are rejected
// and a batch can only become SEALED when its full chunk set is present.
package store

import (
	"bytes"
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

const (
	StatusOpen   = "OPEN"
	StatusSealed = "SEALED"
)

var (
	ErrNotFound   = errors.New("batch not found")
	ErrConflict   = errors.New("chunk conflict")
	ErrSealed     = errors.New("batch sealed")
	ErrIncomplete = errors.New("batch incomplete")
	ErrSeqRange   = errors.New("sequence out of range")
)

// Batch is the persisted batch header.
type Batch struct {
	ID             string
	ExpectedChunks int
	Status         string
	CreatedAt      time.Time
	SealedAt       *time.Time
}

// Snapshot is a consistent view of a batch: counts and gaps are computed by
// the same SQL statement.
type Snapshot struct {
	Batch
	Received int
	Gaps     []int
}

// SubmitResult describes one accepted chunk write. A duplicate (same batch,
// seq and byte-identical payload) reports Created == false together with the
// stored confirmation of the original write.
type SubmitResult struct {
	Created    bool
	Seq        int
	Size       int
	ReceivedAt time.Time
}

// Store wraps a connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New connects, verifies the connection and applies the schema.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 10
	// Transactions must run at the default READ COMMITTED. Every
	// adjudication first locks the batch row, then reads; the post-lock
	// statements have to observe whatever the previous lock holder
	// committed while this transaction was waiting. A frozen snapshot
	// (REPEATABLE READ) is taken at the first statement — the lock
	// acquisition itself — so a lock waiter would miss the holder's
	// committed chunks: retransmissions would degrade into duplicate-key
	// errors and complete batches would be judged INCOMPLETE.
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// migrationLockID serialises schema migrations across API instances booting
// at the same time. Concurrent CREATE TABLE IF NOT EXISTS on a fresh database
// otherwise races in the system catalog (SQLSTATE 23505).
const migrationLockID int64 = 0x5B4A_4545_414C_0001

func (s *Store) migrate(ctx context.Context) error {
	// DDL is transactional in PostgreSQL; the advisory lock makes concurrent
	// first boots take turns: the loser blocks here, then finds every object
	// present and the IF NOT EXISTS statements become no-ops.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if _, err := tx.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}

// NewBatchID returns a random 128-bit hex identifier.
func NewBatchID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// CreateBatch inserts an OPEN batch. A random ID collision is retried.
func (s *Store) CreateBatch(ctx context.Context, expectedChunks int) (*Batch, error) {
	const maxAttempts = 3
	for range maxAttempts {
		id := NewBatchID()
		var b Batch
		err := s.pool.QueryRow(ctx,
			`INSERT INTO batches (id, expected_chunks) VALUES ($1, $2)
			 RETURNING id, expected_chunks, status, created_at, sealed_at`,
			id, expectedChunks,
		).Scan(&b.ID, &b.ExpectedChunks, &b.Status, &b.CreatedAt, &b.SealedAt)
		if err == nil {
			return &b, nil
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			continue
		}
		return nil, err
	}
	return nil, errors.New("could not allocate unique batch id")
}

// SubmitChunk stores a chunk. The batch row is locked for the duration of the
// transaction, serialising it against a concurrent seal. Return values:
//
//   - first write: result.Created == true, nil
//   - retransmission with identical UTF-8 bytes: result.Created == false, nil
//   - same seq, different payload: zero result, ErrConflict
//   - sealed batch, any non-identical write: zero result, ErrSealed
func (s *Store) SubmitChunk(ctx context.Context, batchID string, seq int, payload []byte) (SubmitResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SubmitResult{}, err
	}
	defer tx.Rollback(ctx)

	var expected int
	var status string
	err = tx.QueryRow(ctx,
		`SELECT expected_chunks, status FROM batches WHERE id = $1 FOR UPDATE`,
		batchID,
	).Scan(&expected, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return SubmitResult{}, ErrNotFound
	}
	if err != nil {
		return SubmitResult{}, err
	}
	if seq < 1 || seq > expected {
		return SubmitResult{}, ErrSeqRange
	}
	if status == StatusSealed {
		// Identical retransmissions of an existing chunk stay allowed;
		// anything else is rejected.
		var existing []byte
		var receivedAt time.Time
		err := tx.QueryRow(ctx,
			`SELECT payload, received_at FROM chunks WHERE batch_id = $1 AND seq = $2`,
			batchID, seq,
		).Scan(&existing, &receivedAt)
		if err == nil && bytes.Equal(existing, payload) {
			return SubmitResult{Created: false, Seq: seq, Size: len(payload), ReceivedAt: receivedAt}, nil
		}
		return SubmitResult{}, ErrSealed
	}

	var existing []byte
	var receivedAt time.Time
	err = tx.QueryRow(ctx,
		`SELECT payload, received_at FROM chunks WHERE batch_id = $1 AND seq = $2`,
		batchID, seq,
	).Scan(&existing, &receivedAt)
	switch {
	case err == nil:
		if !bytes.Equal(existing, payload) {
			return SubmitResult{}, ErrConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return SubmitResult{}, err
		}
		return SubmitResult{Created: false, Seq: seq, Size: len(payload), ReceivedAt: receivedAt}, nil
	case errors.Is(err, pgx.ErrNoRows):
		// proceed to insert
	default:
		return SubmitResult{}, err
	}

	if err := tx.QueryRow(ctx,
		`INSERT INTO chunks (batch_id, seq, payload) VALUES ($1, $2, $3)
		 RETURNING received_at`,
		batchID, seq, payload,
	).Scan(&receivedAt); err != nil {
		return SubmitResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{Created: true, Seq: seq, Size: len(payload), ReceivedAt: receivedAt}, nil
}

// Snapshot returns a consistent status view of a batch.
func (s *Store) Snapshot(ctx context.Context, batchID string) (*Snapshot, error) {
	return snapshotQuery(ctx, s.pool, batchID, false)
}

// SealBatch atomically seals a batch when its chunk set is complete.
//
// The batch row is locked before counting, so a seal racing with the final
// chunk cannot commit a SEALED batch that is still missing a piece: one
// transaction waits for the other, then observes the final state.
// ErrIncomplete is returned together with the post-lock snapshot so the API
// can report the ascending gap list. A repeated seal is idempotent and
// returns the existing SEALED snapshot with nil.
func (s *Store) SealBatch(ctx context.Context, batchID string) (*Snapshot, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	snap, err := snapshotQuery(ctx, tx, batchID, true)
	if err != nil {
		return nil, err
	}
	if snap.Status == StatusSealed {
		// Idempotent: repeated seal returns the existing result unchanged.
		return snap, nil
	}
	if len(snap.Gaps) > 0 {
		return snap, ErrIncomplete
	}
	var sealedAt time.Time
	if err := tx.QueryRow(ctx,
		`UPDATE batches SET status = $1, sealed_at = now() WHERE id = $2
		 RETURNING sealed_at`,
		StatusSealed, batchID,
	).Scan(&sealedAt); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	snap.Status = StatusSealed
	snap.SealedAt = &sealedAt
	return snap, nil
}

// querier is satisfied by both a pool and a transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// snapshotQuery reads header, received count and ascending gaps in one
// transaction snapshot, so the numbers always agree. With forUpdate the
// caller owns the batch row lock for the surrounding transaction.
func snapshotQuery(ctx context.Context, q querier, batchID string, forUpdate bool) (*Snapshot, error) {
	// Missing sequences are the complement of the stored set within
	// [1, expectedChunks]. generate_series materialises the full range; the
	// expected count is capped at 10000 so this stays cheap.
	lock := ""
	if forUpdate {
		lock = " FOR UPDATE"
	}

	var snap Snapshot
	err := q.QueryRow(ctx,
		`SELECT id, expected_chunks, status, created_at, sealed_at
		 FROM batches WHERE id = $1`+lock, batchID,
	).Scan(&snap.ID, &snap.ExpectedChunks, &snap.Status, &snap.CreatedAt, &snap.SealedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	rows, err := q.Query(ctx, `
		SELECT s.seq
		FROM generate_series(1, $1::int) AS s(seq)
		WHERE NOT EXISTS (
			SELECT 1 FROM chunks c WHERE c.batch_id = $2 AND c.seq = s.seq
		)
		ORDER BY s.seq ASC`, snap.ExpectedChunks, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var g int
		if err := rows.Scan(&g); err != nil {
			return nil, err
		}
		snap.Gaps = append(snap.Gaps, g)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	snap.Received = snap.ExpectedChunks - len(snap.Gaps)
	return &snap, nil
}
