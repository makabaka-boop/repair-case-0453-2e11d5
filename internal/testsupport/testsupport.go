// Package testsupport shares database setup helpers between integration tests.
package testsupport

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"batchseal/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RequireURL returns the database URL tests should use. Tests skip when the
// database is unreachable.
func RequireURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = "postgres://postgres:postgres@localhost:5432/batchseal?sslmode=disable"
	}

	adminOnce.Do(func() {
		p, err := pgxpool.New(context.Background(), url)
		if err == nil {
			err = p.Ping(context.Background())
		}
		adminPool, adminErr = p, err
	})
	if adminErr != nil {
		t.Skipf("real PostgreSQL not available at %q: %v", url, adminErr)
	}
	return url
}

var (
	adminOnce sync.Once
	adminPool *pgxpool.Pool
	adminErr  error
)

// IsolatedSchema creates a throwaway schema for one test run.
func IsolatedSchema(ctx context.Context, t *testing.T) string {
	t.Helper()
	schema := fmt.Sprintf("it_%d", time.Now().UnixNano())
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return schema
}

// DropSchema removes an isolated schema.
func DropSchema(ctx context.Context, schema string) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, _ = adminPool.Exec(cctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
}

// SchemaURL appends search_path for the given schema to the base URL.
func SchemaURL(baseURL, schema string) string {
	sep := "?"
	for i := 0; i < len(baseURL); i++ {
		if baseURL[i] == '?' {
			sep = "&"
			break
		}
	}
	return baseURL + sep + "search_path=" + schema
}

// Migrate applies the application schema against a freshly created pool.
func Migrate(ctx context.Context, t *testing.T, url string) *store.Store {
	t.Helper()
	s, err := store.New(ctx, url)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return s
}
