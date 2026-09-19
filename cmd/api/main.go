// Command batchseal runs the batch sealing API.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"batchseal/internal/api"
	"batchseal/internal/store"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	logger := log.New(os.Stdout, "batchseal ", log.LstdFlags|log.Lmsgprefix)

	addr := ":" + getenv("API_PORT", "8080")
	databaseURL := getenv("DATABASE_URL",
		"postgres://postgres:postgres@localhost:5432/batchseal?sslmode=disable")

	// Postgres in the compose stack may still be starting; retry briefly.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var st *store.Store
	var err error
	deadline := time.Now().Add(30 * time.Second)
	for {
		st, err = store.New(ctx, databaseURL)
		if err == nil {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			logger.Fatalf("database unavailable: %v", err)
		}
		logger.Printf("waiting for database: %v", err)
		select {
		case <-ctx.Done():
			logger.Fatalf("interrupted while waiting for database")
		case <-time.After(time.Second):
		}
	}
	defer st.Close()

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewServer(st, logger).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Printf("listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Printf("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}
