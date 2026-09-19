package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"batchseal/internal/api"
	"batchseal/internal/store"
	"batchseal/internal/testsupport"
)

// Two in-process HTTP servers backed by independent connection pools on the
// same schema stand in for two API instances arbitrating via PostgreSQL.
func newAPIs(t *testing.T) (string, string, func()) {
	t.Helper()
	ctx := context.Background()
	url := testsupport.RequireURL(t)
	schema := testsupport.IsolatedSchema(ctx, t)

	open := func() (*store.Store, *httptest.Server) {
		st, err := store.New(ctx, testsupport.SchemaURL(url, schema))
		if err != nil {
			t.Fatal(err)
		}
		return st, httptest.NewServer(api.NewServer(st, nil).Handler())
	}
	s1, srv1 := open()
	s2, srv2 := open()

	cleanup := func() {
		srv1.Close()
		srv2.Close()
		s1.Close()
		s2.Close()
		testsupport.DropSchema(ctx, schema)
	}
	return srv1.URL, srv2.URL, cleanup
}

type client struct {
	http *http.Client
}

func newClient() *client {
	return &client{http: &http.Client{Timeout: 15 * time.Second}}
}

func (c *client) do(t *testing.T, method, url, raw string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if raw != "" {
		rdr = strings.NewReader(raw)
	} else if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil || raw != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	parsed := map[string]any{}
	if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("decode %q: %v", data, err)
		}
	}
	return resp.StatusCode, parsed
}

func (c *client) post(t *testing.T, url string, body any) (int, map[string]any) {
	return c.do(t, http.MethodPost, url, "", body)
}

func (c *client) postRaw(t *testing.T, url, raw string) (int, map[string]any) {
	return c.do(t, http.MethodPost, url, raw, nil)
}

func (c *client) get(t *testing.T, url string) (int, map[string]any) {
	return c.do(t, http.MethodGet, url, "", nil)
}

func TestHTTPLifecycleAcrossTwoInstances(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	code, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 3})
	if code != 201 {
		t.Fatalf("create: code=%d body=%v", code, body)
	}
	batchID, _ := body["batchId"].(string)

	// Out-of-order chunks alternating between the two instances.
	payloads := map[int]string{1: "one", 2: "two", 3: "three"}
	for _, seq := range []int{3, 1, 2} {
		base := u1
		if seq == 1 {
			base = u2
		}
		code, b := c.post(t, base+"/api/v1/batches/"+batchID+"/chunks",
			map[string]any{"seq": seq, "payload": payloads[seq]})
		if code != 201 || b["duplicate"] != false {
			t.Fatalf("chunk %d first write: code=%d body=%v", seq, code, b)
		}
	}

	// Retransmission on the other instance returns the original confirmation.
	code, b := c.post(t, u1+"/api/v1/batches/"+batchID+"/chunks",
		map[string]any{"seq": 3, "payload": "three"})
	if code != 200 || b["duplicate"] != true {
		t.Fatalf("retransmission: code=%d body=%v", code, b)
	}
	if int(b["size"].(float64)) != len("three") {
		t.Fatalf("size = %v", b["size"])
	}

	// Different content conflicts.
	code, b = c.post(t, u2+"/api/v1/batches/"+batchID+"/chunks",
		map[string]any{"seq": 3, "payload": "THREE"})
	if code != 409 || b["error"] != "CHUNK_CONFLICT" {
		t.Fatalf("conflict: code=%d body=%v", code, b)
	}

	// Seal on instance 1; the verdict is immediately visible on instance 2.
	code, b = c.post(t, u1+"/api/v1/batches/"+batchID+"/seal", nil)
	if code != 200 || b["status"] != "SEALED" {
		t.Fatalf("seal: code=%d body=%v", code, b)
	}
	firstSealTime, _ := b["sealedAt"].(string)

	code, b = c.get(t, u2+"/api/v1/batches/"+batchID)
	if code != 200 || b["status"] != "SEALED" ||
		int(b["expected"].(float64)) != 3 || int(b["received"].(float64)) != 3 {
		t.Fatalf("cross-instance status: code=%d body=%v", code, b)
	}

	// Repeated seal on instance 2 returns the existing result.
	code, b = c.post(t, u2+"/api/v1/batches/"+batchID+"/seal", nil)
	if code != 200 || b["sealedAt"] != firstSealTime {
		t.Fatalf("reseal: code=%d body=%v", code, b)
	}

	// Sealed: identical retransmission accepted, changed content rejected.
	code, _ = c.post(t, u2+"/api/v1/batches/"+batchID+"/chunks",
		map[string]any{"seq": 1, "payload": "one"})
	if code != 200 {
		t.Fatalf("identical after seal: %d", code)
	}
	code, b = c.post(t, u1+"/api/v1/batches/"+batchID+"/chunks",
		map[string]any{"seq": 1, "payload": "changed"})
	if code != 409 || b["error"] != "BATCH_SEALED" {
		t.Fatalf("changed after seal: code=%d body=%v", code, b)
	}
}

func TestHTTPIncompleteSealReportsGaps(t *testing.T) {
	u1, _, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	_, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 5})
	batchID := body["batchId"].(string)
	for _, seq := range []int{1, 3, 5} {
		c.post(t, u1+"/api/v1/batches/"+batchID+"/chunks",
			map[string]any{"seq": seq, "payload": "x"})
	}

	code, body := c.post(t, u1+"/api/v1/batches/"+batchID+"/seal", nil)
	if code != 409 || body["error"] != "INCOMPLETE" {
		t.Fatalf("seal incomplete: code=%d body=%v", code, body)
	}
	gaps := body["gaps"].([]any)
	if len(gaps) != 2 || int(gaps[0].(float64)) != 2 || int(gaps[1].(float64)) != 4 {
		t.Fatalf("gaps: %v", gaps)
	}
	if body["status"] != "OPEN" || int(body["received"].(float64)) != 3 {
		t.Fatalf("incomplete body wrong: %v", body)
	}

	code, body = c.get(t, u1+"/api/v1/batches/"+batchID)
	if code != 200 || body["status"] != "OPEN" {
		t.Fatalf("batch must remain OPEN after failed seal: %v", body)
	}
}

func TestHTTPValidationCodes(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	_, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 2})
	batchID := body["batchId"].(string)

	cases := []struct {
		name       string
		method     string
		url        string
		body       any
		wantStatus int
		wantError  string
	}{
		{"expected zero", http.MethodPost, u1 + "/api/v1/batches",
			map[string]int{"expectedChunks": 0}, 400, "INVALID_EXPECTED_CHUNKS"},
		{"expected too big", http.MethodPost, u1 + "/api/v1/batches",
			map[string]int{"expectedChunks": 10001}, 400, "INVALID_EXPECTED_CHUNKS"},
		{"malformed batch id", http.MethodGet, u1 + "/api/v1/batches/zzz", nil,
			404, "BATCH_NOT_FOUND"},
		{"missing batch", http.MethodGet, u1 + "/api/v1/batches/ffffffffffffffffffffffffffffffff", nil,
			404, "BATCH_NOT_FOUND"},
		{"chunk on missing batch", http.MethodPost,
			u2 + "/api/v1/batches/ffffffffffffffffffffffffffffffff/chunks",
			map[string]any{"seq": 1, "payload": "x"}, 404, "BATCH_NOT_FOUND"},
		{"seq out of range", http.MethodPost, u1 + "/api/v1/batches/" + batchID + "/chunks",
			map[string]any{"seq": 3, "payload": "x"}, 400, "SEQ_OUT_OF_RANGE"},
		{"payload not string", http.MethodPost, u1 + "/api/v1/batches/" + batchID + "/chunks",
			map[string]any{"seq": 1, "payload": 9}, 400, "INVALID_PAYLOAD"},
		{"seq not number", http.MethodPost, u2 + "/api/v1/batches/" + batchID + "/chunks",
			map[string]any{"seq": "1", "payload": "x"}, 400, "INVALID_SEQ"},
		{"missing field", http.MethodPost, u1 + "/api/v1/batches/" + batchID + "/chunks",
			map[string]any{"seq": 1}, 400, "INVALID_REQUEST"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var code int
			var b map[string]any
			if tc.method == http.MethodPost {
				code, b = c.post(t, tc.url, tc.body)
			} else {
				code, b = c.get(t, tc.url)
			}
			if code != tc.wantStatus || b["error"] != tc.wantError {
				t.Fatalf("got code=%d body=%v, want %d %s", code, b, tc.wantStatus, tc.wantError)
			}
		})
	}

	// 413: payload strictly over 65536 UTF-8 bytes.
	big := strings.Repeat("a", 65537)
	code, b := c.postRaw(t, u1+"/api/v1/batches/"+batchID+"/chunks",
		`{"seq":1,"payload":"`+big+`"}`)
	if code != 413 || b["error"] != "PAYLOAD_TOO_LARGE" {
		t.Fatalf("oversize payload: code=%d body=%v", code, b)
	}

	// Exactly 65536 bytes is accepted.
	exact := strings.Repeat("a", 65536)
	code, _ = c.postRaw(t, u1+"/api/v1/batches/"+batchID+"/chunks",
		`{"seq":2,"payload":"`+exact+`"}`)
	if code != 201 {
		t.Fatalf("exact-limit payload should be accepted, got %d", code)
	}
}

// TestHTTPCrossInstanceSealRace drives the final chunk through one instance
// and the seal through the other concurrently. A SEALED batch must never be
// missing a chunk.
func TestHTTPCrossInstanceSealRace(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()
	c := newClient()

	const rounds = 20
	for range rounds {
		_, body := c.post(t, u1+"/api/v1/batches", map[string]int{"expectedChunks": 1})
		batchID := body["batchId"].(string)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); c.post(t, u1+"/api/v1/batches/"+batchID+"/seal", nil) }()
		go func() {
			defer wg.Done()
			c.post(t, u2+"/api/v1/batches/"+batchID+"/chunks",
				map[string]any{"seq": 1, "payload": "only"})
		}()
		wg.Wait()

		code, snap := c.get(t, u1+"/api/v1/batches/"+batchID)
		if code != 200 {
			t.Fatalf("status: %d %v", code, snap)
		}
		if snap["status"] == "SEALED" && int(snap["received"].(float64)) != 1 {
			t.Fatalf("SEALED with missing chunk: %v", snap)
		}
	}
}

// TestHTTPRestartBothInstances closes both servers/stores and serves the same
// data with freshly created ones: every verdict must be identical.
func TestHTTPRestartBothInstances(t *testing.T) {
	ctx := context.Background()
	base := testsupport.RequireURL(t)
	schema := testsupport.IsolatedSchema(ctx, t)
	defer testsupport.DropSchema(ctx, schema)
	c := newClient()

	var batchOpen, batchSealed string
	var sealTime string
	{
		s1, err := store.New(ctx, testsupport.SchemaURL(base, schema))
		if err != nil {
			t.Fatal(err)
		}
		s2, err := store.New(ctx, testsupport.SchemaURL(base, schema))
		if err != nil {
			t.Fatal(err)
		}
		srv1 := httptest.NewServer(api.NewServer(s1, nil).Handler())
		srv2 := httptest.NewServer(api.NewServer(s2, nil).Handler())

		_, b := c.post(t, srv1.URL+"/api/v1/batches", map[string]int{"expectedChunks": 3})
		batchOpen = b["batchId"].(string)
		c.post(t, srv2.URL+"/api/v1/batches/"+batchOpen+"/chunks", map[string]any{"seq": 2, "payload": "x"})

		_, b = c.post(t, srv1.URL+"/api/v1/batches", map[string]int{"expectedChunks": 1})
		batchSealed = b["batchId"].(string)
		c.post(t, srv2.URL+"/api/v1/batches/"+batchSealed+"/chunks", map[string]any{"seq": 1, "payload": "z"})
		_, b = c.post(t, srv2.URL+"/api/v1/batches/"+batchSealed+"/seal", nil)
		sealTime, _ = b["sealedAt"].(string)

		// "Restart" both instances.
		srv1.Close()
		srv2.Close()
		s1.Close()
		s2.Close()
	}

	{
		s1, err := store.New(ctx, testsupport.SchemaURL(base, schema))
		if err != nil {
			t.Fatal(err)
		}
		s2, err := store.New(ctx, testsupport.SchemaURL(base, schema))
		if err != nil {
			t.Fatal(err)
		}
		srv1 := httptest.NewServer(api.NewServer(s1, nil).Handler())
		srv2 := httptest.NewServer(api.NewServer(s2, nil).Handler())
		defer func() { srv1.Close(); srv2.Close(); s1.Close(); s2.Close() }()

		code, open := c.get(t, srv1.URL+"/api/v1/batches/"+batchOpen)
		if code != 200 || open["status"] != "OPEN" ||
			int(open["received"].(float64)) != 1 ||
			len(open["gaps"].([]any)) != 2 {
			t.Fatalf("open batch changed over restart: %v", open)
		}
		gaps := open["gaps"].([]any)
		if int(gaps[0].(float64)) != 1 || int(gaps[1].(float64)) != 3 {
			t.Fatalf("gaps not ascending over restart: %v", gaps)
		}

		code, sealed := c.get(t, srv2.URL+"/api/v1/batches/"+batchSealed)
		if code != 200 || sealed["status"] != "SEALED" || sealed["sealedAt"] != sealTime {
			t.Fatalf("sealed verdict changed over restart: %v (want sealedAt %s)", sealed, sealTime)
		}

		// Duplicate ack after restart: still the original, 200 + duplicate.
		code, ack := c.post(t, srv1.URL+"/api/v1/batches/"+batchSealed+"/chunks",
			map[string]any{"seq": 1, "payload": "z"})
		if code != 200 || ack["duplicate"] != true {
			t.Fatalf("post-restart retransmission: code=%d body=%v", code, ack)
		}

		// Repeated seal after restart: existing result, same sealedAt.
		code, reseal := c.post(t, srv2.URL+"/api/v1/batches/"+batchSealed+"/seal", nil)
		if code != 200 || reseal["sealedAt"] != sealTime {
			t.Fatalf("reseal after restart: code=%d body=%v", code, reseal)
		}
	}
}
