package events

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWebhookDispatcherDeliveryAndSignature(t *testing.T) {
	const secret = "super-secret-webhook-key"

	var (
		mu      sync.Mutex
		gotEvt  Event
		gotBody []byte
		gotSig  string
	)
	done := make(chan struct{}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body failed: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var evt Event
		if err := json.Unmarshal(body, &evt); err != nil {
			t.Errorf("unmarshal event failed: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		mu.Lock()
		gotEvt = evt
		gotBody = body
		gotSig = r.Header.Get(SignatureHeader)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		select {
		case done <- struct{}{}:
		default:
		}
	}))
	defer srv.Close()

	d := NewWebhookDispatcher(srv.URL, secret, 8)
	ts := time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC)
	want := Event{
		ID:        "evt-1",
		Type:      EventJobSubmitted,
		OrgID:     "org-acme",
		JobID:     "job-1",
		Step:      0,
		Status:    "QUEUED",
		Timestamp: ts,
	}

	d.Emit(want)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for webhook delivery")
	}
	d.Close()

	mu.Lock()
	defer mu.Unlock()

	if gotEvt.ID != want.ID || gotEvt.Type != want.Type || gotEvt.JobID != want.JobID || gotEvt.OrgID != want.OrgID {
		t.Fatalf("delivered event = %+v, want %+v", gotEvt, want)
	}
	if !VerifySignature(secret, gotBody, gotSig) {
		t.Fatalf("VerifySignature returned false for valid signature %q", gotSig)
	}
}

func TestVerifySignature(t *testing.T) {
	const secret = "webhook-signing-secret"
	payload := []byte(`{"id":"evt-1","type":"job.completed"}`)
	validHeader := signPayload(secret, payload)

	if !VerifySignature(secret, payload, validHeader) {
		t.Fatal("expected valid signature to verify")
	}
	if VerifySignature("wrong-secret", payload, validHeader) {
		t.Fatal("expected wrong secret to fail verification")
	}
	if VerifySignature(secret, []byte(`{"id":"evt-2"}`), validHeader) {
		t.Fatal("expected tampered payload to fail verification")
	}
	if VerifySignature(secret, payload, "md5=deadbeef") {
		t.Fatal("expected invalid prefix to fail verification")
	}
	if VerifySignature(secret, payload, "sha256=not-valid-hex") {
		t.Fatal("expected invalid hex to fail verification")
	}
	if VerifySignature(secret, payload, "") {
		t.Fatal("expected empty header to fail verification")
	}
}

func TestWebhookDispatcherNonBlockingDropOnFullQueue(t *testing.T) {
	release := make(chan struct{})
	inHandler := make(chan struct{})
	var once sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(inHandler) })
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := NewWebhookDispatcher(srv.URL, "secret", 2)

	// Emit first event and wait until worker is blocked inside the HTTP handler.
	d.Emit(Event{ID: "evt-0", Type: EventJobSubmitted, JobID: "job-0"})
	select {
	case <-inHandler:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to enter handler")
	}

	// Fill the buffer (capacity 2) and emit extra events; none should block.
	start := time.Now()
	for i := 1; i <= 10; i++ {
		d.Emit(Event{ID: "evt", Type: EventJobAssigned, JobID: "job"})
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("Emit blocked for %v on full queue", elapsed)
	}

	if dropped := d.DroppedCount(); dropped != 8 {
		t.Fatalf("DroppedCount() = %d, want 8", dropped)
	}

	close(release)
	d.Close()
}

func TestWebhookDispatcherGracefulClose(t *testing.T) {
	var (
		mu       sync.Mutex
		received int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		received++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := NewWebhookDispatcher(srv.URL, "secret", 16)
	const total = 5
	for i := 0; i < total; i++ {
		d.Emit(Event{ID: "evt", Type: EventJobCheckpointed, JobID: "job-1", Step: i + 1})
	}

	// Close must drain all queued events before returning.
	d.Close()
	// Idempotent second Close and post-close Emit must not panic.
	d.Close()
	d.Emit(Event{ID: "evt-after-close", Type: EventJobCancelled, JobID: "job-1"})

	mu.Lock()
	got := received
	mu.Unlock()
	if got != total {
		t.Fatalf("received %d events before Close returned, want %d", got, total)
	}
	if d.DroppedCount() != 1 {
		t.Fatalf("DroppedCount() = %d, want 1 (post-close emit)", d.DroppedCount())
	}
}

func TestWebhookDispatcherRetryAndCounters(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch attempts.Add(1) {
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable)
		case 2:
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	d := NewWebhookDispatcher(srv.URL, "secret", 8).WithRetry(2, 5*time.Millisecond)
	d.Emit(Event{ID: "evt-retry", Type: EventJobCompleted, JobID: "job-1"})
	d.Close()

	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
	if d.DeliveredCount() != 1 {
		t.Fatalf("DeliveredCount() = %d, want 1", d.DeliveredCount())
	}
	if d.FailedCount() != 0 {
		t.Fatalf("FailedCount() = %d, want 0", d.FailedCount())
	}
}

func TestWebhookDispatcherPermanentFailureCounter(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	d := NewWebhookDispatcher(srv.URL, "secret", 8).WithRetry(2, 5*time.Millisecond)
	d.Emit(Event{ID: "evt-perm-fail", Type: EventJobFailed, JobID: "job-2"})
	d.Close()

	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1 (400 must not be retried)", got)
	}
	if d.DeliveredCount() != 0 {
		t.Fatalf("DeliveredCount() = %d, want 0", d.DeliveredCount())
	}
	if d.FailedCount() != 1 {
		t.Fatalf("FailedCount() = %d, want 1", d.FailedCount())
	}
}

func TestValidateWebhookURL(t *testing.T) {
	valid := []string{
		"http://127.0.0.1:8080/webhook",
		"https://hooks.example.com/computeharbor",
		"HTTPS://hooks.example.com:8443/events",
	}
	for _, raw := range valid {
		if err := ValidateWebhookURL(raw); err != nil {
			t.Errorf("ValidateWebhookURL(%q) unexpected error: %v", raw, err)
		}
	}

	invalid := []struct {
		name string
		url  string
	}{
		{name: "empty", url: ""},
		{name: "whitespace", url: "   "},
		{name: "file scheme", url: "file:///etc/passwd"},
		{name: "ftp scheme", url: "ftp://files.example.com/hook"},
		{name: "empty host", url: "https:///webhook"},
		{name: "port only host", url: "http://:8080/webhook"},
		{name: "embedded userinfo", url: "https://user:pass@example.com/webhook"},
		{name: "embedded username only", url: "https://token@example.com/webhook"},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateWebhookURL(tc.url); err == nil {
				t.Fatalf("ValidateWebhookURL(%q) expected error, got nil", tc.url)
			}
		})
	}
}

func TestWebhookDispatcherRejectsRedirects(t *testing.T) {
	var targetHits atomic.Int32
	targetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer targetSrv.Close()

	redirectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, targetSrv.URL, http.StatusFound)
	}))
	defer redirectSrv.Close()

	d := NewWebhookDispatcher(redirectSrv.URL, "secret", 8).WithRetry(0, 0)
	d.Emit(Event{ID: "evt-redirect", Type: EventJobSubmitted, JobID: "job-redirect"})
	d.Close()

	if got := targetHits.Load(); got != 0 {
		t.Fatalf("redirect target received %d requests, want 0", got)
	}
	if d.DeliveredCount() != 0 {
		t.Fatalf("DeliveredCount() = %d, want 0", d.DeliveredCount())
	}
	if d.FailedCount() != 1 {
		t.Fatalf("FailedCount() = %d, want 1", d.FailedCount())
	}
}
