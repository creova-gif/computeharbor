package events

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// SignatureHeader is the HTTP header carrying the HMAC-SHA256 payload signature.
	SignatureHeader = "X-ComputeHarbor-Signature"
	signaturePrefix = "sha256="
	defaultBufSize  = 256
)

// EventType identifies a job lifecycle transition emitted by the orchestrator.
type EventType string

const (
	EventJobSubmitted    EventType = "job.submitted"
	EventJobAssigned     EventType = "job.assigned"
	EventJobPreempted    EventType = "job.preempted"
	EventJobCheckpointed EventType = "job.checkpointed"
	EventJobCompleted    EventType = "job.completed"
	EventJobCancelled    EventType = "job.cancelled"
	EventJobFailed       EventType = "job.failed"
)

// Event represents a single orchestrator job lifecycle event.
type Event struct {
	ID        string    `json:"id"`
	Type      EventType `json:"type"`
	OrgID     string    `json:"org_id"`
	JobID     string    `json:"job_id"`
	Step      int       `json:"step"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// Sink receives orchestrator lifecycle events.
type Sink interface {
	Emit(Event)
}

// WebhookDispatcher asynchronously delivers events as HMAC-SHA256 signed HTTP POST requests.
type WebhookDispatcher struct {
	endpoint string
	secret   string
	client   *http.Client

	ch      chan Event
	mu      sync.RWMutex
	closed  bool
	wg      sync.WaitGroup
	dropped atomic.Int64
}

// NewWebhookDispatcher creates a WebhookDispatcher with a bounded queue and starts its worker goroutine.
func NewWebhookDispatcher(endpoint, secret string, bufferSize int) *WebhookDispatcher {
	if bufferSize <= 0 {
		bufferSize = defaultBufSize
	}
	d := &WebhookDispatcher{
		endpoint: endpoint,
		secret:   secret,
		client:   &http.Client{Timeout: 10 * time.Second},
		ch:       make(chan Event, bufferSize),
	}
	d.wg.Add(1)
	go d.worker()
	return d
}

// WithHTTPClient overrides the HTTP client used for webhook delivery.
func (d *WebhookDispatcher) WithHTTPClient(client *http.Client) *WebhookDispatcher {
	if client != nil {
		d.client = client
	}
	return d
}

// Emit enqueues an event without blocking. If the queue is full or the dispatcher is closed,
// the event is dropped and counted.
func (d *WebhookDispatcher) Emit(evt Event) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		d.dropped.Add(1)
		return
	}

	select {
	case d.ch <- evt:
	default:
		d.dropped.Add(1)
	}
}

// DroppedCount returns the number of events dropped due to a full queue or post-close emission.
func (d *WebhookDispatcher) DroppedCount() int64 {
	return d.dropped.Load()
}

// Close stops accepting new events, drains the buffered queue, and waits for the worker to exit.
func (d *WebhookDispatcher) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	close(d.ch)
	d.mu.Unlock()

	d.wg.Wait()
}

func (d *WebhookDispatcher) worker() {
	defer d.wg.Done()
	for evt := range d.ch {
		d.deliver(evt)
	}
}

func (d *WebhookDispatcher) deliver(evt Event) {
	if d.endpoint == "" {
		return
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return
	}

	req, err := http.NewRequest(http.MethodPost, d.endpoint, bytes.NewReader(payload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(SignatureHeader, signPayload(d.secret, payload))

	resp, err := d.client.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

func signPayload(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature validates an X-ComputeHarbor-Signature header against payload and secret
// using constant-time HMAC-SHA256 comparison.
func VerifySignature(secret string, payload []byte, header string) bool {
	if !strings.HasPrefix(header, signaturePrefix) {
		return false
	}
	sigHex := strings.TrimPrefix(header, signaturePrefix)
	gotMAC, err := hex.DecodeString(sigHex)
	if err != nil || len(gotMAC) != sha256.Size {
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	expectedMAC := mac.Sum(nil)
	return hmac.Equal(gotMAC, expectedMAC)
}
