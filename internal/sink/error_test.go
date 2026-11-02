package sink

import (
	"errors"
	"testing"
)

func TestPermanentError(t *testing.T) {
	base := errors.New("broker rejected it")
	err := Permanent(base, "too_large")

	if !IsPermanent(err) {
		t.Fatal("IsPermanent = false")
	}
	if got := Reason(err); got != "too_large" {
		t.Errorf("reason = %q", got)
	}
	if !errors.Is(err, base) {
		t.Error("classified error does not wrap the original")
	}
	if IsPermanent(base) {
		t.Error("plain error should be transient")
	}
	if got := Reason(base); got != "" {
		t.Errorf("plain error reason = %q", got)
	}
	if Permanent(nil, "x") != nil {
		t.Error("Permanent(nil) should be nil")
	}
}

func TestPermanentSurvivesWrapping(t *testing.T) {
	err := errors.Join(errors.New("context"), Permanent(errors.New("nope"), "auth"))
	if !IsPermanent(err) {
		t.Error("IsPermanent should find a nested classified error")
	}
}

func TestDLQHeaders(t *testing.T) {
	m := &Message{Topic: "walcast.public.users", Op: "insert", Key: []byte("7"), ID: "id-1", LSN: "0/1"}
	h := DLQHeaders(m, Failure{Reason: "unroutable", Attempts: 3, Permanent: true})
	want := map[string]string{
		"op":                    "insert",
		"key":                   "7",
		"id":                    "id-1",
		"lsn":                   "0/1",
		"walcast-dlq-source":    "walcast.public.users",
		"walcast-dlq-reason":    "unroutable",
		"walcast-dlq-attempts":  "3",
		"walcast-dlq-permanent": "true",
	}
	for k, v := range want {
		if h[k] != v {
			t.Errorf("header %s = %q, want %q", k, h[k], v)
		}
	}
}
