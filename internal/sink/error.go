package sink

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

// ErrorKind says whether retrying a publish can help.
type ErrorKind uint8

const (
	KindTransient ErrorKind = iota
	KindPermanent
)

// ClassifiedError marks an error as permanent. Unclassified errors are
// treated as transient.
type ClassifiedError struct {
	Kind   ErrorKind
	Reason string
	Err    error
}

func (e *ClassifiedError) Error() string {
	if e.Reason == "" {
		return e.Err.Error()
	}
	return e.Reason + ": " + e.Err.Error()
}

func (e *ClassifiedError) Unwrap() error { return e.Err }

// Permanent wraps err with a stable reason. Retrying it will not help.
func Permanent(err error, reason string) error {
	if err == nil {
		return nil
	}
	return &ClassifiedError{Kind: KindPermanent, Reason: reason, Err: err}
}

// IsPermanent reports whether err was classified as permanent.
func IsPermanent(err error) bool {
	var e *ClassifiedError
	return errors.As(err, &e) && e.Kind == KindPermanent
}

// Reason returns the stable reason of a classified error, or an empty string.
func Reason(err error) string {
	var e *ClassifiedError
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

// Failure describes a message that could not be delivered.
type Failure struct {
	Err       error
	Reason    string
	Attempts  int
	Permanent bool
}

// DeadLetterer is implemented by sinks that can publish a failed message to a
// dead letter destination. Sinks without a DLQ return ErrDeadLetterNotConfigured.
type DeadLetterer interface {
	DeadLetter(ctx context.Context, m *Message, f Failure) error
}

var ErrDeadLetterNotConfigured = errors.New("sink: dead letter queue not configured")

// DLQHeaders returns the original message metadata plus the failure details.
func DLQHeaders(m *Message, f Failure) map[string]string {
	reason := f.Reason
	if reason == "" {
		reason = "unknown"
	}
	h := map[string]string{
		"op":                   m.Op,
		"key":                  string(m.Key),
		"walcast-dlq-source":   m.Topic,
		"walcast-dlq-reason":   reason,
		"walcast-dlq-attempts": strconv.Itoa(f.Attempts),
	}
	if m.ID != "" {
		h["id"] = m.ID
	}
	if m.LSN != "" {
		h["lsn"] = m.LSN
	}
	if f.Permanent {
		h["walcast-dlq-permanent"] = "true"
	}
	return h
}

// DeadLetterError annotates a DLQ publish failure.
func DeadLetterError(sinkName string, err error) error {
	return fmt.Errorf("sink %s: dead letter publish failed: %w", sinkName, err)
}
