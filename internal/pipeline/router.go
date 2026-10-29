package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"

	"walcast/internal/config"
	"walcast/internal/event"
	"walcast/internal/sink"
)

// Router turns an Event into a broker-ready Message.
type Router struct {
	prefix     string
	keyColumns map[string][]string
}

func NewRouter(r config.Routing) *Router {
	return &Router{prefix: r.Prefix, keyColumns: r.KeyColumns}
}

func (r *Router) Route(e event.Event) (*sink.Message, error) {
	value, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode %s event for %s.%s: %w", e.Op, e.Schema, e.Table, err)
	}
	return &sink.Message{
		Topic: r.prefix + "." + token(e.Schema) + "." + token(e.Table),
		Op:    string(e.Op),
		Key:   []byte(r.key(e)),
		Value: value,
		ID:    e.ID,
		LSN:   e.LSN,
	}, nil
}

// key is the configured key columns of the row (from the new image, or the old
// one for deletes), falling back to "schema.table" so ordering is never lost.
func (r *Router) key(e event.Event) string {
	fallback := e.Schema + "." + e.Table
	cols := r.keyColumns[fallback]
	if len(cols) == 0 {
		return fallback
	}
	row := e.After
	if row == nil {
		row = e.Before
	}
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		v, ok := row[c]
		if !ok {
			return fallback // e.g. truncate, or column missing from the image
		}
		parts = append(parts, fmt.Sprint(v))
	}
	return strings.Join(parts, ":")
}

// token makes an identifier safe for both Kafka topic names and NATS subject
// tokens by replacing everything outside [A-Za-z0-9_-] (notably '.', '*', '>').
func token(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			return r
		}
		return '_'
	}, s)
}
