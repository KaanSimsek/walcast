package pipeline

import (
	"testing"

	"walcast/internal/config"
	"walcast/internal/event"
)

func TestRouterKey(t *testing.T) {
	r := NewRouter(config.Routing{Prefix: "p", KeyColumns: map[string][]string{
		"public.orders": {"tenant", "id"},
	}})
	cases := []struct {
		name string
		e    event.Event
		want string
	}{
		{"composite from after", event.Event{Schema: "public", Table: "orders", After: map[string]any{"tenant": "acme", "id": 3}}, "acme:3"},
		{"delete uses before", event.Event{Schema: "public", Table: "orders", Before: map[string]any{"tenant": "acme", "id": 3}}, "acme:3"},
		{"missing column falls back", event.Event{Schema: "public", Table: "orders", After: map[string]any{"id": 3}}, "public.orders"},
		{"unconfigured table", event.Event{Schema: "public", Table: "other", After: map[string]any{"id": 1}}, "public.other"},
		{"truncate", event.Event{Op: event.OpTruncate, Schema: "public", Table: "orders"}, "public.orders"},
	}
	for _, c := range cases {
		m, err := r.Route(c.e)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if string(m.Key) != c.want {
			t.Errorf("%s: key = %q, want %q", c.name, m.Key, c.want)
		}
	}
}

func TestRouterSanitizesTopic(t *testing.T) {
	r := NewRouter(config.Routing{Prefix: "p"})
	m, _ := r.Route(event.Event{Op: event.OpInsert, Schema: "my.schema", Table: "we ird>*"})
	if m.Topic != "p.my_schema.we_ird__" {
		t.Errorf("topic = %q", m.Topic)
	}
}
