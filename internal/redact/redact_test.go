package redact

import "testing"

func TestURL(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"nats://user:secret@localhost:4222", "nats://redacted@localhost:4222"},
		{"nats://token@localhost:4222", "nats://redacted@localhost:4222"},
		{"nats://localhost:4222?token=secret&x=1", "nats://localhost:4222?token=redacted&x=1"},
		{"nats://localhost:4222", "nats://localhost:4222"},
		{"://bad", "<invalid-url>"},
	}
	for _, c := range cases {
		if got := URL(c.in); got != c.want {
			t.Errorf("URL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
