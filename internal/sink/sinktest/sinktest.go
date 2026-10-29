// Package sinktest has helpers shared by the sink backend tests.
package sinktest

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// Decode returns a sink.Config.Decode-compatible function for a YAML document.
func Decode(t *testing.T, doc string) func(any) error {
	t.Helper()
	var n yaml.Node
	if err := yaml.Unmarshal([]byte(doc), &n); err != nil {
		t.Fatal(err)
	}
	return n.Decode
}
