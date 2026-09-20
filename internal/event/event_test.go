package event

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
)

func TestFromReplication(t *testing.T) {
	up := FromReplication(&format.Update{
		TableNamespace: "public", TableName: "users",
		OldDecoded: map[string]any{"id": 1, "n": "a"}, NewDecoded: map[string]any{"id": 1, "n": "b"},
	})
	if len(up) != 1 || up[0].Op != OpUpdate || up[0].Before["n"] != "a" || up[0].After["n"] != "b" {
		t.Errorf("update: %+v", up)
	}

	tr := FromReplication(&format.Truncate{Relations: []*format.Relation{{Namespace: "public", Name: "a"}, {Namespace: "x", Name: "b"}}})
	if len(tr) != 2 || tr[1].Schema != "x" || tr[1].Op != OpTruncate {
		t.Errorf("truncate: %+v", tr)
	}

	if got := FromReplication(&format.Snapshot{EventType: format.SnapshotEventTypeEnd}); got != nil {
		t.Errorf("snapshot END should yield nothing, got %+v", got)
	}
	snap := FromReplication(&format.Snapshot{EventType: format.SnapshotEventTypeData, Schema: "public", Table: "t", Data: map[string]any{"id": 1}})
	if len(snap) != 1 || snap[0].Op != OpSnapshot || snap[0].ID != "" {
		t.Errorf("snapshot data: %+v", snap)
	}
	if FromReplication("garbage") != nil {
		t.Error("unknown message type should yield nothing")
	}
}
