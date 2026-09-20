// Package event converts go-pq-cdc replication messages into the
// broker-agnostic change events that walcast publishes.
package event

import (
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
)

type Op string

const (
	OpInsert   Op = "insert"
	OpUpdate   Op = "update"
	OpDelete   Op = "delete"
	OpTruncate Op = "truncate"
	// OpSnapshot is a row read during the initial snapshot, not a WAL change.
	OpSnapshot Op = "snapshot"
)

// Event is the JSON envelope published to every sink.
//
// Before is only populated for update/delete, and for updates only when the
// table's REPLICA IDENTITY is FULL (otherwise it holds just the key columns,
// or nothing if the key did not change).
type Event struct {
	// ID is unique per WAL change and stable across redelivery, so sinks can
	// use it for broker-side deduplication. Empty when no stable ID exists.
	ID        string         `json:"id,omitempty"`
	Op        Op             `json:"op"`
	Schema    string         `json:"schema"`
	Table     string         `json:"table"`
	LSN       string         `json:"lsn,omitempty"`
	Timestamp time.Time      `json:"ts"`
	Before    map[string]any `json:"before,omitempty"`
	After     map[string]any `json:"after,omitempty"`
}

// FromReplication converts a go-pq-cdc message into zero or more events.
// Messages that carry no data (snapshot BEGIN/END markers, unknown types)
// yield none. A TRUNCATE yields one event per truncated table.
func FromReplication(msg any) []Event {
	switch m := msg.(type) {
	case *format.Insert:
		return []Event{{
			ID: changeID(m.LSN.String(), m.TableNamespace, m.TableName, OpInsert),
			Op: OpInsert, Schema: m.TableNamespace, Table: m.TableName,
			LSN: m.LSN.String(), Timestamp: m.MessageTime, After: m.Decoded,
		}}
	case *format.Update:
		return []Event{{
			ID: changeID(m.LSN.String(), m.TableNamespace, m.TableName, OpUpdate),
			Op: OpUpdate, Schema: m.TableNamespace, Table: m.TableName,
			LSN: m.LSN.String(), Timestamp: m.MessageTime, Before: m.OldDecoded, After: m.NewDecoded,
		}}
	case *format.Delete:
		return []Event{{
			ID: changeID(m.LSN.String(), m.TableNamespace, m.TableName, OpDelete),
			Op: OpDelete, Schema: m.TableNamespace, Table: m.TableName,
			LSN: m.LSN.String(), Timestamp: m.MessageTime, Before: m.OldDecoded,
		}}
	case *format.Truncate:
		events := make([]Event, 0, len(m.Relations))
		for _, rel := range m.Relations {
			events = append(events, Event{
				Op: OpTruncate, Schema: rel.Namespace, Table: rel.Name, Timestamp: m.MessageTime,
			})
		}
		return events
	case *format.Snapshot:
		if m.EventType != format.SnapshotEventTypeData {
			return nil
		}
		return []Event{{
			Op: OpSnapshot, Schema: m.Schema, Table: m.Table,
			LSN: m.LSN.String(), Timestamp: m.ServerTime, After: m.Data,
		}}
	}
	return nil
}

func changeID(lsn, schema, table string, op Op) string {
	return lsn + "/" + schema + "." + table + "/" + string(op)
}
