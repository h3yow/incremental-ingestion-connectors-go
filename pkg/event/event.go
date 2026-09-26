// Package event defines the normalized internal event shape produced by all
// connectors and consumed by sinks.
package event

// Event is the connector-independent representation of one source record.
type Event struct {
	// Key is the business identity of the record. It is the idempotency key
	// used by the sink and must be non-empty after normalization.
	Key string

	// Offset is the connector-encoded source position of this record.
	// It is opaque to the engine but must be monotonically non-decreasing
	// within one source.
	Offset string

	// Payload holds the normalized field values (validated, typed values).
	Payload map[string]any

	// Source is the source tag the event originated from.
	Source string
}

// Clone returns a deep copy of the event so workers can mutate payloads
// without sharing memory.
func (e Event) Clone() Event {
	var p map[string]any
	if e.Payload != nil {
		p = make(map[string]any, len(e.Payload))
		for k, v := range e.Payload {
			p[k] = v
		}
	}
	return Event{Key: e.Key, Offset: e.Offset, Payload: p, Source: e.Source}
}
