// Package cerror provides the external error taxonomy. Underlying driver /
// library errors are never leaked: they are wrapped as a classified *Error.
package cerror

// Class is the coarse error category used for metrics and logs.
type Class string

const (
	// ClassConnector invalid connector/config usage (registry, lifecycle).
	ClassConnector Class = "connector_error"
	// ClassRecord a source record failed normalization/validation.
	ClassRecord Class = "record_error"
	// ClassOffset an invalid, stale or mismatched checkpoint offset.
	ClassOffset Class = "offset_error"
	// ClassCheckpoint checkpoint storage failure.
	ClassCheckpoint Class = "checkpoint_error"
	// ClassSink a downstream delivery failure.
	ClassSink Class = "sink_error"
	// ClassInternal an unexpected internal failure.
	ClassInternal Class = "internal_error"
)

// Kind refines a Class with a stable, machine-readable reason so callers can
// distinguish e.g. a duplicate registration from a missing field.
type Kind string

const (
	KindDuplicateRegister  Kind = "duplicate_connector_registered"
	KindConnectorNotFound  Kind = "connector_not_found"
	KindAlreadyInitialized Kind = "already_initialized"
	KindNotInitialized     Kind = "not_initialized"
	KindInvalidConfig      Kind = "invalid_config"

	KindMissingField    Kind = "missing_field"
	KindTypeMismatch    Kind = "type_mismatch"
	KindMalformedRecord Kind = "malformed_record"

	KindOffsetStale      Kind = "offset_stale"
	KindOffsetOutOfRange Kind = "offset_out_of_range"
	KindOffsetMismatch   Kind = "offset_source_mismatch"
	KindOffsetBadFormat  Kind = "offset_bad_format"

	KindCheckpointCorrupt Kind = "checkpoint_corrupt"
	KindSinkDelivery      Kind = "sink_delivery_failed"
	KindInternal          Kind = "internal"
)

// Error is the only error type exposed at framework boundaries.
type Error struct {
	Class Class
	Kind  Kind
	Msg   string
	Cause error
}

func (e *Error) Error() string {
	s := string(e.Class) + "/" + string(e.Kind) + ": " + e.Msg
	if e.Cause != nil {
		s += " (cause: " + e.Cause.Error() + ")"
	}
	return s
}

func (e *Error) Unwrap() error { return e.Cause }

// New builds a classified error.
func New(class Class, kind Kind, msg string) *Error {
	return &Error{Class: class, Kind: kind, Msg: msg}
}

// Wrap builds a classified error carrying the underlying cause. The cause is
// rendered in Error() but callers should branch on Class/Kind rather than on
// the wrapped driver error.
func Wrap(class Class, kind Kind, msg string, cause error) *Error {
	return &Error{Class: class, Kind: kind, Msg: msg, Cause: cause}
}

// IsClass reports whether err (or any wrapped error) belongs to class.
func IsClass(err error, class Class) bool {
	ce, ok := As(err)
	return ok && ce.Class == class
}

// As extracts a classified *Error from err.
func As(err error) (*Error, bool) {
	for err != nil {
		if ce, ok := err.(*Error); ok {
			return ce, true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil, false
		}
		err = u.Unwrap()
	}
	return nil, false
}
