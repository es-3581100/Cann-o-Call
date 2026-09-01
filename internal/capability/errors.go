package capability

import "fmt"

// Error codes - normalized taxonomy for capability plane.
const (
	ErrUnknownCapability          = "unknown_capability"
	ErrCapabilityDisabled         = "capability_disabled"
	ErrCapabilityDenied           = "capability_denied"
	ErrInvalidInput               = "invalid_input"
	ErrTimeout                    = "timeout"
	ErrResourceLimit              = "resource_limit"
	ErrExecutionFailed            = "execution_failed"
	ErrNativeUnavailable          = "native_unavailable"
	ErrAdmissionRejected          = "admission_rejected"
	ErrDurableRecorderUnavailable = "durable_recorder_unavailable"
	ErrDurableAppendFailed        = "durable_append_failed"
)

// CapabilityError is typed error preserving code and cause.
type CapabilityError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Cause   error  `json:"-"`
}

func (e *CapabilityError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *CapabilityError) Unwrap() error { return e.Cause }

func NewError(code, message string, cause error) *CapabilityError {
	return &CapabilityError{Code: code, Message: message, Cause: cause}
}

func IsCode(err error, code string) bool {
	if ce, ok := err.(*CapabilityError); ok {
		return ce.Code == code
	}
	return false
}
