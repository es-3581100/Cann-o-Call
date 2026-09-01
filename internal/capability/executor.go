package capability

import (
	"context"
)

// Executor is the stable Go interface for capability execution.
// Policy is ABOVE executors: executor must not decide actor depth/budget, transition admission, Rust durability, or projection mutation.
type Executor interface {
	Describe() Descriptor
	Execute(ctx context.Context, req Request) (Result, error)
}
