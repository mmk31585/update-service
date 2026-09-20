package operation

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidTransition = errors.New("invalid operation status transition")
	ErrVersionMismatch   = errors.New("version mismatch: operation was modified concurrently")
	ErrInvalidStateForOp = errors.New("operation not in valid state for this action")
)

type TransitionGuard struct{}

func NewTransitionGuard() *TransitionGuard {
	return &TransitionGuard{}
}

func (g *TransitionGuard) CanTransition(from, to OperationStatus, isRetry bool) error {
	if from == to {
		return nil
	}

	if from == StatusCompleted || from == StatusCancelled {
		return fmt.Errorf("%w: %s is terminal", ErrInvalidTransition, from)
	}

	if isRetry {
		if from != StatusFailed && from != StatusTimeout {
			return fmt.Errorf("%w: retry only allowed from FAILED or TIMEOUT, not %s", ErrInvalidStateForOp, from)
		}
		return g.canTransition(from, to)
	}

	return g.canTransition(from, to)
}

func (g *TransitionGuard) canTransition(from, to OperationStatus) error {
	valid := map[OperationStatus][]OperationStatus{
		StatusCreated:      {StatusQueued, StatusCancelled},
		StatusQueued:       {StatusDispatched, StatusCancelled},
		StatusDispatched:   {StatusTransferring, StatusTimeout, StatusFailed, StatusCancelled},
		StatusTransferring: {StatusTransferred, StatusTimeout, StatusFailed, StatusCancelled},
		StatusTransferred:  {StatusProcessing, StatusTimeout, StatusCancelled},
		StatusProcessing:   {StatusCompleted, StatusFailed, StatusTimeout, StatusCancelled},
		StatusFailed:       {StatusQueued, StatusFailed, StatusTimeout, StatusCancelled},
		StatusTimeout:      {StatusQueued, StatusFailed, StatusTimeout, StatusCancelled},
	}

	allowed, ok := valid[from]
	if !ok {
		return fmt.Errorf("%w: unknown status %s", ErrInvalidTransition, from)
	}

	for _, s := range allowed {
		if s == to {
			return nil
		}
	}

	return fmt.Errorf("%w: %s -> %s not allowed", ErrInvalidTransition, from, to)
}

func (g *TransitionGuard) ValidateVersion(current, expected int) error {
	if current != expected {
		return ErrVersionMismatch
	}
	return nil
}
