package operations

import (
	"context"
	"errors"
	"fmt"
	"time"

	"update/internal/application/ports"
	"update/internal/domain/event"
	"update/internal/domain/operation"
)

type CancelOperationInput struct {
	OperationID operation.OperationID
	Reason      string
}

type CancelOperationOutput struct {
	OperationID       operation.OperationID
	Status            operation.OperationStatus
	CancelRequestedAt time.Time
	StatusURL         string
	Version           int
}

var (
	ErrOperationNotFound = errors.New("operation not found")
	ErrAlreadyTerminal   = errors.New("operation is in terminal state, cancellation not applicable")
)

type CancelOperationUseCase struct {
	operations ports.OperationRepository
	events     ports.OperationEventRepository
	baseURL    string
}

func NewCancelOperationUseCase(
	operations ports.OperationRepository,
	events ports.OperationEventRepository,
	baseURL string,
) *CancelOperationUseCase {
	return &CancelOperationUseCase{
		operations: operations,
		events:     events,
		baseURL:    baseURL,
	}
}

func (uc *CancelOperationUseCase) Execute(ctx context.Context, input CancelOperationInput) (*CancelOperationOutput, error) {
	op, err := uc.operations.GetByID(ctx, input.OperationID)
	if err != nil {
		return nil, err
	}
	if op == nil {
		return nil, ErrOperationNotFound
	}

	// Check if already in terminal state
	switch op.Status {
	case operation.StatusCompleted, operation.StatusCancelled:
		return &CancelOperationOutput{
			OperationID: op.ID,
			Status:      op.Status,
			StatusURL:   uc.baseURL + "/api/v1/operations/" + op.ID.String(),
			Version:     op.Version,
		}, ErrAlreadyTerminal
	case operation.StatusFailed, operation.StatusTimeout:
		// Allow cancellation from failed/timeout states
	}

	now := time.Now().UTC()
	op.CancelRequestedAt = &now
	op.UpdatedAt = now
	op.Version++

	if err := uc.operations.Update(ctx, op); err != nil {
		return nil, err
	}

	// Emit cancellation event
	evt := &event.OperationEvent{
		ID:            event.EventID(fmt.Sprintf("evt-%x", time.Now().UnixNano())),
		OperationID:   op.ID.String(),
		EventType:     event.EventTypeOperationCancelled,
		Payload:       map[string]any{"reason": input.Reason},
		CorrelationID: op.ID.String(),
		IssuedAt:      now,
		ActorNodeID:   "self",
		Version:       op.Version,
	}
	_ = uc.events.Append(ctx, evt)

	return &CancelOperationOutput{
		OperationID:       op.ID,
		Status:            op.Status,
		CancelRequestedAt: now,
		StatusURL:         uc.baseURL + "/api/v1/operations/" + op.ID.String(),
		Version:           op.Version,
	}, nil
}
