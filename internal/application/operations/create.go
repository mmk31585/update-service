package operations

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"update/internal/application/ports"
	"update/internal/domain/event"
	"update/internal/domain/operation"
)

type CreateOperationInput struct {
	Processor       string
	Options         map[string]any
	Metadata        map[string]any
	Deadline        *time.Time
	RetryPolicy     *operation.RetryPolicy
	ClientReference string
}

type CreateOperationOutput struct {
	OperationID  operation.OperationID
	Status       operation.OperationStatus
	Stage        operation.OperationStage
	UploadTarget operation.UploadTarget
	StatusURL    string
	ResultURL    string
	CreatedAt    time.Time
	Version      int
}

type CreateOperationUseCase struct {
	operations  ports.OperationRepository
	idempotency ports.IdempotencyRepository
	events      ports.OperationEventRepository
	baseURL     string
}

func NewCreateOperationUseCase(
	operations ports.OperationRepository,
	idempotency ports.IdempotencyRepository,
	events ports.OperationEventRepository,
	baseURL string,
) *CreateOperationUseCase {
	return &CreateOperationUseCase{
		operations:  operations,
		idempotency: idempotency,
		events:      events,
		baseURL:     baseURL,
	}
}

func (uc *CreateOperationUseCase) Execute(ctx context.Context, input CreateOperationInput, idempotencyKey, method, path string) (*CreateOperationOutput, error) {
	now := time.Now().UTC()

	maxAttempts := 3
	if input.RetryPolicy != nil && input.RetryPolicy.MaxAttempts > 0 {
		maxAttempts = input.RetryPolicy.MaxAttempts
	}

	opID := operation.OperationID(generateID("op"))
	op := &operation.Operation{
		ID:              opID,
		Status:          operation.StatusCreated,
		Stage:           operation.StageWaitingForUpload,
		Processor:       input.Processor,
		RetryCount:      0,
		MaxAttempts:     maxAttempts,
		ClientReference: input.ClientReference,
		CreatedAt:       now,
		UpdatedAt:       now,
		Deadline:        input.Deadline,
		Version:         1,
	}

	if err := uc.operations.Create(ctx, op); err != nil {
		return nil, err
	}

	uploadTarget := operation.UploadTarget{
		Method: "PUT",
		URL:    uc.baseURL + "/api/v1/operations/" + opID.String() + "/file",
		Headers: map[string]string{
			"Content-Type": "application/octet-stream",
		},
		Expires:  now.Add(1 * time.Hour),
		MaxBytes: 1 << 30,
	}

	evt := &event.OperationEvent{
		ID:            event.EventID(generateID("evt")),
		OperationID:   opID.String(),
		EventType:     event.EventTypeOperationCreated,
		Payload:       map[string]any{"processor": input.Processor, "max_attempts": maxAttempts},
		CorrelationID: opID.String(),
		IssuedAt:      now,
		ActorNodeID:   "self",
		Version:       1,
	}
	_ = uc.events.Append(ctx, evt)

	return &CreateOperationOutput{
		OperationID:  opID,
		Status:       operation.StatusCreated,
		Stage:        operation.StageWaitingForUpload,
		UploadTarget: uploadTarget,
		StatusURL:    uc.baseURL + "/api/v1/operations/" + opID.String(),
		ResultURL:    uc.baseURL + "/api/v1/operations/" + opID.String() + "/result",
		CreatedAt:    now,
		Version:      1,
	}, nil
}

func generateID(prefix string) string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return prefix + "-" + hex.EncodeToString(buf)
}
