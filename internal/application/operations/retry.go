package operations

import (
	"context"
	"errors"
	"fmt"
	"time"

	"update/internal/application/ports"
	"update/internal/domain/event"
	"update/internal/domain/job"
	"update/internal/domain/operation"
)

type RetryOperationInput struct {
	OperationID operation.OperationID
	Reason      string
}

type RetryOperationOutput struct {
	OperationID      operation.OperationID
	Status           operation.OperationStatus
	RetryCount       int
	MaxAttempts      int
	CurrentJobID     *operation.JobID
	CurrentAttemptID *operation.AttemptID
	StatusURL        string
	UpdatedAt        time.Time
	Version          int
}

var (
	ErrInvalidStateForRetry = errors.New("operation not in FAILED or TIMEOUT state, or retry not allowed")
)

type RetryOperationUseCase struct {
	operations ports.OperationRepository
	jobs       ports.JobRepository
	attempts   ports.JobAttemptRepository
	events     ports.OperationEventRepository
	baseURL    string
}

func NewRetryOperationUseCase(
	operations ports.OperationRepository,
	jobs ports.JobRepository,
	attempts ports.JobAttemptRepository,
	events ports.OperationEventRepository,
	baseURL string,
) *RetryOperationUseCase {
	return &RetryOperationUseCase{
		operations: operations,
		jobs:       jobs,
		attempts:   attempts,
		events:     events,
		baseURL:    baseURL,
	}
}

func (uc *RetryOperationUseCase) Execute(ctx context.Context, input RetryOperationInput) (*RetryOperationOutput, error) {
	op, err := uc.operations.GetByID(ctx, input.OperationID)
	if err != nil {
		return nil, err
	}
	if op == nil {
		return nil, ErrOperationNotFound
	}

	// Check if retry is allowed (only from FAILED or TIMEOUT)
	if op.Status != operation.StatusFailed && op.Status != operation.StatusTimeout {
		return nil, ErrInvalidStateForRetry
	}

	// Check retry budget
	if op.RetryCount >= op.MaxAttempts {
		return nil, ErrInvalidStateForRetry
	}

	now := time.Now().UTC()
	op.RetryCount++
	op.Status = operation.StatusQueued
	op.Stage = operation.StageQueued
	op.UpdatedAt = now
	op.Version++
	// Clear current execution references
	op.CurrentJobID = nil
	op.CurrentAttemptID = nil
	op.CurrentTransferID = nil
	op.CancelRequestedAt = nil
	op.Error = nil

	if err := uc.operations.Update(ctx, op); err != nil {
		return nil, err
	}

	// Create new job and attempt
	jobIDStr := fmt.Sprintf("job-%x", time.Now().UnixNano())
	attemptIDStr := fmt.Sprintf("attempt-%x", time.Now().UnixNano())
	jobID := operation.JobID(jobIDStr)
	attemptID := operation.AttemptID(attemptIDStr)

	newJob := &job.Job{
		ID:          job.JobID(jobIDStr),
		OperationID: op.ID.String(),
		Status:      job.JobStatusCreated,
		Processor:   op.Processor,
		AssignedAt:  now,
		Version:     1,
	}
	newAttempt := &job.JobAttempt{
		ID:            job.AttemptID(attemptIDStr),
		JobID:         job.JobID(jobIDStr),
		OperationID:   op.ID.String(),
		AttemptNumber: op.RetryCount,
		Status:        job.AttemptStatusPending,
		CommandID:     fmt.Sprintf("cmd-%x", time.Now().UnixNano()),
		ClaimToken:    fmt.Sprintf("claim-%x", time.Now().UnixNano()),
		LeaseUntil:    now.Add(5 * time.Minute),
		Version:       1,
	}

	// Persist job and attempt
	if err := uc.jobs.Create(ctx, newJob); err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}
	if err := uc.attempts.Create(ctx, newAttempt); err != nil {
		return nil, fmt.Errorf("create attempt: %w", err)
	}

	// Update operation with new job/attempt
	op.CurrentJobID = &jobID
	op.CurrentAttemptID = &attemptID
	op.Version++
	if err := uc.operations.Update(ctx, op); err != nil {
		return nil, err
	}

	// Emit retry event
	evt := &event.OperationEvent{
		ID:            event.EventID(fmt.Sprintf("evt-%x", time.Now().UnixNano())),
		OperationID:   op.ID.String(),
		EventType:     event.EventTypeOperationRetryScheduled,
		Payload:       map[string]any{"retry_count": op.RetryCount, "new_job_id": jobIDStr, "new_attempt_id": attemptIDStr, "reason": input.Reason},
		CorrelationID: op.ID.String(),
		IssuedAt:      now,
		ActorNodeID:   "self",
		Version:       op.Version,
	}
	_ = uc.events.Append(ctx, evt)

	return &RetryOperationOutput{
		OperationID:      op.ID,
		Status:           op.Status,
		RetryCount:       op.RetryCount,
		MaxAttempts:      op.MaxAttempts,
		CurrentJobID:     &jobID,
		CurrentAttemptID: &attemptID,
		StatusURL:        uc.baseURL + "/api/v1/operations/" + op.ID.String(),
		UpdatedAt:        op.UpdatedAt,
		Version:          op.Version,
	}, nil
}
