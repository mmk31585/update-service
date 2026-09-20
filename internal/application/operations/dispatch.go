package operations

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"update/internal/application/ports"
	"update/internal/application/scheduling"
	"update/internal/domain/event"
	"update/internal/domain/job"
	"update/internal/domain/operation"

	"update/internal/infrastructure/nats"
)

type DispatchOperationInput struct {
	OperationID operation.OperationID
	Processor   string
}

type DispatchOperationOutput struct {
	OperationID  operation.OperationID
	Status       operation.OperationStatus
	JobID        operation.JobID
	AttemptID    job.AttemptID
	WorkerNodeID string
	StatusURL    string
	UpdatedAt    time.Time
	Version      int
}

var ErrNoHealthyWorker = scheduling.ErrNoHealthyWorker

type DispatchOperationUseCase struct {
	operations ports.OperationRepository
	jobs       ports.JobRepository
	attempts   ports.JobAttemptRepository
	events     ports.OperationEventRepository
	nats       *nats.Client
	scheduler  *scheduling.Scheduler
	baseURL    string
	transfers  ports.FileTransferRepository
	chunks     ports.FileChunkRepository
	stagingDir string
}

func NewDispatchOperationUseCase(
	operations ports.OperationRepository,
	jobs ports.JobRepository,
	attempts ports.JobAttemptRepository,
	events ports.OperationEventRepository,
	natsClient *nats.Client,
	scheduler *scheduling.Scheduler,
	baseURL string,
	transfers ports.FileTransferRepository,
	chunks ports.FileChunkRepository,
	stagingDir string,
) *DispatchOperationUseCase {
	return &DispatchOperationUseCase{
		operations: operations,
		jobs:       jobs,
		attempts:   attempts,
		events:     events,
		nats:       natsClient,
		scheduler:  scheduler,
		baseURL:    baseURL,
		transfers:  transfers,
		chunks:     chunks,
		stagingDir: stagingDir,
	}
}

func (uc *DispatchOperationUseCase) Execute(ctx context.Context, input DispatchOperationInput) (*DispatchOperationOutput, error) {
	log.Printf("dispatch: starting for operation %s", input.OperationID)
	op, err := uc.operations.GetByID(ctx, input.OperationID)
	if err != nil {
		log.Printf("dispatch: get operation failed: %v", err)
		return nil, err
	}
	if op == nil {
		log.Printf("dispatch: operation not found: %s", input.OperationID)
		return nil, ErrOperationNotFound
	}
	log.Printf("dispatch: operation found, selecting worker")

	worker, err := uc.scheduler.SelectWorker(ctx)
	if err != nil {
		log.Printf("dispatch: select worker failed: %v", err)
		return nil, err
	}
	log.Printf("dispatch: selected worker %s", worker.NodeID)

	now := time.Now().UTC()
	jobID := operation.JobID(generateID("job"))
	attemptID := job.AttemptID(generateID("attempt"))
	commandID := fmt.Sprintf("cmd-%s", generateID("")[4:])

	newJob := &job.Job{
		ID:           job.JobID(jobID.String()),
		OperationID:  op.ID.String(),
		WorkerNodeID: worker.NodeID,
		Status:       job.JobStatusAssigned,
		Processor:    input.Processor,
		AssignedAt:   now,
		Version:      1,
	}

	newAttempt := &job.JobAttempt{
		ID:            attemptID,
		JobID:         job.JobID(jobID.String()),
		OperationID:   op.ID.String(),
		NodeID:        worker.NodeID,
		AttemptNumber: op.RetryCount + 1,
		Status:        job.AttemptStatusPending,
		CommandID:     commandID,
		ClaimToken:    fmt.Sprintf("claim-%s", generateID("")[4:]),
		LeaseUntil:    now.Add(5 * time.Minute),
		Version:       1,
	}

	transferID := job.TransferID(fmt.Sprintf("transfer-%s-%d", op.ID.String(), now.UnixNano()))
	fileID := fmt.Sprintf("file-%s", op.ID.String())
	fileSize := int64(0)
	fileSHA256 := ""
	if op.InputFile != nil {
		fileSize = op.InputFile.SizeBytes
		fileSHA256 = op.InputFile.SHA256
	}
	chunkSize := int64(64 * 1024)
	totalChunks := int((fileSize + chunkSize - 1) / chunkSize)
	deadlineAt := now.Add(30 * time.Minute)

	transfer := &job.FileTransfer{
		ID:                transferID,
		AttemptID:         attemptID,
		OperationID:       op.ID.String(),
		FileID:            fileID,
		SourceNodeID:      "self",
		DestinationNodeID: worker.NodeID,
		FileSizeBytes:     fileSize,
		ChunkSizeBytes:    chunkSize,
		TotalChunks:       totalChunks,
		FileChecksum:      fileSHA256,
		ChecksumAlgorithm: "sha256",
		Status:            job.TransferStatusCreated,
		StagingPath:       filepath.Join(uc.stagingDir, op.ID.String()),
		DeadlineAt:        deadlineAt,
		Version:           1,
	}

	if err := uc.transfers.Create(ctx, transfer); err != nil {
		return nil, fmt.Errorf("create transfer: %w", err)
	}

	startTransferPayload := map[string]any{
		"command_id":   commandID,
		"operation_id": op.ID.String(),
		"job_id":       jobID.String(),
		"type":         "START_TRANSFER",
		"payload": map[string]any{
			"attempt_id":           attemptID.String(),
			"transfer_id":          transferID.String(),
			"file_id":              fileID,
			"source_node_id":       "self",
			"destination_node_id":  worker.NodeID,
			"file_size_bytes":      fileSize,
			"chunk_size_bytes":     chunkSize,
			"total_chunks":         totalChunks,
			"file_sha256":          fileSHA256,
			"checksum_algorithm":   "sha256",
			"transfer_deadline_at": deadlineAt.Format(time.RFC3339),
		},
	}
	startTransferBytes, err := json.Marshal(startTransferPayload)
	if err != nil {
		return nil, fmt.Errorf("marshal start transfer: %w", err)
	}

	if err := uc.nats.PublishCommand(ctx, worker.NodeID, "START_TRANSFER", startTransferBytes); err != nil {
		return nil, fmt.Errorf("publish start transfer: %w", err)
	}

	payload := map[string]any{
		"command_id":   commandID,
		"operation_id": op.ID.String(),
		"job_id":       jobID.String(),
		"type":         "PROCESS_UPDATE",
		"payload": map[string]any{
			"attempt_id": attemptID.String(),
			"processor":  input.Processor,
		},
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}

	if err := uc.nats.PublishCommand(ctx, worker.NodeID, "PROCESS_UPDATE", payloadBytes); err != nil {
		return nil, fmt.Errorf("publish command: %w", err)
	}

	now = time.Now().UTC()
	newJob.Status = job.JobStatusDispatched
	newJob.DispatchedAt = &now
	newAttempt.Status = job.AttemptStatusCommandSent

	if err := uc.jobs.Create(ctx, &job.Job{
		ID:           job.JobID(jobID.String()),
		OperationID:  op.ID.String(),
		WorkerNodeID: worker.NodeID,
		Status:       newJob.Status,
		Processor:    input.Processor,
		AssignedAt:   newJob.AssignedAt,
		DispatchedAt: newJob.DispatchedAt,
		Version:      1,
	}); err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}

	if err := uc.attempts.Create(ctx, newAttempt); err != nil {
		return nil, fmt.Errorf("create attempt: %w", err)
	}

	op.Status = operation.StatusDispatched
	op.Stage = operation.StageWaitingForAck
	op.UpdatedAt = now
	op.CurrentJobID = (*operation.JobID)(&jobID)
	op.CurrentAttemptID = (*operation.AttemptID)(&attemptID)
	op.Version++

	if err := uc.operations.Update(ctx, op); err != nil {
		return nil, fmt.Errorf("update operation: %w", err)
	}

	evt := &event.OperationEvent{
		ID:            event.EventID(generateID("evt")),
		OperationID:   op.ID.String(),
		EventType:     event.EventTypeOperationDispatched,
		Payload:       map[string]any{"job_id": jobID.String(), "attempt_id": attemptID.String(), "worker_node_id": worker.NodeID, "transfer_id": transferID.String()},
		CorrelationID: op.ID.String(),
		IssuedAt:      now,
		ActorNodeID:   "self",
		Version:       1,
	}
	_ = uc.events.Append(ctx, evt)

	return &DispatchOperationOutput{
		OperationID:  op.ID,
		Status:       op.Status,
		JobID:        jobID,
		AttemptID:    attemptID,
		WorkerNodeID: worker.NodeID,
		StatusURL:    uc.baseURL + "/api/v1/operations/" + op.ID.String(),
		UpdatedAt:    now,
		Version:      op.Version,
	}, nil
}
