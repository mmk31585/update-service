package operations

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
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
	log.Printf("DISPATCH_START operation_id=%s processor=%s", input.OperationID, input.Processor)
	op, err := uc.operations.GetByID(ctx, input.OperationID)
	if err != nil {
		log.Printf("DISPATCH: get operation failed: %v", err)
		return nil, err
	}
	if op == nil {
		log.Printf("DISPATCH: operation not found: %s", input.OperationID)
		return nil, ErrOperationNotFound
	}
	log.Printf("DISPATCH: operation found, loading operation details")
	log.Printf("DISPATCH_OPERATION_LOADED operation_id=%s status=%s stage=%s version=%d input_file_present=%t input_file_size=%d input_file_sha256=%s input_file_name=%s",
		op.ID, op.Status, op.Stage, op.Version, op.InputFile != nil, op.InputFile.SizeBytes, op.InputFile.SHA256, op.InputFile.FileName)

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

	// Add DEBUG snapshot before creating transfer
	log.Printf("DISPATCH_SNAPSHOT operation_id=%s job_id=%s attempt_id=%s status=%s stage=%s version=%d input_file_present=%t file_name=%s file_size=%d file_sha256=%s staging_path=%s staging_file_exists=%t worker_node_id=%s transfer_id=%s chunk_size=%d total_chunks=%d",
		op.ID, jobID, attemptID, op.Status, op.Stage, op.Version, op.InputFile != nil,
		op.InputFile.FileName, fileSize, fileSHA256, filepath.Join(uc.stagingDir, op.ID.String()), false, worker.NodeID, transferID, chunkSize, totalChunks)

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
  log.Printf("NATS_COMMAND_PUBLISHED subject=START_TRANSFER command_type=START_TRANSFER command_id=%s", commandID)

  if err := uc.publishFileChunks(ctx, op, transfer, worker.NodeID); err != nil {
    log.Printf("dispatch: publish chunks failed for %s: %v", op.ID, err)
  }

  log.Printf("PROCESS_UPDATE_SNAPSHOT node_id=%s operation_id=%s job_id=%s attempt_id=%s transfer_id=%s transfer_db_status=%s active_transfer=false received_chunks=0 expected_chunks=%d",
    worker.NodeID, op.ID, jobID, attemptID, transferID, op.Status, totalChunks)

  log.Printf("PROCESS_UPDATE_PUBLISH operation_id=%s job_id=%s attempt_id=%s transfer_id=%s worker_node_id=%s",
    op.ID, jobID, attemptID, transferID, worker.NodeID)

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

func (uc *DispatchOperationUseCase) publishFileChunks(ctx context.Context, op *operation.Operation, transfer *job.FileTransfer, targetNodeID string) error {
  ingressPath := filepath.Join(transfer.StagingPath, "input")
  log.Printf("CHUNK_SOURCE_OPEN operation_id=%s transfer_id=%s path=%s", op.ID, transfer.ID, ingressPath)
  file, err := os.Open(ingressPath)
  if err != nil {
    log.Printf("CHUNK_SOURCE_ERROR transfer_id=%s operation_id=%s path=%s step=open error=%v", transfer.ID, op.ID, ingressPath, err)
    return fmt.Errorf("open ingress file: %w", err)
  }
  defer file.Close()

  fileInfo, err := file.Stat()
  if err != nil {
    log.Printf("CHUNK_SOURCE_ERROR transfer_id=%s operation_id=%s path=%s step=stat error=%v", transfer.ID, op.ID, ingressPath, err)
    return fmt.Errorf("stat ingress file: %w", err)
  }
  actualFileSize := fileInfo.Size()
  log.Printf("CHUNK_SOURCE_OPENED operation_id=%s transfer_id=%s path=%s actual_file_size=%d", op.ID, transfer.ID, ingressPath, actualFileSize)

  chunkSize := transfer.ChunkSizeBytes
  totalChunks := transfer.TotalChunks
  chunkBuf := make([]byte, chunkSize)
  
  log.Printf("CHUNK_PUBLISH_START transfer_id=%s expected_total_chunks=%d chunk_size=%d file_size=%d", transfer.ID, totalChunks, chunkSize, actualFileSize)

  publishedChunks := 0
  totalBytes := 0
  for chunkIdx := 0; chunkIdx < totalChunks; chunkIdx++ {
    n, err := file.Read(chunkBuf)
    if err != nil && err != io.EOF {
      log.Printf("CHUNK_SOURCE_ERROR transfer_id=%s operation_id=%s path=%s step=read error=%v", transfer.ID, op.ID, ingressPath, err)
      return fmt.Errorf("read chunk %d: %w", chunkIdx, err)
    }
    if n == 0 {
      break
    }

    payload := chunkBuf[:n]
    chunkChecksum := fmt.Sprintf("%x", sha256.Sum256(payload))
    chunkID := fmt.Sprintf("%s:%d", transfer.ID, chunkIdx)

    chunk := &job.FileChunk{
      ID:           job.ChunkID(chunkID),
      TransferID:   transfer.ID,
      ChunkIndex:   chunkIdx,
      Offset:       int64(chunkIdx) * chunkSize,
      ChunkSize:    int64(n),
      Checksum:     chunkChecksum,
      PublishState: "published",
      AckState:     "pending",
      Version:      1,
    }
    if err := uc.chunks.Upsert(ctx, chunk); err != nil {
      log.Printf("CHUNK_SOURCE_ERROR transfer_id=%s operation_id=%s path=%s step=upsert error=%v", transfer.ID, op.ID, ingressPath, err)
      return fmt.Errorf("upsert chunk %d: %w", chunkIdx, err)
    }

    headers := map[string]string{
      "Nats-Msg-Id":      chunkID,
      "X-Chunk-Index":    fmt.Sprintf("%d", chunkIdx),
      "X-Offset":         fmt.Sprintf("%d", int64(chunkIdx)*chunkSize),
      "X-Payload-Length": fmt.Sprintf("%d", n),
      "X-Chunk-SHA256":   chunkChecksum,
      "X-Transfer-Id":    transfer.ID.String(),
    }

    subject := nats.TransferChunksSubject(transfer.ID.String())
    log.Printf("CHUNK_PUBLISH transfer_id=%s chunk_index=%d offset=%d chunk_size=%d subject=%s",
      transfer.ID, chunkIdx, int64(chunkIdx)*chunkSize, n, subject)
    if err := uc.nats.PublishWithHeaders(ctx, subject, payload, headers); err != nil {
      log.Printf("dispatch: publish chunk %d failed: %v", chunkIdx, err)
      log.Printf("CHUNK_SOURCE_ERROR transfer_id=%s operation_id=%s path=%s step=publish error=%v", transfer.ID, op.ID, ingressPath, err)
    }
    publishedChunks++
    totalBytes += n
  }

  if publishedChunks == 0 {
    log.Printf("CHUNK_PUBLISH_ZERO transfer_id=%s file_size=%d expected_total_chunks=%d path=%s", transfer.ID, actualFileSize, totalChunks, ingressPath)
  }

  log.Printf("CHUNK_PUBLISH_COMPLETE transfer_id=%s published_chunks=%d expected_chunks=%d total_bytes=%d",
    transfer.ID, publishedChunks, totalChunks, totalBytes)
  log.Printf("dispatch: published %d chunks for transfer %s", totalChunks, transfer.ID)
  return nil
}
