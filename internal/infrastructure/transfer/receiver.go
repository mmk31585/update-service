package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"update/internal/application/ports"
	"update/internal/domain/event"
	"update/internal/domain/job"
	"update/internal/infrastructure/nats"

	"github.com/nats-io/nats.go/jetstream"
)

type TransferReceiver struct {
	operations ports.OperationRepository
	transfers  ports.FileTransferRepository
	chunks     ports.FileChunkRepository
	events     ports.OperationEventRepository
	nats       *nats.Client
	stagingDir string
	nodeID     string
}

type ActiveTransfer struct {
	transfer     *job.FileTransfer
	totalChunks  int
	totalBytes   int64
	fileSHA256   string
	received     map[int]*job.FileChunk
	receivedSize int64
	file         *os.File
	manifestPath string
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewTransferReceiver(
	operations ports.OperationRepository,
	transfers ports.FileTransferRepository,
	chunks ports.FileChunkRepository,
	events ports.OperationEventRepository,
	natsClient *nats.Client,
	stagingDir string,
	nodeID string,
) *TransferReceiver {
	return &TransferReceiver{
		operations: operations,
		transfers:  transfers,
		chunks:     chunks,
		events:     events,
		nats:       natsClient,
		stagingDir: stagingDir,
		nodeID:     nodeID,
	}
}

func (r *TransferReceiver) AcceptTransfer(ctx context.Context, cmd map[string]any) (*ActiveTransfer, error) {
	operationID, _ := cmd["operation_id"].(string)
	transferIDStr, _ := cmd["transfer_id"].(string)
	fileSize, _ := cmd["file_size_bytes"].(float64)
	totalChunks, _ := cmd["total_chunks"].(float64)
	fileSHA256, _ := cmd["file_sha256"].(string)
	transferDeadlineStr, _ := cmd["transfer_deadline_at"].(string)

	transferID := job.TransferID(transferIDStr)

	transfer, err := r.transfers.GetByID(ctx, transferID)
	if err != nil {
		return nil, fmt.Errorf("get transfer: %w", err)
	}
	if transfer == nil {
		return nil, fmt.Errorf("transfer not found: %s", transferID)
	}

	if transfer.DestinationNodeID != r.nodeID {
		return nil, fmt.Errorf("transfer destination mismatch: got %s want %s", transfer.DestinationNodeID, r.nodeID)
	}

	stagingPath := filepath.Join(r.stagingDir, operationID, transferIDStr)
	if err := os.MkdirAll(stagingPath, 0755); err != nil {
		return nil, fmt.Errorf("create staging dir: %w", err)
	}

	finalPath := filepath.Join(stagingPath, "final")
	f, err := os.OpenFile(finalPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("create staging file: %w", err)
	}

	if fileSize > 0 {
		if err := f.Truncate(int64(fileSize)); err != nil {
			f.Close()
			return nil, fmt.Errorf("preallocate staging file: %w", err)
		}
	}

manifestPath := filepath.Join(stagingPath, "manifest.json")
  now := time.Now().UTC()
  deadlineAt, _ := time.Parse(time.RFC3339, transferDeadlineStr)
  if deadlineAt.IsZero() {
    deadlineAt = now.Add(30 * time.Minute)
  }

  log.Printf("TRANSFER_ACCEPT_METADATA transfer_id=%s file_size_bytes=%d total_chunks=%d file_sha256=%s",
    transferID, int64(fileSize), int(totalChunks), fileSHA256)

  transfer.Status = job.TransferStatusAccepted
	transfer.AcceptedAt = &now
	if err := r.transfers.Update(ctx, transfer); err != nil {
		f.Close()
		return nil, fmt.Errorf("update transfer accepted: %w", err)
	}

	childCtx, cancel := context.WithCancel(ctx)
	active := &ActiveTransfer{
		transfer:     transfer,
		totalChunks:  int(totalChunks),
		totalBytes:   int64(fileSize),
		fileSHA256:   fileSHA256,
		received:     make(map[int]*job.FileChunk),
		receivedSize: 0,
		file:         f,
		manifestPath: manifestPath,
		ctx:          childCtx,
		cancel:       cancel,
	}

consumerName := fmt.Sprintf("chunks-%s", transferID)
  chunkSubject := nats.TransferChunksSubject(transferIDStr)
  log.Printf("TRANSFER_CONSUMER_CREATE_START transfer_id=%s subject=%s", transferID, chunkSubject)
  _, err = r.nats.Subscribe(childCtx, chunkSubject, consumerName, func(msg jetstream.Msg) {
    r.handleChunk(childCtx, active, msg)
    _ = msg.Ack()
  })
  if err != nil {
    cancel()
    f.Close()
    return nil, fmt.Errorf("subscribe chunks: %w", err)
  }
  log.Printf("TRANSFER_CONSUMER_CREATED transfer_id=%s consumer_name=%s subject=%s", transferID, consumerName, chunkSubject)
  log.Printf("TRANSFER_ACTIVE transfer_id=%s total_chunks=%d", transferID, active.totalChunks)
  log.Printf("TRANSFER_ACCEPTED transfer_id=%s operation_id=%s", transferID, operationID)
  log.Printf("TRANSFER_ACCEPT_START node_id=%s transfer_id=%s operation_id=%s", r.nodeID, transferID, operationID)

  if active.totalChunks == 0 {
    log.Printf("TRANSFER_ZERO_CHUNKS transfer_id=%s file_size_bytes=%d file_sha256=%s total_chunks=%d", transferID, int64(fileSize), fileSHA256, int(totalChunks))
    log.Printf("transfer receiver: transfer %s has no chunks, finalizing immediately", transferID)
    if err := r.FinalizeTransfer(ctx, active, transferIDStr); err != nil {
      log.Printf("transfer receiver: finalize failed for empty transfer: %v", err)
    }
  }

	return active, nil
}

func (r *TransferReceiver) handleChunk(ctx context.Context, active *ActiveTransfer, msg jetstream.Msg) {
	headers := msg.Headers()
	chunkIndexStr := headers.Get("X-Chunk-Index")
	if chunkIndexStr == "" {
		log.Printf("transfer receiver: missing chunk index header")
		return
	}
	var chunkIndex int
	if _, err := fmt.Sscanf(chunkIndexStr, "%d", &chunkIndex); err != nil {
		log.Printf("transfer receiver: invalid chunk index: %s", chunkIndexStr)
		return
	}

	offsetStr := headers.Get("X-Offset")
	expectedLenStr := headers.Get("X-Payload-Length")
	chunkSHA256 := headers.Get("X-Chunk-SHA256")
	transferIDStr := headers.Get("X-Transfer-Id")

	expectedLen := 0
	if expectedLenStr != "" {
		_, _ = fmt.Sscanf(expectedLenStr, "%d", &expectedLen)
	}

	payload := msg.Data()
	if len(payload) != expectedLen {
		log.Printf("transfer receiver: chunk %d payload length mismatch: got %d want %d", chunkIndex, len(payload), expectedLen)
		return
	}

	sum := sha256.Sum256(payload)
	computedChecksum := hex.EncodeToString(sum[:])
	if computedChecksum != chunkSHA256 {
		log.Printf("transfer receiver: chunk %d checksum mismatch: got %s want %s", chunkIndex, computedChecksum, chunkSHA256)
		return
	}

active.mu.Lock()
  if active.file == nil {
    active.mu.Unlock()
    return
  }

  if existing, ok := active.received[chunkIndex]; ok {
    if existing.Checksum == chunkSHA256 {
      active.mu.Unlock()
      return
    }
    active.mu.Unlock()
    log.Printf("transfer receiver: chunk %d checksum conflict", chunkIndex)
    return
  }

  offset := int64(0)
  if offsetStr != "" {
    _, _ = fmt.Sscanf(offsetStr, "%d", &offset)
  }

  if _, err := active.file.WriteAt(payload, offset); err != nil {
    active.mu.Unlock()
    log.Printf("transfer receiver: write chunk %d: %v", chunkIndex, err)
    return
  }

  chunk := &job.FileChunk{
    ID:           job.ChunkID(fmt.Sprintf("%s:%d", transferIDStr, chunkIndex)),
    TransferID:   job.TransferID(transferIDStr),
    ChunkIndex:   chunkIndex,
    Offset:       offset,
    ChunkSize:    int64(len(payload)),
    Checksum:     chunkSHA256,
    PublishState: "received",
    AckState:     "acknowledged",
    Version:      1,
  }
  active.received[chunkIndex] = chunk
  active.receivedSize += int64(len(payload))

  receivedCount := len(active.received)
  isComplete := receivedCount == active.totalChunks
  
  log.Printf("CHUNK_RECEIVED node_id=%s transfer_id=%s chunk_index=%d chunk_size=%d expected_total_chunks=%d",
    r.nodeID, transferIDStr, chunkIndex, len(payload), active.totalChunks)
  log.Printf("CHUNK_STORED transfer_id=%s chunk_index=%d bytes_written=%d",
    transferIDStr, chunkIndex, len(payload))
  log.Printf("TRANSFER_PROGRESS transfer_id=%s received_chunks=%d expected_chunks=%d received_bytes=%d expected_bytes=%d",
    transferIDStr, receivedCount, active.totalChunks, active.receivedSize, active.totalBytes)

  active.mu.Unlock()

	_ = r.chunks.Upsert(ctx, chunk)

	evt := &event.OperationEvent{
		ID:            event.EventID(fmt.Sprintf("evt-chunk-%s-%d-%d", transferIDStr, chunkIndex, time.Now().UnixNano())),
		OperationID:   active.transfer.OperationID,
		EventType:     event.EventTypeChunkAccepted,
		Payload:       map[string]any{"chunk_index": chunkIndex, "offset": offset, "bytes": len(payload)},
		JobID:         (*string)(&active.transfer.ID),
		AttemptID:     (*string)(&active.transfer.ID),
		TransferID:    (*string)(&active.transfer.ID),
		CorrelationID: active.transfer.OperationID,
		IssuedAt:      time.Now().UTC(),
		ActorNodeID:   r.nodeID,
		Version:       1,
	}
	_ = r.events.Append(ctx, evt)

	eventPayload, _ := json.Marshal(evt)
	_ = r.nats.PublishEvent(ctx, string(evt.EventType), eventPayload)

	log.Printf("transfer receiver: chunk %d accepted (%d/%d)", chunkIndex, receivedCount, active.totalChunks)

if isComplete {
    log.Printf("TRANSFER_ALL_CHUNKS_RECEIVED transfer_id=%s received_chunks=%d expected_chunks=%d received_bytes=%d",
      transferIDStr, receivedCount, active.totalChunks, active.receivedSize)
    if err := r.FinalizeTransfer(ctx, active, transferIDStr); err != nil {
      log.Printf("transfer receiver: finalize failed: %v", err)
    }
  }
}

func (r *TransferReceiver) FinalizeTransfer(ctx context.Context, active *ActiveTransfer, transferIDStr string) error {
  active.mu.Lock()
  defer active.mu.Unlock()

  log.Printf("TRANSFER_FINALIZE_START transfer_id=%s operation_id=%s received_chunks=%d expected_chunks=%d received_bytes=%d expected_bytes=%d expected_checksum=%s",
    transferIDStr, active.transfer.OperationID, active.totalChunks, active.totalChunks, active.receivedSize, active.totalBytes, active.fileSHA256)

  if err := active.file.Sync(); err != nil {
    log.Printf("TRANSFER_FINALIZE_ERROR transfer_id=%s operation_id=%s error=%v", transferIDStr, active.transfer.OperationID, err)
    return fmt.Errorf("sync staging file: %w", err)
  }

  stat, err := active.file.Stat()
  if err != nil {
    log.Printf("TRANSFER_FINALIZE_ERROR transfer_id=%s operation_id=%s error=%v", transferIDStr, active.transfer.OperationID, err)
    return fmt.Errorf("stat staging file: %w", err)
  }

  if stat.Size() != active.totalBytes {
    log.Printf("TRANSFER_FINALIZE_ERROR transfer_id=%s operation_id=%s error=%v", transferIDStr, active.transfer.OperationID, fmt.Errorf("final file size mismatch: got %d want %d", stat.Size(), active.totalBytes))
    return fmt.Errorf("final file size mismatch: got %d want %d", stat.Size(), active.totalBytes)
  }

  if _, err := active.file.Seek(0, io.SeekStart); err != nil {
    log.Printf("TRANSFER_FINALIZE_ERROR transfer_id=%s operation_id=%s error=%v", transferIDStr, active.transfer.OperationID, err)
    return fmt.Errorf("seek to start: %w", err)
  }

  log.Printf("TRANSFER_CHECKSUM_CALC_START transfer_id=%s staged_file_path=%s", transferIDStr, active.file.Name())

  h := sha256.New()
  if _, err := io.Copy(h, active.file); err != nil {
    log.Printf("TRANSFER_FINALIZE_ERROR transfer_id=%s operation_id=%s error=%v", transferIDStr, active.transfer.OperationID, err)
    return fmt.Errorf("compute file checksum: %w", err)
  }
  computedChecksum := hex.EncodeToString(h.Sum(nil))

  log.Printf("TRANSFER_CHECKSUM_CALCULATED transfer_id=%s actual_checksum=%s expected_checksum=%s", transferIDStr, computedChecksum, active.fileSHA256)

  if computedChecksum != active.fileSHA256 {
    log.Printf("TRANSFER_CHECKSUM_MISMATCH transfer_id=%s actual_checksum=%s expected_checksum=%s", transferIDStr, computedChecksum, active.fileSHA256)
    return fmt.Errorf("file checksum mismatch: got %s want %s", computedChecksum, active.fileSHA256)
  }

  log.Printf("TRANSFER_CHECKSUM_OK transfer_id=%s checksum=%s", transferIDStr, computedChecksum)

  finalPath := filepath.Join(filepath.Dir(active.file.Name()), "final")
  _ = finalPath
  if err := active.file.Close(); err != nil {
    log.Printf("TRANSFER_FINALIZE_ERROR transfer_id=%s operation_id=%s error=%v", transferIDStr, active.transfer.OperationID, err)
    return fmt.Errorf("close staging file: %w", err)
  }
  active.file = nil
  active.cancel()

  now := time.Now().UTC()
  active.transfer.Status = job.TransferStatusCompleted
  active.transfer.CompletedAt = &now
  active.transfer.ReceivedChunks = active.totalChunks
  if err := r.transfers.Update(ctx, active.transfer); err != nil {
    log.Printf("TRANSFER_FINALIZE_ERROR transfer_id=%s operation_id=%s error=%v", transferIDStr, active.transfer.OperationID, err)
    return fmt.Errorf("update transfer completed: %w", err)
  }

  manifest := map[string]any{
    "transfer_id":  active.transfer.ID.String(),
    "operation_id": active.transfer.OperationID,
    "file_sha256":  computedChecksum,
    "total_bytes":  active.totalBytes,
    "total_chunks": active.totalChunks,
    "finalized_at": now.Format(time.RFC3339),
  }
  manifestBytes, _ := json.Marshal(manifest)
  if err := os.WriteFile(active.manifestPath, manifestBytes, 0644); err != nil {
    log.Printf("write manifest: %v", err)
  }

  evt := &event.OperationEvent{
    ID:            event.EventID(fmt.Sprintf("evt-%s-%d", active.transfer.ID, time.Now().UnixNano())),
    OperationID:   active.transfer.OperationID,
    EventType:     event.EventTypeTransferVerified,
    Payload:       map[string]any{"transfer_id": transferIDStr, "file_sha256": computedChecksum},
    CorrelationID: active.transfer.OperationID,
    IssuedAt:      now,
    ActorNodeID:   r.nodeID,
    Version:       1,
  }
  if err := r.events.Append(ctx, evt); err != nil {
    log.Printf("append transfer verified event: %v", err)
  }

  eventPayload, _ := json.Marshal(evt)
  if err := r.nats.PublishEvent(ctx, string(evt.EventType), eventPayload); err != nil {
    log.Printf("publish transfer verified event: %v", err)
  }

  log.Printf("TRANSFER_FINALIZE_SUCCESS transfer_id=%s operation_id=%s", transferIDStr, active.transfer.OperationID)
  return nil
}
