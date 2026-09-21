package transfer

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
	"update/internal/domain/job"
	"update/internal/domain/operation"
	"update/internal/infrastructure/nats"
)

const (
	ChunkHeaderSize = 64 * 1024 // 64KB default chunk size
	MaxInFlight     = 10        // max chunks in flight
)

type TransferSender struct {
	operations ports.OperationRepository
	transfers  ports.FileTransferRepository
	chunks     ports.FileChunkRepository
	events     ports.OperationEventRepository
	nats       *nats.Client
	stagingDir string
}

func NewTransferSender(
	operations ports.OperationRepository,
	transfers ports.FileTransferRepository,
	chunks ports.FileChunkRepository,
	events ports.OperationEventRepository,
	natsClient *nats.Client,
	stagingDir string,
) *TransferSender {
	return &TransferSender{
		operations: operations,
		transfers:  transfers,
		chunks:     chunks,
		events:     events,
		nats:       natsClient,
		stagingDir: stagingDir,
	}
}

func (s *TransferSender) Execute(ctx context.Context, operationID string, targetNodeID string) error {
	op, err := s.operations.GetByID(ctx, operation.OperationID(operationID))
	if err != nil {
		return err
	}
	if op == nil {
		return fmt.Errorf("operation not found: %s", operationID)
	}

	ingressPath := filepath.Join(s.stagingDir, operationID, "input")
	fileInfo, err := os.Stat(ingressPath)
	if err != nil {
		return fmt.Errorf("stat ingress file: %w", err)
	}

	fileSize := fileInfo.Size()
	chunkSize := ChunkHeaderSize
	totalChunks := int((fileSize + int64(chunkSize) - 1) / int64(chunkSize))

	transferID := job.TransferID(fmt.Sprintf("transfer-%s-%d", operationID, time.Now().UnixNano()))
	fileID := fmt.Sprintf("file-%s", operationID)
	now := time.Now().UTC()
	deadlineAt := now.Add(30 * time.Minute)

	fileChecksum := op.InputFile.SHA256

	transfer := &job.FileTransfer{
		ID:                transferID,
		OperationID:       operationID,
		FileID:            fileID,
		SourceNodeID:      "self",
		DestinationNodeID: targetNodeID,
		FileSizeBytes:     fileSize,
		ChunkSizeBytes:    int64(chunkSize),
		TotalChunks:       totalChunks,
		FileChecksum:      fileChecksum,
		ChecksumAlgorithm: "sha256",
		Status:            job.TransferStatusCreated,
		StagingPath:       filepath.Join(s.stagingDir, operationID),
		DeadlineAt:        deadlineAt,
		Version:           1,
	}

	if err := s.transfers.Create(ctx, transfer); err != nil {
		return fmt.Errorf("create transfer: %w", err)
	}

	startCmd := map[string]any{
		"command_id":           fmt.Sprintf("cmd-start-%s", transferID),
		"command_type":         "StartTransfer",
		"operation_id":         operationID,
		"transfer_id":          transferID.String(),
		"file_id":              fileID,
		"source_node_id":       "self",
		"destination_node_id":  targetNodeID,
		"file_size_bytes":      fileSize,
		"chunk_size_bytes":     int64(chunkSize),
		"total_chunks":         totalChunks,
		"file_sha256":          fileChecksum,
		"checksum_algorithm":   "sha256",
		"transfer_deadline_at": deadlineAt.Format(time.RFC3339),
	}
	cmdBytes, err := json.Marshal(startCmd)
	if err != nil {
		return fmt.Errorf("marshal start command: %w", err)
	}

	subject := nats.NodeCommandSubject(targetNodeID)
	if err := s.nats.Publish(ctx, subject, cmdBytes); err != nil {
		return fmt.Errorf("publish start transfer: %w", err)
	}

	log.Printf("transfer sender: started transfer %s to %s (%d chunks, %d bytes)",
		transferID, targetNodeID, totalChunks, fileSize)

	file, err := os.Open(ingressPath)
	if err != nil {
		return fmt.Errorf("open ingress file: %w", err)
	}
	defer file.Close()

	chunkBuf := make([]byte, chunkSize)
	seq := 0
	inFlight := 0
	for chunkIdx := 0; chunkIdx < totalChunks; chunkIdx++ {
		n, err := file.Read(chunkBuf)
		if err != nil && err != io.EOF {
			return fmt.Errorf("read chunk %d: %w", chunkIdx, err)
		}
		if n == 0 {
			break
		}

		payload := chunkBuf[:n]
		chunkChecksum := fmt.Sprintf("%x", sha256.Sum256(payload))

		chunkID := fmt.Sprintf("%s:%d", transferID, chunkIdx)
		chunk := &job.FileChunk{
			ID:           job.ChunkID(chunkID),
			TransferID:   transferID,
			ChunkIndex:   chunkIdx,
			ChunkOffset:  int64(chunkIdx) * int64(chunkSize),
			ChunkSize:    int64(n),
			Checksum:     chunkChecksum,
			PublishState: "published",
			AckState:     "pending",
			Version:      1,
		}

		if err := s.chunks.Upsert(ctx, chunk); err != nil {
			return fmt.Errorf("upsert chunk %d: %w", chunkIdx, err)
		}

		headers := map[string]string{
			"Nats-Msg-Id":            chunkID,
			"X-Schema-Version":       "1",
			"X-Message-Type":         "chunk",
			"X-Operation-Id":         operationID,
			"X-Correlation-Id":       operationID,
			"X-Transfer-Id":          transferID.String(),
			"X-Chunk-Id":             chunkID,
			"X-Chunk-Index":          fmt.Sprintf("%d", chunkIdx),
			"X-Offset":               fmt.Sprintf("%d", int64(chunkIdx)*int64(chunkSize)),
			"X-Payload-Length":       fmt.Sprintf("%d", n),
			"X-Chunk-Size":           fmt.Sprintf("%d", chunkSize),
			"X-Total-Bytes":          fmt.Sprintf("%d", fileSize),
			"X-Total-Chunks":         fmt.Sprintf("%d", totalChunks),
			"X-Chunk-SHA256":         chunkChecksum,
			"X-File-SHA256":          fileChecksum,
			"X-Source-Node-Id":       "self",
			"X-Destination-Node-Id":  targetNodeID,
			"X-Chunk-Deadline-At":    deadlineAt.Format(time.RFC3339),
			"X-Transfer-Deadline-At": deadlineAt.Format(time.RFC3339),
		}

		subject := nats.TransferChunksSubject(transferID.String())
		if err := s.nats.PublishWithHeaders(ctx, subject, payload, headers); err != nil {
			log.Printf("transfer sender: publish chunk %d failed: %v", chunkIdx, err)
			continue
		}

		seq++
		inFlight++

		if inFlight >= MaxInFlight {
			time.Sleep(10 * time.Millisecond)
			inFlight = 0
		}
	}

	log.Printf("transfer sender: published %d chunks for transfer %s", totalChunks, transferID)
	return nil
}
