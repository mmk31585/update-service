package operations

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"update/internal/application/ports"
	"update/internal/domain/event"
	"update/internal/domain/operation"
)

type FileOperationInput struct {
	OperationID operation.OperationID
	Body        io.Reader
	Headers     map[string]string
}

type FileOperationOutput struct {
	OperationID operation.OperationID
	Status      operation.OperationStatus
	Stage       operation.OperationStage
	InputFile   *operation.InputFile
	StatusURL   string
	ResultURL   string
	UpdatedAt   time.Time
	Version     int
}

var (
	ErrInvalidState = errors.New("operation not in CREATED state")
)

type FileOperationUseCase struct {
	operations  ports.OperationRepository
	events      ports.OperationEventRepository
	stagingDir  string
	chunkSize   int64
	maxFileSize int64
	baseURL     string
	dispatchUC  *DispatchOperationUseCase
}

func NewFileOperationUseCase(
	operations ports.OperationRepository,
	events ports.OperationEventRepository,
	stagingDir string,
	chunkSize int64,
	maxFileSize int64,
	baseURL string,
	dispatchUC *DispatchOperationUseCase,
) *FileOperationUseCase {
	return &FileOperationUseCase{
		operations:  operations,
		events:      events,
		stagingDir:  stagingDir,
		chunkSize:   chunkSize,
		maxFileSize: maxFileSize,
		baseURL:     baseURL,
		dispatchUC:  dispatchUC,
	}
}

func (uc *FileOperationUseCase) Execute(ctx context.Context, input FileOperationInput, idempotencyKey, method, path string) (*FileOperationOutput, error) {
	log.Printf("file operation: Execute called for %s", input.OperationID)
	op, err := uc.operations.GetByID(ctx, input.OperationID)
	if err != nil {
		return nil, err
	}
	if op == nil {
		return nil, ErrOperationNotFound
	}
	if op.Status != operation.StatusCreated {
		return nil, ErrInvalidState
	}

	// Create staging directory
	stagingPath := filepath.Join(uc.stagingDir, input.OperationID.String())
	if err := os.MkdirAll(stagingPath, 0755); err != nil {
		return nil, fmt.Errorf("create staging dir: %w", err)
	}

	// Create temporary file for ingress
	tmpFile, err := os.CreateTemp(stagingPath, "ingress-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}
	defer func() {
		_ = tmpFile.Close()
	}()
	tmpPath := tmpFile.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	// Stream body to temp file with size limit and SHA-256
	hasher := sha256.New()
	multiWriter := io.MultiWriter(tmpFile, hasher)
	limitedReader := io.LimitReader(input.Body, uc.maxFileSize+1)
	n, err := io.Copy(multiWriter, limitedReader)
	if err != nil {
		return nil, fmt.Errorf("write ingress file: %w", err)
	}
	if n > uc.maxFileSize {
		return nil, fmt.Errorf("file size exceeds limit: %d > %d", n, uc.maxFileSize)
	}

	// Verify digest if provided
	if digest := input.Headers["Digest"]; digest != "" {
		expected := fmt.Sprintf("SHA-256=%x", hasher.Sum(nil))
		if digest != expected {
			return nil, fmt.Errorf("digest mismatch: expected %s, got %s", expected, digest)
		}
	}

	// Move temp file to final path
	finalPath := filepath.Join(stagingPath, "input")
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return nil, fmt.Errorf("move ingress file: %w", err)
	}

	// Update operation with file metadata
	now := time.Now().UTC()
	op.InputFile = &operation.InputFile{
		SizeBytes: n,
		SHA256:    fmt.Sprintf("%x", hasher.Sum(nil)),
		FileName:  input.Headers["X-File-Name"],
	}
	op.Status = operation.StatusQueued
	op.Stage = operation.StageQueued
	op.UpdatedAt = now
	op.Version++

	if err := uc.operations.Update(ctx, op); err != nil {
		return nil, err
	}

	// Emit event
	evt := &event.OperationEvent{
		ID:            event.EventID(fmt.Sprintf("evt-%x", time.Now().UnixNano())),
		OperationID:   op.ID.String(),
		EventType:     event.EventTypeOperationQueued,
		Payload:       map[string]any{"file_size": n, "sha256": op.InputFile.SHA256},
		CorrelationID: op.ID.String(),
		IssuedAt:      now,
		ActorNodeID:   "self",
		Version:       op.Version,
	}
	_ = uc.events.Append(ctx, evt)

	// Background dispatch to worker
	if uc.dispatchUC != nil {
		log.Printf("file operation: starting background dispatch for %s", op.ID)
		go func() {
			if _, err := uc.dispatchUC.Execute(context.Background(), DispatchOperationInput{
				OperationID: op.ID,
				Processor:   op.Processor,
			}); err != nil {
				log.Printf("file operation: background dispatch failed for %s: %v", op.ID, err)
			}
		}()
	} else {
		log.Printf("file operation: dispatchUC is nil for %s", op.ID)
	}

	return &FileOperationOutput{
		OperationID: op.ID,
		Status:      op.Status,
		Stage:       op.Stage,
		InputFile:   op.InputFile,
		StatusURL:   uc.baseURL + "/api/v1/operations/" + op.ID.String(),
		ResultURL:   uc.baseURL + "/api/v1/operations/" + op.ID.String() + "/result",
		UpdatedAt:   op.UpdatedAt,
		Version:     op.Version,
	}, nil
}
