package elasticsearch

import (
	"time"

	"update/internal/domain/event"
	"update/internal/domain/operation"
)

type CompletedDocument struct {
	OperationID   string    `json:"operation_id"`
	NodeID        string    `json:"node_id"`
	UpdateType    string    `json:"update_type"`
	Status        string    `json:"status"`
	DurationMs    int64     `json:"duration_ms"`
	FileSize      int64     `json:"file_size"`
	StartedAt     time.Time `json:"started_at"`
	CompletedAt   time.Time `json:"completed_at"`
	Processor     string    `json:"processor"`
	EventType     string    `json:"event_type"`
	ActorNodeID   string    `json:"actor_node_id"`
	IssuedAt      time.Time `json:"issued_at"`
	ClientRef     string    `json:"client_reference,omitempty"`
	RetryCount    int       `json:"retry_count"`
	MaxAttempts   int       `json:"max_attempts"`
	InputFileName string    `json:"input_file_name,omitempty"`
	InputFileSize int64     `json:"input_file_size,omitempty"`
}

func BuildCompletedDocument(op *operation.Operation, evt *event.OperationEvent, nodeID string) CompletedDocument {
	startedAt := op.CreatedAt
	completedAt := op.UpdatedAt
	if evt.IssuedAt.After(op.CreatedAt) {
		completedAt = evt.IssuedAt
	}

	var durationMs int64
	if !op.CreatedAt.IsZero() && !completedAt.IsZero() {
		durationMs = completedAt.Sub(op.CreatedAt).Milliseconds()
	}

	var fileSize int64
	var inputFileName string
	if op.InputFile != nil {
		fileSize = op.InputFile.SizeBytes
		inputFileName = op.InputFile.FileName
	}

	return CompletedDocument{
		OperationID:   op.ID.String(),
		NodeID:        nodeID,
		UpdateType:    "PROCESS_UPDATE",
		Status:        string(op.Status),
		DurationMs:    durationMs,
		FileSize:      fileSize,
		StartedAt:     startedAt.UTC(),
		CompletedAt:   completedAt.UTC(),
		Processor:     op.Processor,
		EventType:     string(evt.EventType),
		ActorNodeID:   evt.ActorNodeID,
		IssuedAt:      evt.IssuedAt.UTC(),
		ClientRef:     op.ClientReference,
		RetryCount:    op.RetryCount,
		MaxAttempts:   op.MaxAttempts,
		InputFileName: inputFileName,
		InputFileSize: fileSize,
	}
}
