package operation

import (
	"time"
)

type OperationID string

func (id OperationID) String() string {
	return string(id)
}

type OperationStatus string

const (
	StatusCreated      OperationStatus = "CREATED"
	StatusQueued       OperationStatus = "QUEUED"
	StatusDispatched   OperationStatus = "DISPATCHED"
	StatusTransferring OperationStatus = "TRANSFERRING"
	StatusTransferred  OperationStatus = "TRANSFERRED"
	StatusProcessing   OperationStatus = "PROCESSING"
	StatusCompleted    OperationStatus = "COMPLETED"
	StatusFailed       OperationStatus = "FAILED"
	StatusTimeout      OperationStatus = "TIMEOUT"
	StatusCancelled    OperationStatus = "CANCELLED"
)

type OperationStage string

const (
	StageWaitingForUpload OperationStage = "WAITING_FOR_UPLOAD"
	StageQueued           OperationStage = "QUEUED"
	StageScheduling       OperationStage = "SCHEDULING"
	StageWaitingForAck    OperationStage = "WAITING_FOR_ACK"
	StageTransferring     OperationStage = "TRANSFERRING"
	StageTransferred      OperationStage = "TRANSFERRED"
	StageVerifying        OperationStage = "VERIFYING"
	StageProcessing       OperationStage = "PROCESSING"
	StageCompleted        OperationStage = "COMPLETED"
	StageFailed           OperationStage = "FAILED"
	StageTimeout          OperationStage = "TIMEOUT"
	StageCancelled        OperationStage = "CANCELLED"
)

type OperationError struct {
	Code    string
	Message string
	Details map[string]any
}

type RetryPolicy struct {
	MaxAttempts int
}

type InputFile struct {
	SizeBytes int64
	SHA256    string
	FileName  string
}

type UploadTarget struct {
	Method   string
	URL      string
	Headers  map[string]string
	Expires  time.Time
	MaxBytes int64
}

type Operation struct {
	ID                OperationID
	Status            OperationStatus
	Stage             OperationStage
	Processor         string
	RetryCount        int
	MaxAttempts       int
	ClientReference   string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Deadline          *time.Time
	InputFile         *InputFile
	CurrentJobID      *JobID
	CurrentAttemptID  *AttemptID
	CurrentTransferID *TransferID
	CancelRequestedAt *time.Time
	Error             *OperationError
	Version           int
}
