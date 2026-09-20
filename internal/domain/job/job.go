package job

import "time"

type JobID string

func (id JobID) String() string {
	return string(id)
}

type JobStatus string

const (
	JobStatusCreated      JobStatus = "CREATED"
	JobStatusAssigned     JobStatus = "ASSIGNED"
	JobStatusDispatched   JobStatus = "DISPATCHED"
	JobStatusAcknowledged JobStatus = "ACKNOWLEDGED"
	JobStatusExecuting    JobStatus = "EXECUTING"
	JobStatusSucceeded    JobStatus = "SUCCEEDED"
	JobStatusFailed       JobStatus = "FAILED"
	JobStatusTimeout      JobStatus = "TIMEOUT"
	JobStatusCancelled    JobStatus = "CANCELLED"
)

type Job struct {
	ID             JobID
	OperationID    string
	WorkerNodeID   string
	Status         JobStatus
	Processor      string
	Requirements   map[string]any
	AssignedAt     time.Time
	DispatchedAt   *time.Time
	AcknowledgedAt *time.Time
	CompletedAt    *time.Time
	Error          *JobError
	Version        int
}

type JobError struct {
	Code    string
	Message string
	Details map[string]any
}

type AttemptID string

func (id AttemptID) String() string {
	return string(id)
}

type AttemptStatus string

const (
	AttemptStatusPending      AttemptStatus = "PENDING"
	AttemptStatusCommandSent  AttemptStatus = "COMMAND_SENT"
	AttemptStatusAcknowledged AttemptStatus = "ACKNOWLEDGED"
	AttemptStatusTransferring AttemptStatus = "TRANSFERRING"
	AttemptStatusTransferred  AttemptStatus = "TRANSFERRED"
	AttemptStatusProcessing   AttemptStatus = "PROCESSING"
	AttemptStatusSucceeded    AttemptStatus = "SUCCEEDED"
	AttemptStatusFailed       AttemptStatus = "FAILED"
	AttemptStatusTimeout      AttemptStatus = "TIMEOUT"
	AttemptStatusCancelled    AttemptStatus = "CANCELLED"
)

type JobAttempt struct {
	ID            AttemptID
	JobID         JobID
	OperationID   string
	NodeID        string
	AttemptNumber int
	Status        AttemptStatus
	CommandID     string
	ClaimToken    string
	LeaseUntil    time.Time
	ProcessingID  *string
	Error         *JobError
	StartedAt     *time.Time
	CompletedAt   *time.Time
	Version       int
}

type TransferID string

func (id TransferID) String() string {
	return string(id)
}

type TransferStatus string

const (
	TransferStatusCreated      TransferStatus = "CREATED"
	TransferStatusAccepted     TransferStatus = "ACCEPTED"
	TransferStatusTransferring TransferStatus = "TRANSFERRING"
	TransferStatusVerifying    TransferStatus = "VERIFYING"
	TransferStatusCompleted    TransferStatus = "COMPLETED"
	TransferStatusFailed       TransferStatus = "FAILED"
	TransferStatusTimeout      TransferStatus = "TIMEOUT"
	TransferStatusCancelled    TransferStatus = "CANCELLED"
)

type FileTransfer struct {
	ID                   TransferID
	AttemptID            AttemptID
	OperationID          string
	FileID               string
	SourceNodeID         string
	DestinationNodeID    string
	FileSizeBytes        int64
	ChunkSizeBytes       int64
	TotalChunks          int
	FileChecksum         string
	ChecksumAlgorithm    string
	Status               TransferStatus
	StagingPath          string
	StagingReservationID *string
	DeadlineAt           time.Time
	AcceptedAt           *time.Time
	CompletedAt          *time.Time
	ReceivedChunks       int
	Error                *JobError
	Version              int
}

type ChunkID string

func (id ChunkID) String() string {
	return string(id)
}

type FileChunk struct {
	ID             ChunkID
	TransferID     TransferID
	ChunkIndex     int
	Offset         int64
	ChunkSize      int64
	Checksum       string
	PublishState   string
	AckState       string
	ReceivedAt     *time.Time
	AcknowledgedAt *time.Time
	Version        int
}
