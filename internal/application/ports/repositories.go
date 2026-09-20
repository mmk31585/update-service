package ports

import (
	"context"
	"errors"
	"time"

	"update/internal/domain/event"
	"update/internal/domain/job"
	"update/internal/domain/node"
	"update/internal/domain/operation"
	"update/internal/domain/result"
)

var (
	ErrOperationNotFound = errors.New("operation not found")
)

type OperationRepository interface {
	Create(ctx context.Context, op *operation.Operation) error
	GetByID(ctx context.Context, id operation.OperationID) (*operation.Operation, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*operation.Operation, error)
	Update(ctx context.Context, op *operation.Operation) error
	Transition(ctx context.Context, id operation.OperationID, from operation.OperationStatus, to operation.OperationStatus, version int) (*operation.Operation, error)
	List(ctx context.Context, filter OperationFilter) ([]*operation.Operation, error)
}

type OperationFilter struct {
	Status    *operation.OperationStatus
	Processor *string
	Cursor    *string
	Limit     int
}

type JobRepository interface {
	Create(ctx context.Context, j *job.Job) error
	GetByID(ctx context.Context, id job.JobID) (*job.Job, error)
	GetByOperationID(ctx context.Context, operationID string) (*job.Job, error)
	Update(ctx context.Context, j *job.Job) error
	ListPendingForScheduler(ctx context.Context, limit int) ([]*job.Job, error)
}

type JobAttemptRepository interface {
	Create(ctx context.Context, a *job.JobAttempt) error
	GetByID(ctx context.Context, id job.AttemptID) (*job.JobAttempt, error)
	GetActiveByJobID(ctx context.Context, jobID job.JobID) (*job.JobAttempt, error)
	Update(ctx context.Context, a *job.JobAttempt) error
}

type FileTransferRepository interface {
	Create(ctx context.Context, t *job.FileTransfer) error
	GetByID(ctx context.Context, id job.TransferID) (*job.FileTransfer, error)
	GetByAttemptID(ctx context.Context, attemptID job.AttemptID) (*job.FileTransfer, error)
	Update(ctx context.Context, t *job.FileTransfer) error
}

type FileChunkRepository interface {
	Upsert(ctx context.Context, c *job.FileChunk) error
	GetByTransferID(ctx context.Context, transferID job.TransferID) ([]*job.FileChunk, error)
	CountReceived(ctx context.Context, transferID job.TransferID) (int, error)
}

type ResultRepository interface {
	Create(ctx context.Context, r *result.Result) error
	GetByOperationID(ctx context.Context, operationID string) (*result.Result, error)
}

type IdempotencyRepository interface {
	Store(ctx context.Context, key, method, path, fingerprint string, response any, expiresAt time.Time) error
	Get(ctx context.Context, key, method, path string) (string, []byte, time.Time, error)
	Delete(ctx context.Context, key, method, path string) error
}

type OperationEventRepository interface {
	Append(ctx context.Context, evt *event.OperationEvent) error
	GetByOperationID(ctx context.Context, operationID string, limit int) ([]*event.OperationEvent, error)
}

type NodeRepository interface {
	Upsert(ctx context.Context, node *node.Node) error
	GetByID(ctx context.Context, nodeID string) (*node.Node, error)
	UpdateStatus(ctx context.Context, nodeID string, status node.NodeStatus) error
	UpdateHeartbeat(ctx context.Context, nodeID string, seq uint64, deadlineAt time.Time) error
	ListHealthy(ctx context.Context) ([]*node.Node, error)
}

type HeartbeatRepository interface {
	Insert(ctx context.Context, hb *node.Heartbeat) error
	GetLatest(ctx context.Context, nodeID string) (*node.Heartbeat, error)
}
