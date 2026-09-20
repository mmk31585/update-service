package event

import "time"

type EventID string

func (id EventID) String() string {
	return string(id)
}

type EventType string

const (
	EventTypeOperationCreated        EventType = "OperationCreated"
	EventTypeOperationQueued         EventType = "OperationQueued"
	EventTypeOperationDispatched     EventType = "OperationDispatched"
	EventTypeOperationTransferred    EventType = "OperationTransferred"
	EventTypeOperationProcessing     EventType = "OperationProcessing"
	EventTypeOperationCompleted      EventType = "OperationCompleted"
	EventTypeOperationFailed         EventType = "OperationFailed"
	EventTypeOperationTimedOut       EventType = "OperationTimedOut"
	EventTypeOperationCancelled      EventType = "OperationCancelled"
	EventTypeOperationRetryScheduled EventType = "OperationRetryScheduled"
	EventTypeCommandAccepted         EventType = "CommandAccepted"
	EventTypeTransferAccepted        EventType = "TransferAccepted"
	EventTypeChunkAccepted           EventType = "ChunkAccepted"
	EventTypeTransferVerified        EventType = "TransferVerified"
	EventTypeTransferFailed          EventType = "TransferFailed"
	EventTypeProcessingStarted       EventType = "ProcessingStarted"
	EventTypeProcessingSucceeded     EventType = "ProcessingSucceeded"
	EventTypeProcessingFailed        EventType = "ProcessingFailed"
	EventTypeCancelAccepted          EventType = "CancelAccepted"
	EventTypeCancelled               EventType = "Cancelled"
)

type OperationEvent struct {
	ID            EventID
	OperationID   string
	EventType     EventType
	JobID         *string
	AttemptID     *string
	TransferID    *string
	Payload       map[string]any
	CorrelationID string
	IssuedAt      time.Time
	ActorNodeID   string
	Version       int
}
