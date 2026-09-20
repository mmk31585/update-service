package operation

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

type AttemptID string

func (id AttemptID) String() string {
	return string(id)
}

type TransferID string

func (id TransferID) String() string {
	return string(id)
}
