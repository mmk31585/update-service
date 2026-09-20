package result

import "time"

type ResultID string

func (id ResultID) String() string {
	return string(id)
}

type Result struct {
	ID            ResultID
	OperationID   string
	Processor     string
	SchemaVersion int
	Value         map[string]any
	ProducedAt    time.Time
	ExpiresAt     *time.Time
	SizeBytes     int64
	Checksum      string
}
