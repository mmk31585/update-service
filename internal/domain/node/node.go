package node

import "time"

type NodeStatus string

const (
	NodeStatusStarting  NodeStatus = "STARTING"
	NodeStatusHealthy   NodeStatus = "HEALTHY"
	NodeStatusDegraded  NodeStatus = "DEGRADED"
	NodeStatusUnhealthy NodeStatus = "UNHEALTHY"
	NodeStatusDraining  NodeStatus = "DRAINING"
	NodeStatusOffline   NodeStatus = "OFFLINE"
)

type Node struct {
	ID                   string
	IncarnationID        string
	IncarnationStartedAt time.Time
	Status               NodeStatus
	Roles                []string
	HeartbeatSequence    uint64
	LastHeartbeatAt      *time.Time
	HeartbeatDeadlineAt  *time.Time
	ActiveJobCount       uint64
	ActiveTransferCount  uint64
	MaxConcurrentJobs    *uint64
	DiskFreeBytes        *uint64
	DiskTotalBytes       *uint64
	CPUSample            map[string]any
	MemorySample         map[string]any
	SelfChecks           map[string]any
	Draining             bool
	ShutdownDeadlineAt   *time.Time
	RegisteredAt         time.Time
	UpdatedAt            time.Time
}

type NodeCapability struct {
	NodeID        string
	IncarnationID string
	Capability    string
	AdvertisedAt  time.Time
}

type Heartbeat struct {
	NodeID              string
	IncarnationID       string
	Sequence            uint64
	Status              NodeStatus
	Capabilities        []string
	ActiveJobs          uint64
	ActiveTransfers     uint64
	Resources           map[string]any
	SelfChecks          map[string]any
	Draining            bool
	ShutdownDeadlineAt  *time.Time
	ObservedAt          time.Time
	HeartbeatDeadlineAt time.Time
}
