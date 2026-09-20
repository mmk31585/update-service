package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"update/internal/domain/node"
)

type NodeRepository struct {
	db *DB
}

func NewNodeRepository(db *DB) *NodeRepository {
	return &NodeRepository{db: db}
}

func (r *NodeRepository) Upsert(ctx context.Context, n *node.Node) error {
	rolesJSON, err := json.Marshal(n.Roles)
	if err != nil {
		return err
	}

	var cpuSample, memorySample, selfChecks []byte
	if n.CPUSample != nil {
		cpuSample, _ = json.Marshal(n.CPUSample)
	}
	if n.MemorySample != nil {
		memorySample, _ = json.Marshal(n.MemorySample)
	}
	if n.SelfChecks != nil {
		selfChecks, _ = json.Marshal(n.SelfChecks)
	}

	var maxConcurrentJobs sql.NullInt64
	if n.MaxConcurrentJobs != nil {
		maxConcurrentJobs = sql.NullInt64{Int64: int64(*n.MaxConcurrentJobs), Valid: true}
	}
	var diskFreeBytes, diskTotalBytes sql.NullInt64
	if n.DiskFreeBytes != nil {
		diskFreeBytes = sql.NullInt64{Int64: int64(*n.DiskFreeBytes), Valid: true}
	}
	if n.DiskTotalBytes != nil {
		diskTotalBytes = sql.NullInt64{Int64: int64(*n.DiskTotalBytes), Valid: true}
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO nodes (
			node_id, incarnation_id, incarnation_started_at, status, roles,
			heartbeat_sequence, last_heartbeat_at, heartbeat_deadline_at,
			active_job_count, active_transfer_count, max_concurrent_jobs,
			disk_free_bytes, disk_total_bytes, cpu_sample, memory_sample, self_checks,
			draining, shutdown_deadline_at, registered_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			incarnation_id = VALUES(incarnation_id),
			incarnation_started_at = VALUES(incarnation_started_at),
			status = VALUES(status),
			roles = VALUES(roles),
			heartbeat_sequence = VALUES(heartbeat_sequence),
			last_heartbeat_at = VALUES(last_heartbeat_at),
			heartbeat_deadline_at = VALUES(heartbeat_deadline_at),
			active_job_count = VALUES(active_job_count),
			active_transfer_count = VALUES(active_transfer_count),
			max_concurrent_jobs = VALUES(max_concurrent_jobs),
			disk_free_bytes = VALUES(disk_free_bytes),
			disk_total_bytes = VALUES(disk_total_bytes),
			cpu_sample = VALUES(cpu_sample),
			memory_sample = VALUES(memory_sample),
			self_checks = VALUES(self_checks),
			draining = VALUES(draining),
			shutdown_deadline_at = VALUES(shutdown_deadline_at),
			updated_at = VALUES(updated_at)`,
		n.ID, n.IncarnationID, n.IncarnationStartedAt, n.Status, string(rolesJSON),
		n.HeartbeatSequence, nullTime(n.LastHeartbeatAt), nullTime(n.HeartbeatDeadlineAt),
		n.ActiveJobCount, n.ActiveTransferCount, maxConcurrentJobs,
		diskFreeBytes, diskTotalBytes, cpuSample, memorySample, selfChecks,
		n.Draining, nullTime(n.ShutdownDeadlineAt), n.RegisteredAt, n.UpdatedAt,
	)
	return err
}

func (r *NodeRepository) GetByID(ctx context.Context, nodeID string) (*node.Node, error) {
	var n node.Node
	var rolesJSON, cpuSample, memorySample, selfChecks []byte
	var maxConcurrentJobs, diskFreeBytes, diskTotalBytes sql.NullInt64
	var lastHeartbeatAt, heartbeatDeadlineAt, shutdownDeadlineAt sql.NullTime

	err := r.db.QueryRowContext(ctx, `
		SELECT node_id, incarnation_id, incarnation_started_at, status, roles,
			heartbeat_sequence, last_heartbeat_at, heartbeat_deadline_at,
			active_job_count, active_transfer_count, max_concurrent_jobs,
			disk_free_bytes, disk_total_bytes, cpu_sample, memory_sample, self_checks,
			draining, shutdown_deadline_at, registered_at, updated_at
		FROM nodes WHERE node_id = ?`, nodeID).
		Scan(&n.ID, &n.IncarnationID, &n.IncarnationStartedAt, &n.Status, &rolesJSON,
			&n.HeartbeatSequence, &lastHeartbeatAt, &heartbeatDeadlineAt,
			&n.ActiveJobCount, &n.ActiveTransferCount, &maxConcurrentJobs,
			&diskFreeBytes, &diskTotalBytes, &cpuSample, &memorySample, &selfChecks,
			&n.Draining, &shutdownDeadlineAt, &n.RegisteredAt, &n.UpdatedAt,
		)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	_ = json.Unmarshal(rolesJSON, &n.Roles)
	_ = json.Unmarshal(cpuSample, &n.CPUSample)
	_ = json.Unmarshal(memorySample, &n.MemorySample)
	_ = json.Unmarshal(selfChecks, &n.SelfChecks)

	if lastHeartbeatAt.Valid {
		n.LastHeartbeatAt = &lastHeartbeatAt.Time
	}
	if heartbeatDeadlineAt.Valid {
		n.HeartbeatDeadlineAt = &heartbeatDeadlineAt.Time
	}
	if shutdownDeadlineAt.Valid {
		n.ShutdownDeadlineAt = &shutdownDeadlineAt.Time
	}
	if maxConcurrentJobs.Valid {
		v := uint64(maxConcurrentJobs.Int64)
		n.MaxConcurrentJobs = &v
	}
	if diskFreeBytes.Valid {
		v := uint64(diskFreeBytes.Int64)
		n.DiskFreeBytes = &v
	}
	if diskTotalBytes.Valid {
		v := uint64(diskTotalBytes.Int64)
		n.DiskTotalBytes = &v
	}

	return &n, nil
}

func (r *NodeRepository) UpdateStatus(ctx context.Context, nodeID string, status node.NodeStatus) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE nodes SET status = ?, updated_at = ? WHERE node_id = ?`,
		status, time.Now().UTC(), nodeID,
	)
	return err
}

func (r *NodeRepository) UpdateHeartbeat(ctx context.Context, nodeID string, seq uint64, deadlineAt time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE nodes SET
			heartbeat_sequence = ?,
			last_heartbeat_at = ?,
			heartbeat_deadline_at = ?,
			updated_at = ?
		WHERE node_id = ?`,
		seq, time.Now().UTC(), deadlineAt, time.Now().UTC(), nodeID,
	)
	return err
}

func (r *NodeRepository) ListHealthy(ctx context.Context) ([]*node.Node, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT node_id, incarnation_id, incarnation_started_at, status, roles,
			heartbeat_sequence, last_heartbeat_at, heartbeat_deadline_at,
			active_job_count, active_transfer_count, max_concurrent_jobs,
			disk_free_bytes, disk_total_bytes, cpu_sample, memory_sample, self_checks,
			draining, shutdown_deadline_at, registered_at, updated_at
		FROM nodes
		WHERE status IN (?, ?)
		ORDER BY updated_at ASC`,
		node.NodeStatusHealthy, node.NodeStatusDegraded,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []*node.Node
	for rows.Next() {
		var n node.Node
		var rolesJSON, cpuSample, memorySample, selfChecks []byte
		var maxConcurrentJobs, diskFreeBytes, diskTotalBytes sql.NullInt64
		var lastHeartbeatAt, heartbeatDeadlineAt, shutdownDeadlineAt sql.NullTime

		err := rows.Scan(
			&n.ID, &n.IncarnationID, &n.IncarnationStartedAt, &n.Status, &rolesJSON,
			&n.HeartbeatSequence, &lastHeartbeatAt, &heartbeatDeadlineAt,
			&n.ActiveJobCount, &n.ActiveTransferCount, &maxConcurrentJobs,
			&diskFreeBytes, &diskTotalBytes, &cpuSample, &memorySample, &selfChecks,
			&n.Draining, &shutdownDeadlineAt, &n.RegisteredAt, &n.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}

		_ = json.Unmarshal(rolesJSON, &n.Roles)
		_ = json.Unmarshal(cpuSample, &n.CPUSample)
		_ = json.Unmarshal(memorySample, &n.MemorySample)
		_ = json.Unmarshal(selfChecks, &n.SelfChecks)

		if lastHeartbeatAt.Valid {
			n.LastHeartbeatAt = &lastHeartbeatAt.Time
		}
		if heartbeatDeadlineAt.Valid {
			n.HeartbeatDeadlineAt = &heartbeatDeadlineAt.Time
		}
		if shutdownDeadlineAt.Valid {
			n.ShutdownDeadlineAt = &shutdownDeadlineAt.Time
		}
		if maxConcurrentJobs.Valid {
			v := uint64(maxConcurrentJobs.Int64)
			n.MaxConcurrentJobs = &v
		}
		if diskFreeBytes.Valid {
			v := uint64(diskFreeBytes.Int64)
			n.DiskFreeBytes = &v
		}
		if diskTotalBytes.Valid {
			v := uint64(diskTotalBytes.Int64)
			n.DiskTotalBytes = &v
		}

		nodes = append(nodes, &n)
	}

	return nodes, rows.Err()
}
