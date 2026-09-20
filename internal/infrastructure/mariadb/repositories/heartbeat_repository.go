package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"update/internal/domain/node"
)

type HeartbeatRepository struct {
	db *DB
}

func NewHeartbeatRepository(db *DB) *HeartbeatRepository {
	return &HeartbeatRepository{db: db}
}

func (r *HeartbeatRepository) Insert(ctx context.Context, hb *node.Heartbeat) error {
	capabilitiesJSON, err := json.Marshal(hb.Capabilities)
	if err != nil {
		return err
	}
	resourcesJSON, err := json.Marshal(hb.Resources)
	if err != nil {
		return err
	}
	selfChecksJSON, err := json.Marshal(hb.SelfChecks)
	if err != nil {
		return err
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO node_heartbeats (
			node_id, incarnation_id, sequence, status, capabilities,
			active_jobs, active_transfers, resources, self_checks,
			draining, shutdown_deadline_at, observed_at, heartbeat_deadline_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		hb.NodeID, hb.IncarnationID, hb.Sequence, hb.Status, string(capabilitiesJSON),
		hb.ActiveJobs, hb.ActiveTransfers, resourcesJSON, selfChecksJSON,
		hb.Draining, nullTime(hb.ShutdownDeadlineAt), hb.ObservedAt, hb.HeartbeatDeadlineAt,
	)
	return err
}

func (r *HeartbeatRepository) GetLatest(ctx context.Context, nodeID string) (*node.Heartbeat, error) {
	var hb node.Heartbeat
	var capabilitiesJSON, resourcesJSON, selfChecksJSON []byte
	var shutdownDeadlineAt sql.NullTime

	err := r.db.QueryRowContext(ctx, `
		SELECT node_id, incarnation_id, sequence, status, capabilities,
			active_jobs, active_transfers, resources, self_checks,
			draining, shutdown_deadline_at, observed_at, heartbeat_deadline_at
		FROM node_heartbeats
		WHERE node_id = ?
		ORDER BY sequence DESC
		LIMIT 1`, nodeID).
		Scan(&hb.NodeID, &hb.IncarnationID, &hb.Sequence, &hb.Status, &capabilitiesJSON,
			&hb.ActiveJobs, &hb.ActiveTransfers, &resourcesJSON, &selfChecksJSON,
			&hb.Draining, &shutdownDeadlineAt, &hb.ObservedAt, &hb.HeartbeatDeadlineAt,
		)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	_ = json.Unmarshal(capabilitiesJSON, &hb.Capabilities)
	_ = json.Unmarshal(resourcesJSON, &hb.Resources)
	_ = json.Unmarshal(selfChecksJSON, &hb.SelfChecks)

	if shutdownDeadlineAt.Valid {
		hb.ShutdownDeadlineAt = &shutdownDeadlineAt.Time
	}

	return &hb, nil
}
