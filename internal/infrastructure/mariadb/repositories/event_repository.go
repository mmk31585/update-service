package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"

	"update/internal/domain/event"
)

type OperationEventRepository struct {
	db *DB
}

func NewOperationEventRepository(db *DB) *OperationEventRepository {
	return &OperationEventRepository{db: db}
}

func (r *OperationEventRepository) Append(ctx context.Context, evt *event.OperationEvent) error {
	payloadJSON, err := json.Marshal(evt.Payload)
	if err != nil {
		return err
	}
	return r.db.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO operation_events (id, operation_id, event_type, job_id, attempt_id, transfer_id,
				payload, correlation_id, issued_at, actor_node_id, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			evt.ID.String(), evt.OperationID, evt.EventType,
			nullString(evt.JobID), nullString(evt.AttemptID), nullString(evt.TransferID),
			string(payloadJSON), evt.CorrelationID, evt.IssuedAt, evt.ActorNodeID, evt.Version,
		)
		return err
	})
}

func (r *OperationEventRepository) GetByOperationID(ctx context.Context, operationID string, limit int) ([]*event.OperationEvent, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, operation_id, event_type, job_id, attempt_id, transfer_id,
			payload, correlation_id, issued_at, actor_node_id, version
		FROM operation_events
		WHERE operation_id = ?
		ORDER BY issued_at ASC
		LIMIT ?`,
		operationID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*event.OperationEvent
	for rows.Next() {
		var evt event.OperationEvent
		var payloadJSON []byte
		var jobID, attemptID, transferID sql.NullString

		err := rows.Scan(
			&evt.ID, &evt.OperationID, &evt.EventType, &jobID, &attemptID, &transferID,
			&payloadJSON, &evt.CorrelationID, &evt.IssuedAt, &evt.ActorNodeID, &evt.Version,
		)
		if err != nil {
			return nil, err
		}

		if jobID.Valid {
			v := jobID.String
			evt.JobID = &v
		}
		if attemptID.Valid {
			v := attemptID.String
			evt.AttemptID = &v
		}
		if transferID.Valid {
			v := transferID.String
			evt.TransferID = &v
		}
		if payloadJSON != nil {
			_ = json.Unmarshal(payloadJSON, &evt.Payload)
		}

		events = append(events, &evt)
	}

	return events, rows.Err()
}
