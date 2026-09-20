package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"update/internal/application/ports"
	"update/internal/domain/job"
)

type JobRepository struct {
	db *DB
}

func NewJobRepository(db *DB) *JobRepository {
	return &JobRepository{db: db}
}

func (r *JobRepository) Create(ctx context.Context, j *job.Job) error {
	reqJSON, err := json.Marshal(j.Requirements)
	if err != nil {
		return err
	}
	return r.db.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO jobs (id, operation_id, worker_node_id, status, processor, requirements,
				assigned_at, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			j.ID.String(), j.OperationID, j.WorkerNodeID, j.Status, j.Processor, string(reqJSON),
			j.AssignedAt, j.Version,
		)
		return err
	})
}

func (r *JobRepository) GetByID(ctx context.Context, id job.JobID) (*job.Job, error) {
	var j job.Job
	var reqJSON []byte
	var dispatchedAt, acknowledgedAt, completedAt sql.NullTime
	var errorCode, errorMessage, errorDetails sql.NullString

	err := r.db.QueryRowContext(ctx, `
		SELECT id, operation_id, worker_node_id, status, processor, requirements,
			assigned_at, dispatched_at, acknowledged_at, completed_at,
			error_code, error_message, error_details, version
		FROM jobs WHERE id = ?`, id.String()).
		Scan(&j.ID, &j.OperationID, &j.WorkerNodeID, &j.Status, &j.Processor, &reqJSON,
			&j.AssignedAt, &dispatchedAt, &acknowledgedAt, &completedAt,
			&errorCode, &errorMessage, &errorDetails, &j.Version,
		)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ports.ErrOperationNotFound
	}
	if err != nil {
		return nil, err
	}

	if reqJSON != nil {
		_ = json.Unmarshal(reqJSON, &j.Requirements)
	}
	if dispatchedAt.Valid {
		j.DispatchedAt = &dispatchedAt.Time
	}
	if acknowledgedAt.Valid {
		j.AcknowledgedAt = &acknowledgedAt.Time
	}
	if completedAt.Valid {
		j.CompletedAt = &completedAt.Time
	}
	if errorCode.Valid || errorMessage.Valid {
		j.Error = &job.JobError{
			Code:    errorCode.String,
			Message: errorMessage.String,
		}
		if errorDetails.Valid && errorDetails.String != "" {
			_ = json.Unmarshal([]byte(errorDetails.String), &j.Error.Details)
		}
	}

	return &j, nil
}

func (r *JobRepository) GetByOperationID(ctx context.Context, operationID string) (*job.Job, error) {
	var j job.Job
	var reqJSON []byte
	var dispatchedAt, acknowledgedAt, completedAt sql.NullTime
	var errorCode, errorMessage, errorDetails sql.NullString

	err := r.db.QueryRowContext(ctx, `
		SELECT id, operation_id, worker_node_id, status, processor, requirements,
			assigned_at, dispatched_at, acknowledged_at, completed_at,
			error_code, error_message, error_details, version
		FROM jobs WHERE operation_id = ? ORDER BY assigned_at DESC LIMIT 1`, operationID).
		Scan(&j.ID, &j.OperationID, &j.WorkerNodeID, &j.Status, &j.Processor, &reqJSON,
			&j.AssignedAt, &dispatchedAt, &acknowledgedAt, &completedAt,
			&errorCode, &errorMessage, &errorDetails, &j.Version,
		)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if reqJSON != nil {
		_ = json.Unmarshal(reqJSON, &j.Requirements)
	}
	if dispatchedAt.Valid {
		j.DispatchedAt = &dispatchedAt.Time
	}
	if acknowledgedAt.Valid {
		j.AcknowledgedAt = &acknowledgedAt.Time
	}
	if completedAt.Valid {
		j.CompletedAt = &completedAt.Time
	}
	if errorCode.Valid || errorMessage.Valid {
		j.Error = &job.JobError{
			Code:    errorCode.String,
			Message: errorMessage.String,
		}
		if errorDetails.Valid && errorDetails.String != "" {
			_ = json.Unmarshal([]byte(errorDetails.String), &j.Error.Details)
		}
	}

	return &j, nil
}

func (r *JobRepository) Update(ctx context.Context, j *job.Job) error {
	reqJSON, err := json.Marshal(j.Requirements)
	if err != nil {
		return err
	}
	var dispatchedAt, acknowledgedAt, completedAt sql.NullTime
	if j.DispatchedAt != nil {
		dispatchedAt = sql.NullTime{Time: *j.DispatchedAt, Valid: true}
	}
	if j.AcknowledgedAt != nil {
		acknowledgedAt = sql.NullTime{Time: *j.AcknowledgedAt, Valid: true}
	}
	if j.CompletedAt != nil {
		completedAt = sql.NullTime{Time: *j.CompletedAt, Valid: true}
	}

	var errorCode, errorMessage, errorDetails sql.NullString
	if j.Error != nil {
		if j.Error.Code != "" {
			errorCode = sql.NullString{String: j.Error.Code, Valid: true}
		}
		if j.Error.Message != "" {
			errorMessage = sql.NullString{String: j.Error.Message, Valid: true}
		}
		if j.Error.Details != nil {
			b, err := json.Marshal(j.Error.Details)
			if err != nil {
				return err
			}
			errorDetails = sql.NullString{String: string(b), Valid: true}
		}
	}

	_, err = r.db.ExecContext(ctx, `
		UPDATE jobs SET status = ?, worker_node_id = ?, processor = ?, requirements = ?,
			dispatched_at = ?, acknowledged_at = ?, completed_at = ?,
			error_code = ?, error_message = ?, error_details = ?, version = ?
		WHERE id = ? AND version = ?`,
		j.Status, j.WorkerNodeID, j.Processor, string(reqJSON),
		dispatchedAt, acknowledgedAt, completedAt,
		errorCode, errorMessage, errorDetails,
		j.Version+1, j.ID, j.Version,
	)
	return err
}

func (r *JobRepository) ListPendingForScheduler(ctx context.Context, limit int) ([]*job.Job, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, operation_id, worker_node_id, status, processor, requirements,
			assigned_at, dispatched_at, acknowledged_at, completed_at,
			error_code, error_message, error_details, version
		FROM jobs
		WHERE status IN (?, ?)
		ORDER BY assigned_at ASC
		LIMIT ?`,
		job.JobStatusCreated, job.JobStatusAssigned,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*job.Job
	for rows.Next() {
		var j job.Job
		var reqJSON []byte
		var dispatchedAt, acknowledgedAt, completedAt sql.NullTime
		var errorCode, errorMessage, errorDetails sql.NullString

		err := rows.Scan(
			&j.ID, &j.OperationID, &j.WorkerNodeID, &j.Status, &j.Processor, &reqJSON,
			&j.AssignedAt, &dispatchedAt, &acknowledgedAt, &completedAt,
			&errorCode, &errorMessage, &errorDetails, &j.Version,
		)
		if err != nil {
			return nil, err
		}

		if reqJSON != nil {
			_ = json.Unmarshal(reqJSON, &j.Requirements)
		}
		if dispatchedAt.Valid {
			j.DispatchedAt = &dispatchedAt.Time
		}
		if acknowledgedAt.Valid {
			j.AcknowledgedAt = &acknowledgedAt.Time
		}
		if completedAt.Valid {
			j.CompletedAt = &completedAt.Time
		}
		if errorCode.Valid || errorMessage.Valid {
			j.Error = &job.JobError{
				Code:    errorCode.String,
				Message: errorMessage.String,
			}
			if errorDetails.Valid && errorDetails.String != "" {
				_ = json.Unmarshal([]byte(errorDetails.String), &j.Error.Details)
			}
		}

		jobs = append(jobs, &j)
	}

	return jobs, rows.Err()
}
