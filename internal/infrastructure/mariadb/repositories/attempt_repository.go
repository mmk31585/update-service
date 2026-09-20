package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"update/internal/application/ports"
	"update/internal/domain/job"
)

type JobAttemptRepository struct {
	db *DB
}

func NewJobAttemptRepository(db *DB) *JobAttemptRepository {
	return &JobAttemptRepository{db: db}
}

func (r *JobAttemptRepository) Create(ctx context.Context, a *job.JobAttempt) error {
	return r.db.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var errorDetails sql.NullString
		if a.Error != nil && a.Error.Details != nil {
			b, err := json.Marshal(a.Error.Details)
			if err != nil {
				return err
			}
			errorDetails = sql.NullString{String: string(b), Valid: true}
		}

		_, err := tx.ExecContext(ctx, `
			INSERT INTO job_attempts (id, job_id, operation_id, node_id, attempt_number,
				status, command_id, claim_token, lease_until, processing_id,
				error_code, error_message, error_details, started_at, completed_at, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			a.ID.String(), a.JobID.String(), a.OperationID, a.NodeID, a.AttemptNumber,
			a.Status, a.CommandID, a.ClaimToken, a.LeaseUntil, nullString(a.ProcessingID),
			nullString(&a.Error.Code), nullString(&a.Error.Message), errorDetails,
			a.StartedAt, a.CompletedAt, a.Version,
		)
		return err
	})
}

func (r *JobAttemptRepository) GetByID(ctx context.Context, id job.AttemptID) (*job.JobAttempt, error) {
	var a job.JobAttempt
	var startedAt, completedAt sql.NullTime
	var processingID, errorCode, errorMessage, errorDetails sql.NullString

	err := r.db.QueryRowContext(ctx, `
		SELECT id, job_id, operation_id, node_id, attempt_number, status, command_id, claim_token,
			lease_until, processing_id, error_code, error_message, error_details, started_at, completed_at, version
		FROM job_attempts WHERE id = ?`, id.String()).
		Scan(&a.ID, &a.JobID, &a.OperationID, &a.NodeID, &a.AttemptNumber, &a.Status,
			&a.CommandID, &a.ClaimToken, &a.LeaseUntil, &processingID,
			&errorCode, &errorMessage, &errorDetails, &startedAt, &completedAt, &a.Version,
		)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ports.ErrOperationNotFound
	}
	if err != nil {
		return nil, err
	}

	if processingID.Valid {
		a.ProcessingID = &processingID.String
	}
	if startedAt.Valid {
		a.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		a.CompletedAt = &completedAt.Time
	}
	if errorCode.Valid || errorMessage.Valid {
		a.Error = &job.JobError{
			Code:    errorCode.String,
			Message: errorMessage.String,
		}
		if errorDetails.Valid && errorDetails.String != "" {
			_ = json.Unmarshal([]byte(errorDetails.String), &a.Error.Details)
		}
	}

	return &a, nil
}

func (r *JobAttemptRepository) GetActiveByJobID(ctx context.Context, jobID job.JobID) (*job.JobAttempt, error) {
	var a job.JobAttempt
	var startedAt, completedAt sql.NullTime
	var processingID, errorCode, errorMessage, errorDetails sql.NullString

	err := r.db.QueryRowContext(ctx, `
		SELECT id, job_id, operation_id, node_id, attempt_number, status, command_id, claim_token,
			lease_until, processing_id, error_code, error_message, error_details, started_at, completed_at, version
		FROM job_attempts
		WHERE job_id = ? AND status NOT IN (?, ?, ?)
		ORDER BY attempt_number DESC LIMIT 1`,
		jobID.String(), job.AttemptStatusSucceeded, job.AttemptStatusFailed, job.AttemptStatusCancelled,
	).Scan(
		&a.ID, &a.JobID, &a.OperationID, &a.NodeID, &a.AttemptNumber, &a.Status,
		&a.CommandID, &a.ClaimToken, &a.LeaseUntil, &processingID,
		&errorCode, &errorMessage, &errorDetails, &startedAt, &completedAt, &a.Version,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if processingID.Valid {
		a.ProcessingID = &processingID.String
	}
	if startedAt.Valid {
		a.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		a.CompletedAt = &completedAt.Time
	}
	if errorCode.Valid || errorMessage.Valid {
		a.Error = &job.JobError{
			Code:    errorCode.String,
			Message: errorMessage.String,
		}
		if errorDetails.Valid && errorDetails.String != "" {
			_ = json.Unmarshal([]byte(errorDetails.String), &a.Error.Details)
		}
	}

	return &a, nil
}

func (r *JobAttemptRepository) Update(ctx context.Context, a *job.JobAttempt) error {
	var startedAt, completedAt sql.NullTime
	if a.StartedAt != nil {
		startedAt = sql.NullTime{Time: *a.StartedAt, Valid: true}
	}
	if a.CompletedAt != nil {
		completedAt = sql.NullTime{Time: *a.CompletedAt, Valid: true}
	}
	var errorCode, errorMessage, errorDetails sql.NullString
	if a.Error != nil {
		if a.Error.Code != "" {
			errorCode = sql.NullString{String: a.Error.Code, Valid: true}
		}
		if a.Error.Message != "" {
			errorMessage = sql.NullString{String: a.Error.Message, Valid: true}
		}
		if a.Error.Details != nil {
			b, err := json.Marshal(a.Error.Details)
			if err != nil {
				return err
			}
			errorDetails = sql.NullString{String: string(b), Valid: true}
		}
	}

	_, err := r.db.ExecContext(ctx, `
		UPDATE job_attempts SET status = ?, processing_id = ?, error_code = ?, error_message = ?,
			error_details = ?, started_at = ?, completed_at = ?, version = ?
		WHERE id = ? AND version = ?`,
		a.Status, nullString(a.ProcessingID), errorCode, errorMessage, errorDetails,
		startedAt, completedAt, a.Version+1, a.ID, a.Version,
	)
	return err
}
