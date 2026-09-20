package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"update/internal/application/ports"
	"update/internal/domain/job"
	"update/internal/domain/operation"
)

type Transaction func(ctx context.Context, tx *sql.Tx) error

type DB struct {
	*sql.DB
}

func NewDB(db *sql.DB) *DB {
	return &DB{DB: db}
}

func (db *DB) InTx(ctx context.Context, fn Transaction) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

var _ ports.OperationRepository = (*OperationRepository)(nil)
var _ ports.JobRepository = (*JobRepository)(nil)
var _ ports.JobAttemptRepository = (*JobAttemptRepository)(nil)
var _ ports.FileTransferRepository = (*FileTransferRepository)(nil)
var _ ports.FileChunkRepository = (*FileChunkRepository)(nil)
var _ ports.ResultRepository = (*ResultRepository)(nil)
var _ ports.IdempotencyRepository = (*IdempotencyRepository)(nil)
var _ ports.OperationEventRepository = (*OperationEventRepository)(nil)
var _ ports.NodeRepository = (*NodeRepository)(nil)

func nullString(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullTime(v *time.Time) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func jobIDString(v *operation.JobID) any {
	if v == nil {
		return nil
	}
	return v.String()
}

func attemptIDString(v *operation.AttemptID) any {
	if v == nil {
		return nil
	}
	return v.String()
}

func transferIDString(v *operation.TransferID) any {
	if v == nil {
		return nil
	}
	return v.String()
}

func rowToOperation(row *sql.Row) (*operation.Operation, error) {
	var op operation.Operation
	var inputFileSize sql.NullInt64
	var inputFileSHA256, inputFileName, currentJobID, currentAttemptID, currentTransferID sql.NullString
	var cancelRequestedAt sql.NullTime
	var errorCode, errorMessage, errorDetails sql.NullString

	err := row.Scan(
		&op.ID, &op.Status, &op.Stage, &op.Processor, &op.RetryCount, &op.MaxAttempts,
		&op.ClientReference, &op.CreatedAt, &op.UpdatedAt, &op.Deadline,
		&inputFileSize, &inputFileSHA256, &inputFileName,
		&currentJobID, &currentAttemptID, &currentTransferID,
		&cancelRequestedAt, &errorCode, &errorMessage, &errorDetails, &op.Version,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if inputFileSize.Valid || inputFileSHA256.Valid {
		op.InputFile = &operation.InputFile{
			SizeBytes: inputFileSize.Int64,
			SHA256:    inputFileSHA256.String,
			FileName:  inputFileName.String,
		}
	}
	if currentJobID.Valid {
		v := operation.JobID(currentJobID.String)
		op.CurrentJobID = &v
	}
	if currentAttemptID.Valid {
		v := operation.AttemptID(currentAttemptID.String)
		op.CurrentAttemptID = &v
	}
	if currentTransferID.Valid {
		v := operation.TransferID(currentTransferID.String)
		op.CurrentTransferID = &v
	}
	if cancelRequestedAt.Valid {
		op.CancelRequestedAt = &cancelRequestedAt.Time
	}
	if errorCode.Valid || errorMessage.Valid {
		op.Error = &operation.OperationError{
			Code:    errorCode.String,
			Message: errorMessage.String,
		}
		if errorDetails.Valid && errorDetails.String != "" {
			_ = json.Unmarshal([]byte(errorDetails.String), &op.Error.Details)
		}
	}

	return &op, nil
}

func rowToJob(row *sql.Row) (*job.Job, error) {
	var j job.Job
	var reqJSON []byte
	var dispatchedAt, acknowledgedAt, completedAt sql.NullTime
	var errorCode, errorMessage, errorDetails sql.NullString

	err := row.Scan(
		&j.ID, &j.OperationID, &j.WorkerNodeID, &j.Status, &j.Processor, &reqJSON,
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
