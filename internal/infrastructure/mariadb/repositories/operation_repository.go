package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"update/internal/application/ports"
	"update/internal/domain/operation"
)

type OperationRepository struct {
	db *DB
}

func NewOperationRepository(db *DB) *OperationRepository {
	return &OperationRepository{db: db}
}

func (r *OperationRepository) Create(ctx context.Context, op *operation.Operation) error {
	return r.db.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var inputFileSize sql.NullInt64
		var inputFileSHA256, inputFileName sql.NullString
		if op.InputFile != nil {
			if op.InputFile.SizeBytes > 0 {
				inputFileSize = sql.NullInt64{Int64: op.InputFile.SizeBytes, Valid: true}
			}
			if op.InputFile.SHA256 != "" {
				inputFileSHA256 = sql.NullString{String: op.InputFile.SHA256, Valid: true}
			}
			if op.InputFile.FileName != "" {
				inputFileName = sql.NullString{String: op.InputFile.FileName, Valid: true}
			}
		}

		_, err := tx.ExecContext(ctx, `
			INSERT INTO operations (id, status, stage, processor, retry_count, max_attempts,
				client_reference, created_at, updated_at, deadline,
				input_file_size, input_file_sha256, input_file_name,
				current_job_id, current_attempt_id, current_transfer_id,
				version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			op.ID.String(), op.Status, op.Stage, op.Processor, op.RetryCount, op.MaxAttempts,
			op.ClientReference, op.CreatedAt, op.UpdatedAt, op.Deadline,
			inputFileSize, inputFileSHA256, inputFileName,
			jobIDString(op.CurrentJobID), attemptIDString(op.CurrentAttemptID), transferIDString(op.CurrentTransferID),
			op.Version,
		)
		return err
	})
}

func (r *OperationRepository) GetByID(ctx context.Context, id operation.OperationID) (*operation.Operation, error) {
  row := r.db.QueryRowContext(ctx, `
    SELECT id, status, stage, processor, retry_count, max_attempts,
      client_reference, created_at, updated_at, deadline,
      input_file_size, input_file_sha256, input_file_name,
      current_job_id, current_attempt_id, current_transfer_id,
      cancel_requested_at, error_code, error_message, error_details, version
    FROM operations WHERE id = ?`, id.String())
  log.Printf("DB_OPERATION_GET operation_id=%s", id)
  op, err := rowToOperation(row)
  if err != nil {
    log.Printf("DB_OPERATION_GET_ERROR operation_id=%s error=%v", id, err)
    return nil, err
  }
  if op == nil {
    log.Printf("DB_OPERATION_NOT_FOUND operation_id=%s", id)
    return nil, nil
  }
  log.Printf("DB_OPERATION_READ_RESULT operation_id=%s status=%s stage=%s version=%d input_file_present=%t input_file_size=%d input_file_sha256=%s input_file_name=%s current_job_id=%s current_attempt_id=%s current_transfer_id=%s",
    op.ID, op.Status, op.Stage, op.Version, op.InputFile != nil, op.InputFile.SizeBytes, op.InputFile.SHA256, op.InputFile.FileName,
    op.CurrentJobID.String(), op.CurrentAttemptID.String(), op.CurrentTransferID.String())
  return op, nil
}

func (r *OperationRepository) GetByIdempotencyKey(ctx context.Context, key string) (*operation.Operation, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT o.id, o.status, o.stage, o.processor, o.retry_count, o.max_attempts,
			o.client_reference, o.created_at, o.updated_at, o.deadline,
			o.input_file_size, o.input_file_sha256, o.input_file_name,
			o.current_job_id, o.current_attempt_id, o.current_transfer_id,
			o.cancel_requested_at, o.error_code, o.error_message, o.error_details, o.version
		FROM operations o
		JOIN idempotency_keys i ON i.key = ?
		WHERE o.id = i.response->>'$.operation_id'`, key)
	return rowToOperation(row)
}

func (r *OperationRepository) Update(ctx context.Context, op *operation.Operation) error {
  log.Printf("DB_OPERATION_UPDATE operation_id=%s old_version=%d new_version=%d status=%s stage=%s input_file_size=%d input_file_sha256=%s input_file_name=%s current_job_id=%s current_attempt_id=%s current_transfer_id=%s",
    op.ID, op.Version, op.Version+1, op.Status, op.Stage,
    op.InputFile.SizeBytes, op.InputFile.SHA256, op.InputFile.FileName,
    op.CurrentJobID.String(), op.CurrentAttemptID.String(), op.CurrentTransferID.String())
  
  var errorCode, errorMessage, errorDetails sql.NullString
  if op.Error != nil {
    if op.Error.Code != "" {
      errorCode = sql.NullString{String: op.Error.Code, Valid: true}
    }
    if op.Error.Message != "" {
      errorMessage = sql.NullString{String: op.Error.Message, Valid: true}
    }
    if op.Error.Details != nil {
      b, err := json.Marshal(op.Error.Details)
      if err != nil {
        return err
      }
      errorDetails = sql.NullString{String: string(b), Valid: true}
    }
  }

  var inputFileSize sql.NullInt64
  var inputFileSHA256, inputFileName sql.NullString
  if op.InputFile != nil {
    inputFileSize = sql.NullInt64{Int64: op.InputFile.SizeBytes, Valid: true}
    if op.InputFile.SHA256 != "" {
      inputFileSHA256 = sql.NullString{String: op.InputFile.SHA256, Valid: true}
    }
    if op.InputFile.FileName != "" {
      inputFileName = sql.NullString{String: op.InputFile.FileName, Valid: true}
    }
  }

  result, err := r.db.ExecContext(ctx, `
    UPDATE operations SET status = ?, stage = ?, processor = ?, retry_count = ?,
      updated_at = ?, current_job_id = ?, current_attempt_id = ?, current_transfer_id = ?,
      cancel_requested_at = ?, error_code = ?, error_message = ?, error_details = ?,
      input_file_size = ?, input_file_sha256 = ?, input_file_name = ?,
      version = ?
    WHERE id = ? AND version = ?`,
    op.Status, op.Stage, op.Processor, op.RetryCount,
    op.UpdatedAt, jobIDString(op.CurrentJobID), attemptIDString(op.CurrentAttemptID), transferIDString(op.CurrentTransferID),
    op.CancelRequestedAt, errorCode, errorMessage, errorDetails,
    inputFileSize, inputFileSHA256, inputFileName,
    op.Version+1, op.ID, op.Version,
  )
  if err != nil {
    log.Printf("DB_OPERATION_UPDATE_ERROR operation_id=%s error=%v", op.ID, err)
    return err
  }
  rowsAffected, err := result.RowsAffected()
  if err != nil {
    log.Printf("DB_OPERATION_UPDATE_ERROR operation_id=%s error=%v", op.ID, err)
    return err
  }
  if rowsAffected == 0 {
    log.Printf("DB_OPERATION_UPDATE_ZERO_ROWS operation_id=%s expected_version=%d", op.ID, op.Version)
    return nil
  }
  log.Printf("DB_OPERATION_UPDATE_OK operation_id=%s rows_affected=%d new_version=%d", op.ID, rowsAffected, op.Version+1)
  return nil
}

func (r *OperationRepository) Transition(ctx context.Context, id operation.OperationID, from, to operation.OperationStatus, version int) (*operation.Operation, error) {
	var op *operation.Operation
	err := r.db.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE operations 
			SET status = ?, stage = ?, updated_at = NOW(6), version = version + 1
			WHERE id = ? AND status = ? AND version = ?`,
			to, stageForStatus(to), id.String(), from, version,
		)
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			return ports.ErrOperationNotFound
		}

		// Fetch updated operation
		row := tx.QueryRowContext(ctx, `
			SELECT id, status, stage, processor, retry_count, max_attempts,
				client_reference, created_at, updated_at, deadline,
				input_file_size, input_file_sha256, input_file_name,
				current_job_id, current_attempt_id, current_transfer_id,
				cancel_requested_at, error_code, error_message, error_details, version
			FROM operations WHERE id = ?`, id.String())
		op, err = rowToOperation(row)
		return err
	})
	if err != nil {
		return nil, err
	}
	return op, nil
}

func (r *OperationRepository) List(ctx context.Context, filter ports.OperationFilter) ([]*operation.Operation, error) {
	query := `
		SELECT id, status, stage, processor, retry_count, max_attempts,
			client_reference, created_at, updated_at, deadline,
			input_file_size, input_file_sha256, input_file_name,
			current_job_id, current_attempt_id, current_transfer_id,
			cancel_requested_at, error_code, error_message, error_details, version
		FROM operations WHERE 1=1`
	args := []any{}

	if filter.Status != nil {
		query += " AND status = ?"
		args = append(args, *filter.Status)
	}
	if filter.Processor != nil {
		query += " AND processor = ?"
		args = append(args, *filter.Processor)
	}
	if filter.Cursor != nil {
		query += " AND id > ?"
		args = append(args, *filter.Cursor)
	}

	query += " ORDER BY id LIMIT ?"
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ops []*operation.Operation
	for rows.Next() {
		op, err := rowToOperationFromRows(rows)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}

func rowToOperationFromRows(rows *sql.Rows) (*operation.Operation, error) {
	var op operation.Operation
	var inputFileSize sql.NullInt64
	var inputFileSHA256, inputFileName, currentJobID, currentAttemptID, currentTransferID sql.NullString
	var cancelRequestedAt sql.NullTime
	var errorCode, errorMessage, errorDetails sql.NullString

	err := rows.Scan(
		&op.ID, &op.Status, &op.Stage, &op.Processor, &op.RetryCount, &op.MaxAttempts,
		&op.ClientReference, &op.CreatedAt, &op.UpdatedAt, &op.Deadline,
		&inputFileSize, &inputFileSHA256, &inputFileName,
		&currentJobID, &currentAttemptID, &currentTransferID,
		&cancelRequestedAt, &errorCode, &errorMessage, &errorDetails, &op.Version,
	)
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

func stageForStatus(status operation.OperationStatus) operation.OperationStage {
	switch status {
	case operation.StatusCreated:
		return operation.StageWaitingForUpload
	case operation.StatusQueued:
		return operation.StageQueued
	case operation.StatusDispatched:
		return operation.StageWaitingForAck
	case operation.StatusTransferring:
		return operation.StageTransferring
	case operation.StatusTransferred:
		return operation.StageVerifying
	case operation.StatusProcessing:
		return operation.StageProcessing
	case operation.StatusCompleted:
		return operation.StageCompleted
	case operation.StatusFailed:
		return operation.StageFailed
	case operation.StatusTimeout:
		return operation.StageTimeout
	case operation.StatusCancelled:
		return operation.StageCancelled
	default:
		return operation.StageWaitingForUpload
	}
}
