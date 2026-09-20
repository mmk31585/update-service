package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"update/internal/application/ports"
	"update/internal/domain/job"
)

type FileTransferRepository struct {
	db *DB
}

func NewFileTransferRepository(db *DB) *FileTransferRepository {
	return &FileTransferRepository{db: db}
}

func (r *FileTransferRepository) Create(ctx context.Context, t *job.FileTransfer) error {
	return r.db.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO file_transfers (id, attempt_id, operation_id, file_id, source_node_id,
				destination_node_id, file_size_bytes, chunk_size_bytes, total_chunks, file_checksum,
				checksum_algorithm, status, staging_path, staging_reservation_id, deadline_at,
				received_chunks, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			t.ID.String(), t.AttemptID.String(), t.OperationID, t.FileID, t.SourceNodeID,
			t.DestinationNodeID, t.FileSizeBytes, t.ChunkSizeBytes, t.TotalChunks, t.FileChecksum,
			t.ChecksumAlgorithm, t.Status, t.StagingPath, nullString(t.StagingReservationID),
			t.DeadlineAt, t.ReceivedChunks, t.Version,
		)
		return err
	})
}

func (r *FileTransferRepository) GetByID(ctx context.Context, id job.TransferID) (*job.FileTransfer, error) {
	var t job.FileTransfer
	var acceptedAt, completedAt sql.NullTime
	var stagingReservationID, errorCode, errorMessage, errorDetails sql.NullString

	err := r.db.QueryRowContext(ctx, `
		SELECT id, attempt_id, operation_id, file_id, source_node_id, destination_node_id,
			file_size_bytes, chunk_size_bytes, total_chunks, file_checksum, checksum_algorithm,
			status, staging_path, staging_reservation_id, deadline_at, accepted_at, completed_at,
			received_chunks, error_code, error_message, error_details, version
		FROM file_transfers WHERE id = ?`, id.String()).
		Scan(&t.ID, &t.AttemptID, &t.OperationID, &t.FileID, &t.SourceNodeID, &t.DestinationNodeID,
			&t.FileSizeBytes, &t.ChunkSizeBytes, &t.TotalChunks, &t.FileChecksum, &t.ChecksumAlgorithm,
			&t.Status, &t.StagingPath, &stagingReservationID, &t.DeadlineAt, &acceptedAt, &completedAt,
			&t.ReceivedChunks, &errorCode, &errorMessage, &errorDetails, &t.Version,
		)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ports.ErrOperationNotFound
	}
	if err != nil {
		return nil, err
	}

	if stagingReservationID.Valid {
		t.StagingReservationID = &stagingReservationID.String
	}
	if acceptedAt.Valid {
		t.AcceptedAt = &acceptedAt.Time
	}
	if completedAt.Valid {
		t.CompletedAt = &completedAt.Time
	}
	if errorCode.Valid || errorMessage.Valid {
		t.Error = &job.JobError{
			Code:    errorCode.String,
			Message: errorMessage.String,
		}
		if errorDetails.Valid && errorDetails.String != "" {
			_ = json.Unmarshal([]byte(errorDetails.String), &t.Error.Details)
		}
	}

	return &t, nil
}

func (r *FileTransferRepository) GetByAttemptID(ctx context.Context, attemptID job.AttemptID) (*job.FileTransfer, error) {
	var t job.FileTransfer
	var acceptedAt, completedAt sql.NullTime
	var stagingReservationID, errorCode, errorMessage, errorDetails sql.NullString

	err := r.db.QueryRowContext(ctx, `
		SELECT id, attempt_id, operation_id, file_id, source_node_id, destination_node_id,
			file_size_bytes, chunk_size_bytes, total_chunks, file_checksum, checksum_algorithm,
			status, staging_path, staging_reservation_id, deadline_at, accepted_at, completed_at,
			received_chunks, error_code, error_message, error_details, version
		FROM file_transfers WHERE attempt_id = ? LIMIT 1`, attemptID.String()).
		Scan(&t.ID, &t.AttemptID, &t.OperationID, &t.FileID, &t.SourceNodeID, &t.DestinationNodeID,
			&t.FileSizeBytes, &t.ChunkSizeBytes, &t.TotalChunks, &t.FileChecksum, &t.ChecksumAlgorithm,
			&t.Status, &t.StagingPath, &stagingReservationID, &t.DeadlineAt, &acceptedAt, &completedAt,
			&t.ReceivedChunks, &errorCode, &errorMessage, &errorDetails, &t.Version,
		)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if stagingReservationID.Valid {
		t.StagingReservationID = &stagingReservationID.String
	}
	if acceptedAt.Valid {
		t.AcceptedAt = &acceptedAt.Time
	}
	if completedAt.Valid {
		t.CompletedAt = &completedAt.Time
	}
	if errorCode.Valid || errorMessage.Valid {
		t.Error = &job.JobError{
			Code:    errorCode.String,
			Message: errorMessage.String,
		}
		if errorDetails.Valid && errorDetails.String != "" {
			_ = json.Unmarshal([]byte(errorDetails.String), &t.Error.Details)
		}
	}

	return &t, nil
}

func (r *FileTransferRepository) Update(ctx context.Context, t *job.FileTransfer) error {
	var acceptedAt, completedAt sql.NullTime
	if t.AcceptedAt != nil {
		acceptedAt = sql.NullTime{Time: *t.AcceptedAt, Valid: true}
	}
	if t.CompletedAt != nil {
		completedAt = sql.NullTime{Time: *t.CompletedAt, Valid: true}
	}
	var errorCode, errorMessage, errorDetails sql.NullString
	if t.Error != nil {
		if t.Error.Code != "" {
			errorCode = sql.NullString{String: t.Error.Code, Valid: true}
		}
		if t.Error.Message != "" {
			errorMessage = sql.NullString{String: t.Error.Message, Valid: true}
		}
		if t.Error.Details != nil {
			b, err := json.Marshal(t.Error.Details)
			if err != nil {
				return err
			}
			errorDetails = sql.NullString{String: string(b), Valid: true}
		}
	}

	_, err := r.db.ExecContext(ctx, `
		UPDATE file_transfers SET status = ?, accepted_at = ?, completed_at = ?, received_chunks = ?,
			error_code = ?, error_message = ?, error_details = ?, version = ?
		WHERE id = ? AND version = ?`,
		t.Status, acceptedAt, completedAt, t.ReceivedChunks,
		errorCode, errorMessage, errorDetails,
		t.Version+1, t.ID, t.Version,
	)
	return err
}
