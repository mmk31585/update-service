package mariadb

import (
	"context"
	"database/sql"

	"update/internal/domain/job"
)

type FileChunkRepository struct {
	db *DB
}

func NewFileChunkRepository(db *DB) *FileChunkRepository {
	return &FileChunkRepository{db: db}
}

func (r *FileChunkRepository) Upsert(ctx context.Context, c *job.FileChunk) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO file_chunks (id, transfer_id, chunk_index, offset, chunk_size, checksum,
			publish_state, ack_state, received_at, acknowledged_at, version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			chunk_size = VALUES(chunk_size), checksum = VALUES(checksum),
			publish_state = VALUES(publish_state), ack_state = VALUES(ack_state),
			acknowledged_at = VALUES(acknowledged_at), version = version + 1`,
		c.ID.String(), c.TransferID.String(), c.ChunkIndex, c.Offset, c.ChunkSize, c.Checksum,
		c.PublishState, c.AckState, c.ReceivedAt, c.AcknowledgedAt, c.Version,
	)
	return err
}

func (r *FileChunkRepository) GetByTransferID(ctx context.Context, transferID job.TransferID) ([]*job.FileChunk, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, transfer_id, chunk_index, offset, chunk_size, checksum,
			publish_state, ack_state, received_at, acknowledged_at, version
		FROM file_chunks WHERE transfer_id = ? ORDER BY chunk_index ASC`,
		transferID.String(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []*job.FileChunk
	for rows.Next() {
		var c job.FileChunk
		var receivedAt, acknowledgedAt sql.NullTime

		err := rows.Scan(
			&c.ID, &c.TransferID, &c.ChunkIndex, &c.Offset, &c.ChunkSize, &c.Checksum,
			&c.PublishState, &c.AckState, &receivedAt, &acknowledgedAt, &c.Version,
		)
		if err != nil {
			return nil, err
		}

		if receivedAt.Valid {
			c.ReceivedAt = &receivedAt.Time
		}
		if acknowledgedAt.Valid {
			c.AcknowledgedAt = &acknowledgedAt.Time
		}

		chunks = append(chunks, &c)
	}

	return chunks, rows.Err()
}

func (r *FileChunkRepository) CountReceived(ctx context.Context, transferID job.TransferID) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM file_chunks WHERE transfer_id = ? AND ack_state = 'received'`,
		transferID.String(),
	).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
