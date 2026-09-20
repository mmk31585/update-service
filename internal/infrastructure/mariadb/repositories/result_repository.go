package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"update/internal/domain/result"
)

type ResultRepository struct {
	db *DB
}

func NewResultRepository(db *DB) *ResultRepository {
	return &ResultRepository{db: db}
}

func (r *ResultRepository) Create(ctx context.Context, res *result.Result) error {
	valueJSON, err := json.Marshal(res.Value)
	if err != nil {
		return err
	}
	return r.db.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO operation_results (id, operation_id, processor, schema_version, value,
				produced_at, expires_at, size_bytes, checksum)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			res.ID.String(), res.OperationID, res.Processor, res.SchemaVersion, string(valueJSON),
			res.ProducedAt, res.ExpiresAt, res.SizeBytes, res.Checksum,
		)
		return err
	})
}

func (r *ResultRepository) GetByOperationID(ctx context.Context, operationID string) (*result.Result, error) {
	var res result.Result
	var valueJSON []byte
	var expiresAt sql.NullTime

	err := r.db.QueryRowContext(ctx, `
		SELECT id, operation_id, processor, schema_version, value, produced_at, expires_at, size_bytes, checksum
		FROM operation_results WHERE operation_id = ?`, operationID).
		Scan(&res.ID, &res.OperationID, &res.Processor, &res.SchemaVersion, &valueJSON,
			&res.ProducedAt, &expiresAt, &res.SizeBytes, &res.Checksum,
		)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if expiresAt.Valid {
		res.ExpiresAt = &expiresAt.Time
	}
	if valueJSON != nil {
		_ = json.Unmarshal(valueJSON, &res.Value)
	}

	return &res, nil
}
