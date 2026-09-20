package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"update/internal/application/ports"
)

type IdempotencyRepository struct {
	db *DB
}

func NewIdempotencyRepository(db *DB) *IdempotencyRepository {
	return &IdempotencyRepository{db: db}
}

func (r *IdempotencyRepository) Store(ctx context.Context, key, method, path, fingerprint string, response any, expiresAt time.Time) error {
	responseJSON, err := json.Marshal(response)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO idempotency_keys (key, method, path, fingerprint, response, expires_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			fingerprint = VALUES(fingerprint), response = VALUES(response), expires_at = VALUES(expires_at)`,
		key, method, path, fingerprint, string(responseJSON), expiresAt,
	)
	return err
}

func (r *IdempotencyRepository) Get(ctx context.Context, key, method, path string) (string, []byte, time.Time, error) {
	var storedKey, storedMethod, storedPath, fingerprint string
	var responseJSON []byte
	var expiresAt time.Time

	err := r.db.QueryRowContext(ctx, `
		SELECT key, method, path, fingerprint, response, expires_at
		FROM idempotency_keys
		WHERE key = ? AND method = ? AND path = ? AND expires_at > NOW(6)`,
		key, method, path,
	).Scan(&storedKey, &storedMethod, &storedPath, &fingerprint, &responseJSON, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, time.Time{}, ports.ErrOperationNotFound
	}
	if err != nil {
		return "", nil, time.Time{}, err
	}

	return fingerprint, responseJSON, expiresAt, nil
}

func (r *IdempotencyRepository) Delete(ctx context.Context, key, method, path string) error {
	_, err := r.db.ExecContext(ctx, `
		DELETE FROM idempotency_keys WHERE key = ? AND method = ? AND path = ?`,
		key, method, path,
	)
	return err
}
