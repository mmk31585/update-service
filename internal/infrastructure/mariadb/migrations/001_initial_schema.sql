-- +goose NO TRANSACTION
-- +goose Up
-- Phase 03 — initial schema
-- Tables: operations, jobs, job_attempts, file_transfers, file_chunks,
--         operation_results, operation_events, idempotency_keys

-- operations: durable client-visible end-to-end request
CREATE TABLE IF NOT EXISTS operations (
    id                  VARCHAR(64) PRIMARY KEY,
    status              VARCHAR(32) NOT NULL,
    stage               VARCHAR(32) NOT NULL,
    processor           VARCHAR(128) NOT NULL,
    retry_count         INT NOT NULL DEFAULT 0,
    max_attempts        INT NOT NULL DEFAULT 3,
    client_reference    VARCHAR(256),
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    deadline            DATETIME(6),
    input_file_size     BIGINT,
    input_file_sha256   VARCHAR(64),
    input_file_name     VARCHAR(256),
    current_job_id      VARCHAR(64),
    current_attempt_id  VARCHAR(64),
    current_transfer_id VARCHAR(64),
    cancel_requested_at DATETIME(6),
    error_code          VARCHAR(64),
    error_message       VARCHAR(1024),
    error_details       JSON,
    version             INT NOT NULL DEFAULT 1,
    INDEX idx_operations_status (status),
    INDEX idx_operations_created_at (created_at),
    INDEX idx_operations_processor (processor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- jobs: one worker execution assignment per retry cycle
CREATE TABLE IF NOT EXISTS jobs (
    id              VARCHAR(64) PRIMARY KEY,
    operation_id    VARCHAR(64) NOT NULL,
    worker_node_id  VARCHAR(128) NOT NULL,
    status          VARCHAR(32) NOT NULL,
    processor       VARCHAR(128) NOT NULL,
    requirements    JSON,
    assigned_at     DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    dispatched_at   DATETIME(6),
    acknowledged_at DATETIME(6),
    completed_at    DATETIME(6),
    error_code      VARCHAR(64),
    error_message   VARCHAR(1024),
    error_details   JSON,
    version         INT NOT NULL DEFAULT 1,
    INDEX idx_jobs_operation_id (operation_id),
    INDEX idx_jobs_worker_node_id (worker_node_id),
    INDEX idx_jobs_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- job_attempts: one concrete execution of a job on one node
CREATE TABLE IF NOT EXISTS job_attempts (
    id              VARCHAR(64) PRIMARY KEY,
    job_id          VARCHAR(64) NOT NULL,
    operation_id    VARCHAR(64) NOT NULL,
    node_id         VARCHAR(128) NOT NULL,
    attempt_number  INT NOT NULL,
    status          VARCHAR(32) NOT NULL,
    command_id      VARCHAR(128) NOT NULL,
    claim_token     VARCHAR(128) NOT NULL,
    lease_until     DATETIME(6) NOT NULL,
    processing_id   VARCHAR(128),
    error_code      VARCHAR(64),
    error_message   VARCHAR(1024),
    error_details   JSON,
    started_at      DATETIME(6),
    completed_at    DATETIME(6),
    version         INT NOT NULL DEFAULT 1,
    INDEX idx_job_attempts_job_id (job_id),
    UNIQUE INDEX idx_job_attempts_command_id (command_id),
    INDEX idx_job_attempts_operation_id (operation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- file_transfers: one chunked movement of one input file for one attempt
CREATE TABLE IF NOT EXISTS file_transfers (
    id                      VARCHAR(64) PRIMARY KEY,
    attempt_id              VARCHAR(64) NOT NULL,
    operation_id            VARCHAR(64) NOT NULL,
    file_id                 VARCHAR(64) NOT NULL,
    source_node_id          VARCHAR(128) NOT NULL,
    destination_node_id     VARCHAR(128) NOT NULL,
    file_size_bytes         BIGINT NOT NULL,
    chunk_size_bytes        BIGINT NOT NULL,
    total_chunks            INT NOT NULL,
    file_checksum           VARCHAR(64) NOT NULL,
    checksum_algorithm      VARCHAR(32) NOT NULL DEFAULT 'sha256',
    status                  VARCHAR(32) NOT NULL,
    staging_path            VARCHAR(512) NOT NULL,
    staging_reservation_id  VARCHAR(128),
    deadline_at             DATETIME(6) NOT NULL,
    accepted_at             DATETIME(6),
    completed_at            DATETIME(6),
    received_chunks         INT NOT NULL DEFAULT 0,
    error_code              VARCHAR(64),
    error_message           VARCHAR(1024),
    error_details           JSON,
    version                 INT NOT NULL DEFAULT 1,
    INDEX idx_file_transfers_attempt_id (attempt_id),
    INDEX idx_file_transfers_operation_id (operation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- file_chunks: logical chunk metadata keyed by transfer_id + chunk_index
CREATE TABLE IF NOT EXISTS file_chunks (
    id              VARCHAR(192) PRIMARY KEY,
    transfer_id     VARCHAR(64) NOT NULL,
    chunk_index     INT NOT NULL,
    chunk_offset    BIGINT NOT NULL,
    chunk_size      BIGINT NOT NULL,
    checksum        VARCHAR(64) NOT NULL,
    publish_state   VARCHAR(32) NOT NULL,
    ack_state       VARCHAR(32) NOT NULL,
    received_at     DATETIME(6),
    acknowledged_at DATETIME(6),
    version         INT NOT NULL DEFAULT 1,
    UNIQUE INDEX idx_file_chunks_transfer_index (transfer_id, chunk_index),
    INDEX idx_file_chunks_transfer_id (transfer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- operation_results: one bounded structured JSON result per completed operation
CREATE TABLE IF NOT EXISTS operation_results (
    id              VARCHAR(64) PRIMARY KEY,
    operation_id    VARCHAR(64) NOT NULL UNIQUE,
    processor       VARCHAR(128) NOT NULL,
    schema_version  INT NOT NULL DEFAULT 1,
    value           JSON NOT NULL,
    produced_at     DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    expires_at      DATETIME(6),
    size_bytes      BIGINT NOT NULL,
    checksum        VARCHAR(64) NOT NULL,
    INDEX idx_operation_results_operation_id (operation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- operation_events: append-only audit/history facts
CREATE TABLE IF NOT EXISTS operation_events (
    id              VARCHAR(64) PRIMARY KEY,
    operation_id    VARCHAR(64) NOT NULL,
    event_type      VARCHAR(64) NOT NULL,
    job_id          VARCHAR(64),
    attempt_id      VARCHAR(64),
    transfer_id     VARCHAR(64),
    payload         JSON NOT NULL,
    correlation_id  VARCHAR(64) NOT NULL,
    issued_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    actor_node_id   VARCHAR(128) NOT NULL,
    version         INT NOT NULL DEFAULT 1,
    INDEX idx_operation_events_operation_id (operation_id),
    INDEX idx_operation_events_issued_at (issued_at),
    INDEX idx_operation_events_correlation_id (correlation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- idempotency_keys: request fingerprint replay guard
CREATE TABLE IF NOT EXISTS idempotency_keys (
    idempotency_key  VARCHAR(128) PRIMARY KEY,
    method           VARCHAR(16) NOT NULL,
    path             VARCHAR(256) NOT NULL,
    fingerprint      VARCHAR(256) NOT NULL,
    response         JSON NOT NULL,
    expires_at       DATETIME(6) NOT NULL,
    created_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_idempotency_keys_method_path (method(16), path(256))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- +goose Down
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS operation_events;
DROP TABLE IF EXISTS operation_results;
DROP TABLE IF EXISTS file_chunks;
DROP TABLE IF EXISTS file_transfers;
DROP TABLE IF EXISTS job_attempts;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS operations;