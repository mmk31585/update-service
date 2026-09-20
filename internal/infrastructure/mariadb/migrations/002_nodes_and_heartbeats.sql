-- +goose Up
-- Phase 1 — node registration and heartbeat

-- nodes: current identity, incarnation, health, and scheduling eligibility
CREATE TABLE IF NOT EXISTS nodes (
    node_id              VARCHAR(64) PRIMARY KEY,
    incarnation_id       VARCHAR(64) NOT NULL,
    incarnation_started_at DATETIME(6) NOT NULL,
    status               VARCHAR(32) NOT NULL,
    roles                JSON NOT NULL,
    heartbeat_sequence   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_heartbeat_at    DATETIME(6) NULL,
    heartbeat_deadline_at DATETIME(6) NULL,
    active_job_count     BIGINT UNSIGNED NOT NULL DEFAULT 0,
    active_transfer_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    max_concurrent_jobs  BIGINT UNSIGNED NULL,
    disk_free_bytes      BIGINT UNSIGNED NULL,
    disk_total_bytes     BIGINT UNSIGNED NULL,
    cpu_sample           JSON NULL,
    memory_sample        JSON NULL,
    self_checks          JSON NULL,
    draining             BOOLEAN NOT NULL DEFAULT FALSE,
    shutdown_deadline_at DATETIME(6) NULL,
    registered_at        DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at           DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    INDEX idx_nodes_status (status),
    INDEX idx_nodes_heartbeat_deadline (heartbeat_deadline_at),
    INDEX idx_nodes_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- node_heartbeats: immutable health samples for scheduling and recovery diagnostics
CREATE TABLE IF NOT EXISTS node_heartbeats (
    node_id              VARCHAR(64) NOT NULL,
    incarnation_id       VARCHAR(64) NOT NULL,
    sequence             BIGINT UNSIGNED NOT NULL,
    status               VARCHAR(32) NOT NULL,
    capabilities         JSON NOT NULL,
    active_jobs          BIGINT UNSIGNED NOT NULL,
    active_transfers     BIGINT UNSIGNED NOT NULL,
    resources            JSON NULL,
    self_checks          JSON NULL,
    draining             BOOLEAN NOT NULL DEFAULT FALSE,
    shutdown_deadline_at DATETIME(6) NULL,
    observed_at          DATETIME(6) NOT NULL,
    persisted_at         DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    heartbeat_deadline_at DATETIME(6) NOT NULL,
    PRIMARY KEY (node_id, incarnation_id, sequence),
    INDEX idx_node_heartbeats_node_observed (node_id, observed_at DESC),
    INDEX idx_node_heartbeats_status_deadline (status, heartbeat_deadline_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- +goose Down
DROP TABLE IF EXISTS node_heartbeats;
DROP TABLE IF EXISTS nodes;
