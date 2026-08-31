package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type executor struct {
	ID                 string         `json:"id"`
	ApplicationName    string         `json:"application_name"`
	ApplicationVersion string         `json:"application_version"`
	Status             string         `json:"status"`
	Hostname           *string        `json:"hostname,omitempty"`
	Language           *string        `json:"language,omitempty"`
	DBOSVersion        *string        `json:"dbos_version,omitempty"`
	Metadata           map[string]any `json:"executor_metadata,omitempty"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

type store struct {
	pool   *pgxpool.Pool
	schema string
}

func newStore(ctx context.Context, databaseURL, schema string) (*store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	s := &store{pool: pool, schema: pgx.Identifier{schema}.Sanitize()}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *store) close() { s.pool.Close() }

func (s *store) migrate(ctx context.Context) error {
	q := fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS %[1]s;

CREATE TABLE IF NOT EXISTS %[1]s.hosts (
    id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    address TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS %[1]s.applications (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    executor_timeout_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS %[1]s.application_versions (
    application_id TEXT NOT NULL REFERENCES %[1]s.applications(id) ON DELETE CASCADE,
    version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (application_id, version)
);

CREATE TABLE IF NOT EXISTS %[1]s.executors (
    id TEXT PRIMARY KEY,
    application_id TEXT NOT NULL REFERENCES %[1]s.applications(id) ON DELETE CASCADE,
    application_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('HEALTHY', 'DISCONNECTED', 'DEAD')),
    host_id TEXT REFERENCES %[1]s.hosts(id),
    hostname TEXT,
    language TEXT,
    dbos_version TEXT,
    executor_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (application_id, application_version)
        REFERENCES %[1]s.application_versions(application_id, version)
);

CREATE INDEX IF NOT EXISTS executors_recovery_idx
    ON %[1]s.executors(status, updated_at);`, s.schema)
	_, err := s.pool.Exec(ctx, q)
	return err
}

func (s *store) registerHost(ctx context.Context, id, hostname, address string) error {
	q := fmt.Sprintf(`INSERT INTO %s.hosts (id, hostname, address)
VALUES ($1, $2, $3)`, s.schema)
	_, err := s.pool.Exec(ctx, q, id, hostname, address)
	return err
}

func (s *store) heartbeatHost(ctx context.Context, id string) error {
	q := fmt.Sprintf(`UPDATE %s.hosts SET updated_at = now() WHERE id = $1`, s.schema)
	_, err := s.pool.Exec(ctx, q, id)
	return err
}

func (s *store) unregisterHost(ctx context.Context, id string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	q := fmt.Sprintf(`UPDATE %s.executors
SET status = 'DISCONNECTED', host_id = NULL, updated_at = now()
WHERE host_id = $1`, s.schema)
	if _, err := tx.Exec(ctx, q, id); err != nil {
		return err
	}
	q = fmt.Sprintf(`DELETE FROM %s.hosts WHERE id = $1`, s.schema)
	if _, err := tx.Exec(ctx, q, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type executorInfo struct {
	ExecutorID         string         `json:"executor_id"`
	ApplicationVersion string         `json:"application_version"`
	Hostname           *string        `json:"hostname"`
	Language           *string        `json:"language"`
	DBOSVersion        *string        `json:"dbos_version"`
	ExecutorMetadata   map[string]any `json:"executor_metadata"`
}

func (s *store) registerExecutor(ctx context.Context, applicationName, hostID string, timeout time.Duration, info executorInfo) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	appID := applicationName
	q := fmt.Sprintf(`INSERT INTO %s.applications (id, name, executor_timeout_ms)
VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET executor_timeout_ms = EXCLUDED.executor_timeout_ms`, s.schema)
	if _, err := tx.Exec(ctx, q, appID, applicationName, timeout.Milliseconds()); err != nil {
		return err
	}
	q = fmt.Sprintf(`INSERT INTO %s.application_versions (application_id, version)
VALUES ($1, $2) ON CONFLICT DO NOTHING`, s.schema)
	if _, err := tx.Exec(ctx, q, appID, info.ApplicationVersion); err != nil {
		return err
	}
	q = fmt.Sprintf(`INSERT INTO %s.executors (
    id, application_id, application_version, status, host_id,
    hostname, language, dbos_version, executor_metadata, updated_at
) VALUES ($1, $2, $3, 'HEALTHY', $4, $5, $6, $7, $8, now())
ON CONFLICT (id) DO UPDATE SET
    application_id = EXCLUDED.application_id,
    application_version = EXCLUDED.application_version,
    status = 'HEALTHY',
    host_id = EXCLUDED.host_id,
    hostname = EXCLUDED.hostname,
    language = EXCLUDED.language,
    dbos_version = EXCLUDED.dbos_version,
    executor_metadata = EXCLUDED.executor_metadata,
    updated_at = now()`, s.schema)
	if _, err := tx.Exec(ctx, q, info.ExecutorID, appID, info.ApplicationVersion, hostID,
		info.Hostname, info.Language, info.DBOSVersion, info.ExecutorMetadata); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *store) disconnectExecutor(ctx context.Context, id, hostID string) error {
	q := fmt.Sprintf(`UPDATE %s.executors
SET status = 'DISCONNECTED', host_id = NULL, updated_at = now()
WHERE id = $1 AND host_id = $2 AND status = 'HEALTHY'`, s.schema)
	_, err := s.pool.Exec(ctx, q, id, hostID)
	return err
}

func (s *store) expireDisconnected(ctx context.Context) (int64, error) {
	q := fmt.Sprintf(`UPDATE %s.executors e
SET status = 'DEAD', updated_at = now()
FROM %s.applications a
WHERE e.application_id = a.id
  AND e.status = 'DISCONNECTED'
  AND e.updated_at < now() - (a.executor_timeout_ms * interval '1 millisecond')`, s.schema, s.schema)
	result, err := s.pool.Exec(ctx, q)
	return result.RowsAffected(), err
}

func (s *store) deadExecutors(ctx context.Context) ([]executor, error) {
	return s.queryExecutors(ctx, `WHERE e.status = 'DEAD' ORDER BY e.updated_at`)
}

func (s *store) listExecutors(ctx context.Context) ([]executor, error) {
	return s.queryExecutors(ctx, `ORDER BY e.created_at, e.id`)
}

func (s *store) queryExecutors(ctx context.Context, suffix string, args ...any) ([]executor, error) {
	q := fmt.Sprintf(`SELECT e.id, a.name, e.application_version, e.status,
       e.hostname, e.language, e.dbos_version, e.executor_metadata, e.updated_at
FROM %s.executors e JOIN %s.applications a ON a.id = e.application_id %s`, s.schema, s.schema, suffix)
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []executor
	for rows.Next() {
		var e executor
		if err := rows.Scan(&e.ID, &e.ApplicationName, &e.ApplicationVersion, &e.Status,
			&e.Hostname, &e.Language, &e.DBOSVersion, &e.Metadata, &e.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *store) matchingHealthy(ctx context.Context, applicationName, version, excludeID string) (*executor, error) {
	q := fmt.Sprintf(`SELECT e.id, a.name, e.application_version, e.status,
       e.hostname, e.language, e.dbos_version, e.executor_metadata, e.updated_at
FROM %s.executors e JOIN %s.applications a ON a.id = e.application_id
WHERE a.name = $1 AND e.application_version = $2 AND e.status = 'HEALTHY' AND e.id <> $3
ORDER BY random() LIMIT 1`, s.schema, s.schema)
	var e executor
	err := s.pool.QueryRow(ctx, q, applicationName, version, excludeID).Scan(
		&e.ID, &e.ApplicationName, &e.ApplicationVersion, &e.Status,
		&e.Hostname, &e.Language, &e.DBOSVersion, &e.Metadata, &e.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return &e, err
}

func (s *store) anyHealthy(ctx context.Context, applicationName string) (*executor, error) {
	q := fmt.Sprintf(`SELECT e.id, a.name, e.application_version, e.status,
       e.hostname, e.language, e.dbos_version, e.executor_metadata, e.updated_at
FROM %s.executors e JOIN %s.applications a ON a.id = e.application_id
WHERE a.name = $1 AND e.status = 'HEALTHY'
ORDER BY random() LIMIT 1`, s.schema, s.schema)
	var e executor
	err := s.pool.QueryRow(ctx, q, applicationName).Scan(
		&e.ID, &e.ApplicationName, &e.ApplicationVersion, &e.Status,
		&e.Hostname, &e.Language, &e.DBOSVersion, &e.Metadata, &e.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return &e, err
}

func (s *store) deleteDeadExecutor(ctx context.Context, id string) (bool, error) {
	q := fmt.Sprintf(`DELETE FROM %s.executors WHERE id = $1 AND status = 'DEAD'`, s.schema)
	result, err := s.pool.Exec(ctx, q, id)
	return result.RowsAffected() == 1, err
}
