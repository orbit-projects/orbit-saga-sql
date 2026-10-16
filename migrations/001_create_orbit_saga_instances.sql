-- Copyright 2026-present Orbit Contributors.
-- Licensed under the Apache License, Version 2.0.
--
-- Apply with orbit-migrations or a reviewed deployment migration. The explicit
-- SQLSagaStore.initialize_schema() helper is intended for local development.
CREATE TABLE orbit_saga_instances (
    saga_id VARCHAR(36) NOT NULL PRIMARY KEY,
    definition VARCHAR(63) NOT NULL,
    revision VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL,
    state_json TEXT NOT NULL,
    version BIGINT NOT NULL,
    CONSTRAINT orbit_saga_idempotency_unique
        UNIQUE (definition, idempotency_key)
);
