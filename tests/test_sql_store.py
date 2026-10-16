# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""SQLite contract tests for durable saga snapshots and compare-and-swap updates."""

from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest
from orbit_saga import SagaConcurrencyError, SagaSnapshot, SagaStatus, SagaStoreError
from orbit_sql_sqlite import SQLiteDatabase

from orbit_saga_sql import SQLSagaStore


@pytest.mark.asyncio
async def test_sql_store_persists_versions_and_definition_scoped_idempotency(tmp_path) -> None:
    database = SQLiteDatabase(tmp_path / "saga.sqlite")
    store = SQLSagaStore(database)
    snapshot = SagaSnapshot(
        saga_id=uuid4(),
        definition="orders",
        revision="2026-01",
        idempotency_key="order-42",
        status=SagaStatus.PENDING,
        input={"order": "o-42"},
        version=0,
    )
    try:
        with pytest.raises(SagaStoreError, match="Initialize"):
            await store.get(snapshot.saga_id)
        await store.initialize_schema()
        await store.create(snapshot)

        assert await store.get(snapshot.saga_id) == snapshot
        assert await store.get_by_key("orders", "order-42") == snapshot

        updated = SagaSnapshot(
            saga_id=snapshot.saga_id,
            definition=snapshot.definition,
            revision=snapshot.revision,
            idempotency_key=snapshot.idempotency_key,
            status=SagaStatus.RUNNING,
            input=snapshot.input,
            outputs=({"reservation": "r-1"},),
            version=1,
        )
        assert await store.save(updated, expected_version=0) == updated
        assert await store.get(snapshot.saga_id) == updated
        completed = SagaSnapshot(
            saga_id=snapshot.saga_id,
            definition=snapshot.definition,
            revision=snapshot.revision,
            idempotency_key=snapshot.idempotency_key,
            status=SagaStatus.COMPLETED,
            input=snapshot.input,
            outputs=updated.outputs,
            version=2,
        )
        assert await store.save(completed, expected_version=1) == completed
        assert await store.get(snapshot.saga_id) == completed
        with pytest.raises(SagaConcurrencyError):
            await store.save(completed, expected_version=1)
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_sql_store_enforces_idempotency_uniqueness_without_exposing_values(tmp_path) -> None:
    database = SQLiteDatabase(tmp_path / "unique.sqlite")
    store = SQLSagaStore(database)
    first = SagaSnapshot(
        saga_id=uuid4(),
        definition="payment",
        revision="2",
        idempotency_key="sensitive-customer-key",
        status=SagaStatus.PENDING,
        input={},
    )
    second = SagaSnapshot(
        saga_id=uuid4(),
        definition="payment",
        revision="1",
        idempotency_key="sensitive-customer-key",
        status=SagaStatus.PENDING,
        input={},
    )
    try:
        await store.initialize_schema()
        await store.create(first)
        with pytest.raises(SagaStoreError) as error:
            await store.create(second)
        assert "sensitive-customer-key" not in str(error.value)
    finally:
        await database.aclose()


@pytest.mark.asyncio
async def test_sql_recovery_scan_is_bounded_and_excludes_terminal_instances(tmp_path) -> None:
    database = SQLiteDatabase(tmp_path / "recovery.sqlite")
    store = SQLSagaStore(database)
    items = (
        ("pending", SagaStatus.PENDING),
        ("running", SagaStatus.RUNNING),
        ("compensating", SagaStatus.COMPENSATING),
        ("needs_compensation", SagaStatus.COMPENSATION_REQUIRED),
        ("completed", SagaStatus.COMPLETED),
        ("failed", SagaStatus.FAILED),
    )
    snapshots = tuple(
        SagaSnapshot(
            saga_id=uuid4(),
            definition="recovery_scan",
            revision="1",
            idempotency_key=key,
            status=SagaStatus.PENDING,
            input={},
        )
        for key, _status in items
    )
    try:
        await store.initialize_schema()
        for snapshot, (_, status) in zip(snapshots, items, strict=True):
            await store.create(snapshot)
            if status is not SagaStatus.PENDING:
                await store.save(replace(snapshot, status=status, version=1), expected_version=0)

        bounded = await store.list_incomplete(limit=2)
        all_incomplete = await store.list_incomplete(limit=10)

        assert len(bounded) == 2
        assert {snapshot.status for snapshot in all_incomplete} == {
            SagaStatus.PENDING,
            SagaStatus.RUNNING,
            SagaStatus.COMPENSATING,
            SagaStatus.COMPENSATION_REQUIRED,
        }
        assert len(all_incomplete) == 4
    finally:
        await database.aclose()
