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
"""Provider-neutral SQL implementation of the Orbit SagaStore compare-and-swap contract."""

from __future__ import annotations

import asyncio
from uuid import UUID

from orbit_saga import SagaConcurrencyError, SagaSnapshot, SagaStatus, SagaStoreError
from orbit_sql import SQLDatabase, SQLRow

_TABLE = "orbit_saga_instances"


class SQLSagaStore:
    """Persist bounded saga snapshots through an injected Orbit ``SQLDatabase``.

    The adapter does not own or close the database. Schema initialization is explicit; production
    deployments should run it through a reviewed migration before application startup.
    """

    def __init__(self, database: SQLDatabase) -> None:
        """Bind a provider-neutral asynchronous SQL database without taking ownership."""
        if not isinstance(database, SQLDatabase):
            raise TypeError("database must implement the Orbit SQLDatabase contract.")
        self._database = database
        self._initialized = False
        self._initialize_lock = asyncio.Lock()

    async def initialize_schema(self) -> None:
        """Explicitly create the portable table and unique idempotency index for local use."""
        async with self._initialize_lock:
            if self._initialized:
                return
            try:
                await self._database.execute(
                    f"CREATE TABLE IF NOT EXISTS {_TABLE} ("
                    "saga_id VARCHAR(36) NOT NULL PRIMARY KEY, "
                    "definition VARCHAR(63) NOT NULL, "
                    "revision VARCHAR(64) NOT NULL, "
                    "idempotency_key VARCHAR(255) NOT NULL, "
                    "status VARCHAR(32) NOT NULL, "
                    "state_json TEXT NOT NULL, "
                    "version BIGINT NOT NULL, "
                    "UNIQUE (definition, idempotency_key))"
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                raise SagaStoreError("Saga SQL schema initialization failed.") from None
            self._initialized = True

    async def create(self, snapshot: SagaSnapshot) -> None:
        """Insert an instance; database uniqueness protects IDs and idempotency keys."""
        self._ensure_initialized()
        if snapshot.version != 0 or snapshot.status is not SagaStatus.PENDING:
            raise SagaStoreError("New saga instances must start pending at version zero.")
        try:
            await self._database.execute(
                f"INSERT INTO {_TABLE} "
                "(saga_id, definition, revision, idempotency_key, status, state_json, version) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                (
                    str(snapshot.saga_id),
                    snapshot.definition,
                    snapshot.revision,
                    snapshot.idempotency_key,
                    snapshot.status.value,
                    snapshot.to_state_json(),
                    snapshot.version,
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise SagaStoreError(
                "Saga SQL insert failed or violated a uniqueness constraint."
            ) from None

    async def get(self, saga_id: UUID) -> SagaSnapshot | None:
        """Load one persisted snapshot from the primary database."""
        self._ensure_initialized()
        try:
            row = await self._database.fetch_one(
                "SELECT saga_id, definition, revision, idempotency_key, status, "
                f"state_json, version FROM {_TABLE} WHERE saga_id = $1",
                (str(saga_id),),
            )
            return None if row is None else self._decode(row)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise SagaStoreError("Saga SQL read failed.") from None

    async def get_by_key(self, definition: str, idempotency_key: str) -> SagaSnapshot | None:
        """Resolve one idempotency key from the primary, preserving immediate write visibility."""
        self._ensure_initialized()
        try:
            row = await self._database.fetch_one(
                "SELECT saga_id, definition, revision, idempotency_key, status, "
                f"state_json, version FROM {_TABLE} WHERE definition = $1 "
                "AND idempotency_key = $2",
                (definition, idempotency_key),
            )
            return None if row is None else self._decode(row)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise SagaStoreError("Saga SQL key lookup failed.") from None

    async def list_incomplete(self, *, limit: int = 100) -> tuple[SagaSnapshot, ...]:
        """Return a bounded page of pending or interrupted snapshots in stable ID order."""
        self._ensure_initialized()
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1_000:
            raise ValueError("limit must be between 1 and 1,000.")
        statuses = (
            SagaStatus.PENDING.value,
            SagaStatus.RUNNING.value,
            SagaStatus.COMPENSATING.value,
            SagaStatus.COMPENSATION_REQUIRED.value,
        )
        try:
            rows = await self._database.fetch_all(
                "SELECT saga_id, definition, revision, idempotency_key, status, "
                f"state_json, version FROM {_TABLE} "
                "WHERE status IN ($1, $2, $3, $4) ORDER BY saga_id LIMIT $5",
                (*statuses, limit),
                limit=limit,
            )
            return tuple(self._decode(row) for row in rows)
        except asyncio.CancelledError:
            raise
        except SagaStoreError:
            raise
        except Exception:
            raise SagaStoreError("Saga SQL recovery scan failed.") from None

    async def save(self, snapshot: SagaSnapshot, *, expected_version: int) -> SagaSnapshot:
        """Atomically update only the expected version or raise a concurrency error."""
        self._ensure_initialized()
        if (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or snapshot.version != expected_version + 1
        ):
            raise SagaConcurrencyError("Saga instance version changed concurrently.")
        try:
            async with self._database.transaction() as transaction:
                result = await transaction.execute(
                    f"UPDATE {_TABLE} SET status = $1, state_json = $2, version = $3 "
                    "WHERE saga_id = $4 AND definition = $5 AND revision = $6 AND version = $7",
                    (
                        snapshot.status.value,
                        snapshot.to_state_json(),
                        snapshot.version,
                        str(snapshot.saga_id),
                        snapshot.definition,
                        snapshot.revision,
                        expected_version,
                    ),
                )
            if result.rowcount != 1:
                raise SagaConcurrencyError("Saga instance version changed concurrently.")
            return snapshot
        except asyncio.CancelledError:
            raise
        except SagaConcurrencyError:
            raise
        except Exception:
            raise SagaStoreError("Saga SQL update failed.") from None

    def _decode(self, row: SQLRow) -> SagaSnapshot:
        """Validate persisted scalar fields and bounded state JSON before returning a snapshot."""
        try:
            saga_id_value = row["saga_id"]
            definition = row["definition"]
            revision = row["revision"]
            key = row["idempotency_key"]
            status_value = row["status"]
            state_json = row["state_json"]
            version = row["version"]
            if not isinstance(saga_id_value, str) or not isinstance(status_value, str):
                raise ValueError
            if not isinstance(definition, str) or not isinstance(revision, str):
                raise ValueError
            if not isinstance(key, str) or not isinstance(state_json, str):
                raise ValueError
            if isinstance(version, bool) or not isinstance(version, int):
                raise ValueError
            return SagaSnapshot.from_state_json(
                saga_id=UUID(saga_id_value),
                definition=definition,
                revision=revision,
                idempotency_key=key,
                status=SagaStatus(status_value),
                state_json=state_json,
                version=version,
            )
        except Exception:
            raise SagaStoreError("Saga SQL row is invalid.") from None

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            raise SagaStoreError("Initialize the Saga SQL schema before using the store.")


__all__ = ["SQLSagaStore"]
