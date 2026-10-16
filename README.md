# Orbit Saga SQL

`orbit-saga-sql` implements the `orbit-saga` persistence contract over the provider-neutral
`orbit-sql` API. It supports the SQL adapters chosen by the application, including SQLite,
PostgreSQL, and MySQL, without importing vendor SDKs.

```bash
python -m pip install orbit-saga orbit-saga-sql orbit-sql orbit-sql-sqlite
```

```python
from orbit_saga_sql import SQLSagaStore

store = SQLSagaStore(sql_database)
await store.initialize_schema()  # development only; production should apply the migration
```

The store does not own the database connection. Reads use the primary consistency path. Inserts
enforce a unique `(definition, idempotency_key)` key across revisions; updates use a transaction and
optimistic version comparison. Safe errors omit SQL, input, outputs, and driver details.

Apply [`migrations/001_create_orbit_saga_instances.sql`](migrations/001_create_orbit_saga_instances.sql)
through `orbit-migrations` or your reviewed deployment migration process. Saga state is stored as
bounded JSON text. `list_incomplete()` returns a stable bounded page for `SagaRecoveryWorker` but
does not claim rows exclusively; `save()` compare-and-swap remains the transition guard when
multiple workers see one instance. The adapter does not encrypt data or prune records, and it does
not claim live PostgreSQL/MySQL compatibility without provider tests.

## Documentation

See the [architecture](docs/architecture/overview.md),
[operations and recovery](docs/operations/README.md), [security](docs/security/overview.md), and
[development](docs/development/README.md) guides.

## Local checks

```bash
python -m pip install -e '.[dev]'
pytest
ruff check src tests
ruff format --check src tests
mypy
```

Licensed under Apache-2.0.
