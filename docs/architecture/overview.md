# Architecture and schema

The adapter implements `orbit_saga.SagaStore` using `orbit_sql.SQLDatabase`. It relies only on
bound positional parameters, the primary database consistency path, and transaction-scoped
compare-and-swap updates. Provider errors are translated to sanitized `SagaStoreError` or
`SagaConcurrencyError` values.

The schema is a single table with a primary UUID, explicit definition revision, idempotency key,
status, bounded JSON state, and integer version. The composite unique constraint prevents a key
from starting another instance after a definition revision changes. SQL types are portable
across the supported Orbit SQL providers; live server behavior remains an external validation gate.

`SQLSagaStore` borrows its database and never closes it. `initialize_schema()` is explicit and
idempotent for local development. Production deployments should apply the checked-in migration
through reviewed migration tooling before starting application workers.
