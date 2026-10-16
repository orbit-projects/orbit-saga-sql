# Operations and recovery

Apply the checked-in migration before the application starts and grant the runtime database
identity only the table operations required by the recovery workflow. The SQL adapter reads from
the primary to avoid stale idempotency lookups and commits each state compare-and-swap atomically.
The surrounding business side effect is not in that SQL transaction.

Back up the table with the database's normal backup process. Define retention only after the
business recovery and audit window is approved. Deleting terminal rows releases their idempotency
keys and can permit an old request to execute again.

The SQLite integration tests use a temporary local database. Live PostgreSQL/MySQL tests,
replica/failover tests, backup/restore, and production migration review remain release gates.
