# Roadmap

Areas for review, without dated delivery promises:

- Exercise migrations and compare-and-swap behavior on live PostgreSQL and MySQL.
- Add an explicit, bounded archive/pruning API only after retention and idempotency expiry policy is
  defined.
- Evaluate provider-specific transaction/isolation behavior using the shared SagaStore contract.

The alpha does not claim encryption at field level, automated cleanup, or live provider validation.
