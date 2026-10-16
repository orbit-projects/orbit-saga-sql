# Development and validation

The local adapter tests use `orbit-sql-sqlite` and a temporary database to verify table setup,
snapshot serialization, uniqueness, and stale-version rejection.

```bash
python -m pip install -e '.[dev]'
pytest
ruff check src tests
ruff format --check src tests
mypy
python -m build
```

Before release, run the same contract suite against PostgreSQL and MySQL, and validate the
checked-in migration with `orbit-migrations` in a disposable environment for each provider.
