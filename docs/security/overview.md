# Security guidance

The adapter persists caller-provided workflow input and operation outputs. Treat `state_json` as
sensitive application data: minimize its contents, do not log SQL parameters, and apply database
least privilege, TLS, encryption at rest, backup access controls, and retention rules. Credential
rotation follows the selected SQL provider and application lifecycle.

Adapter errors intentionally omit SQL statements, values, and provider messages. The adapter does
not perform field-level encryption or provide a key-management system. Validate those controls in
the deployed database.
