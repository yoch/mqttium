# Formal models

TLA+ models of MQTTium's runtime ownership and ordering rules. Each model in
`models/` has a JSON sidecar declaring the outcome TLC must report for each
configuration. Run them with:

```bash
python tools/formal/run_tlc.py
```

See [`docs/formal-models.md`](../docs/formal-models.md) for the conventions,
expected outcomes and model statuses.
