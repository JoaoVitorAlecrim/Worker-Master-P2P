Title: Enforce PDF wire shapes for TCP/UDP and add strict tests

Summary
-------
This PR enforces the JSON wire shapes described in the project PDF (plano_proj_SD-26_1) by:

- Adding strict parsing and builders for master↔master envelopes (`common/protocol.py`).
- Emitting and parsing spec-format UDP election messages (`common/election.py`, `worker.py`).
- Removing forbidden fields from TCP worker<->master messages and ensuring `USER`-only task payloads.
- Adding tests that assert exact wire shapes and strict parsing behavior.
- Adding `docs/WIRE_SPEC.md` which documents the exact keys used on the wire.

Files changed
-------------
- core protocol and election helpers: `common/protocol.py`, `common/election.py`
- master/worker: `master.py`, `worker.py`
- tests: added strict wire-shape tests and envelope/election spec tests under `tests/`
- docs: `docs/WIRE_SPEC.md`

Why
---
To make the network contract deterministic and compatible with the project PDF. Tests were added to prevent regressions.

Checklist
---------
- [x] Tests added and passing (`python -m unittest discover -v tests`)
- [x] Linting and formatting applied (`black`, `ruff --fix`)
- [x] Branch: `feature/wire-spec-docs` pushed
- [ ] Create PR on GitHub (open link below)

Suggested PR body (copy/paste when creating PR):

Changes implement strict JSON wire shapes and add tests ensuring:

- Master ↔ Master envelopes use exact keys: `type`, `request_id`, `payload` (PDF-style spec).
- Worker ↔ Master TCP messages only include allowed keys (no `TASK_ID`, `WORKERS`, `AUTH_TOKEN` on wire).
- Master-to-master negotiation payloads use `master_id` / `workers_needed` and responses use `response_accepted` / `response_rejected` with `workers_offered` in the payload.
- UDP election messages remain unchanged in their selection rule; this PR preserves the existing election algorithm while keeping backward/forward compatible parsing.

All tests pass locally (`python -m unittest discover -v tests`) and linters/formatters were applied (`ruff`, `black`).

Create PR here:
https://github.com/JoaoVitorAlecrim/Worker-Master-P2P/pull/new/feature/wire-spec-docs
