# Protocol Compliance and Internal Consensus Election Design

Date: 2026-05-30

## Goal
Align all network JSON with the official PDF protocol and add an internal, same-farm consensus election so only one worker becomes master after master failure.

## Scope
- Enforce PDF message schemas for Worker<->Master (Sprint 01/02) and Master<->Master (Sprint 03).
- Remove extra fields from network payloads not defined by the PDF.
- Keep internal logic as needed, but never leak extra fields to other farms.
- Implement internal election via UDP broadcast (same farm only), with a single winner.

## Non-goals
- No changes to external farm interoperability beyond exact PDF payloads.
- No new inter-farm message types.
- No cryptographic auth between farms.

## References
- Official PDF: docs/plano_proj_SD-26_1 (2).pdf

---

## Protocol Compliance (Network Boundary)

### General rules
- Message delimiter: newline ("\n").
- Strict parsing: ignore unknown fields but fail if required fields are missing.
- Case sensitivity:
  - Sprint 01/02 control values in upper case (ALIVE, QUERY, NO_TASK, OK, NOK, ACK).
  - Sprint 03 message types in lower case (request_help, response_accepted, response_rejected, command_redirect, register_temporary_worker, command_release, notify_worker_returned).

### Sprint 01: Heartbeat (Worker <-> Master)
Worker -> Master:
```json
{"SERVER_UUID":"Master_A","TASK":"HEARTBEAT"}
```
Master -> Worker:
```json
{"SERVER_UUID":"Master_A","TASK":"HEARTBEAT","RESPONSE":"ALIVE"}
```

### Sprint 02: Task cycle (Worker <-> Master)
Worker presentation / request (local or borrowed):
```json
{"WORKER":"ALIVE","WORKER_UUID":"W-123"}
```
Optional when borrowed:
```json
{"WORKER":"ALIVE","WORKER_UUID":"W-999","SERVER_UUID":"Master-B"}
```
Master -> Worker (task available):
```json
{"TASK":"QUERY","USER":"Michel"}
```
Master -> Worker (no task):
```json
{"TASK":"NO_TASK"}
```
Worker -> Master (status):
```json
{"STATUS":"OK","TASK":"QUERY","WORKER_UUID":"W-123"}
```
Or:
```json
{"STATUS":"NOK","TASK":"QUERY","WORKER_UUID":"W-123"}
```
Master -> Worker (ack):
```json
{"STATUS":"ACK","WORKER_UUID":"W-123"}
```

### Sprint 03: Master <-> Master negotiation
Generic envelope:
```json
{"type":"request_help","request_id":"uuid","payload":{}}
```

Request help (Master A -> Master B):
```json
{
  "type":"request_help",
  "request_id":"uuid",
  "payload":{
    "master_id":"A",
    "current_load":150,
    "capacity":100,
    "workers_needed":2
  }
}
```

Accepted (Master B -> Master A):
```json
{
  "type":"response_accepted",
  "request_id":"uuid",
  "payload":{
    "workers_offered":2,
    "worker_details":[
      {"id":"B1","address":"ip:port"},
      {"id":"B2","address":"ip:port"}
    ]
  }
}
```

Rejected (Master B -> Master A):
```json
{
  "type":"response_rejected",
  "request_id":"uuid",
  "payload":{
    "reason":"high_load"
  }
}
```

Command redirect (Master B -> Worker B1):
```json
{
  "type":"command_redirect",
  "request_id":"uuid",
  "payload":{
    "new_master_address":"ip_master_A:port"
  }
}
```

Register temporary worker (Worker B1 -> Master A):
```json
{
  "type":"register_temporary_worker",
  "request_id":"uuid",
  "payload":{
    "worker_id":"B1",
    "original_master_address":"ip_master_B:port"
  }
}
```

Command release (Master A -> Worker B1):
```json
{
  "type":"command_release",
  "request_id":"uuid",
  "payload":{
    "original_master_address":"ip_master_B:port"
  }
}
```

Notify worker returned (Master A -> Master B):
```json
{
  "type":"notify_worker_returned",
  "request_id":"uuid",
  "payload":{
    "worker_id":"B1"
  }
}
```

### Mapping from current code
- Remove network fields not in PDF: TASK_ID, OPERATION, VALUES, RESULT, WORKERS, AUTH_TOKEN, MASTER, REQUEST_ID (upper case), etc.
- Keep internal fields if needed, but never transmit them over TCP between farms or between worker and master.

---

## Internal Consensus Election (same farm only)

### Transport
- UDP broadcast on ELECTION_PORT (default 5200, override via env).
- Same farm only. No external farm traffic.

### Winner criterion
- Highest FREE_DISK_BYTES wins; tie break by WORKER_UUID (lexicographic).

### Election messages (internal only)
These are separate from the PDF protocol and never go to other farms:

Election start:
```json
{"election":"start","term":123,"from":"W-123","free_disk":999}
```
Vote:
```json
{"election":"vote","term":123,"from":"W-999","candidate":"W-999","free_disk":888}
```
Result:
```json
{"election":"result","term":123,"winner":"W-123","free_disk":999}
```

### Flow
1. After PROMOTE_THRESHOLD failures, a worker broadcasts election start.
2. Workers respond with vote messages containing their candidate (self or best known).
3. Initiator aggregates votes for a short window (1-2s) and broadcasts result.
4. Winner starts master; others update master target to winner.
5. If no result received in a timeout window, workers may start a new election term.

### Safety
- Only one winner announced per term.
- Ignore late votes from older terms.

---

## Internal data model changes
- Task payload in network is now only USER.
- Internally, Task can store user string and status; no external TASK_ID is sent.
- Worker registry no longer relies on WORKERS snapshots from master (removed from network). Election uses UDP for peer visibility.

---

## Configuration
- ELECTION_PORT=5200 (default), override allowed.
- PROMOTE_THRESHOLD, RECONNECT_DELAY, SOCKET_TIMEOUT remain configurable.
- MASTER_HOST/MASTER_PORT and SERVER_UUID map to master_id and addresses in Sprint 03 payloads.

---

## Error handling and logging
- Log and ignore unknown fields or unknown types (per PDF strict parsing rule).
- Fail gracefully and log when required fields are missing.
- Log master-to-master messages with request_id, type, timestamp.
- Log election start, votes received count, and elected winner.

---

## Testing alignment
- Update existing tests to validate PDF payloads (no extra fields).
- Add unit tests for message parsing strictness.
- Add integration test for election consensus: only one worker becomes master.
- Validate request_help timeout (5s) and retry next peer.

---

## Risks
- Removing WORKERS snapshots may reduce visibility; election must not depend on it.
- UDP broadcast may be blocked in some environments; default to loopback broadcast and allow port override.

