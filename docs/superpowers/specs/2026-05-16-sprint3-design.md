# Sprint 3 Design - Farm-to-Farm Negotiation and Dynamic Worker Redirection

## Context

The project already has a working Master-Worker protocol with:
- `WORKER: ALIVE` presentation
- `TASK: HEARTBEAT` response
- `TASK: QUERY` task distribution
- `STATUS: OK|NOK` task reporting
- task tracking with pending / in-progress / completed states
- automatic reassignment when a worker disconnects

Sprint 3 extends the system to support:
- Farm-to-Farm negotiation when a master is saturated
- Dynamic redirection of workers to another master
- Automatic election of a new master if the current master disconnects

## User Decisions

The following implementation choices were confirmed:
- Neighbor farms are hardcoded
- Saturation threshold is hardcoded
- Current load uses `pending + in_progress`
- Idle workers are the criterion for borrowing
- Worker acquisition uses a proportional request based on load
- Simulation runs in a lab with 4 machines
- The primary test environment includes 1 farm with 1 master and 3 workers
- Other farms in the lab have their own independent codebases and masters
- A second farm may be launched when testing the negotiation path
- The master failure scenario is simulated by disconnecting the master from the network
- The system must support complete behavior, not a reduced demo
- Worker promotion is automatic when the master fails

## Goals

1. Detect when a master is saturated.
2. Request help from a known neighbor farm.
3. Receive temporary workers from that farm.
4. Redirect borrowed workers to the saturated master.
5. Release borrowed workers when the load drops.
6. Detect master failure and elect a new master automatically from the workers.
7. Preserve task continuity and keep task tracking consistent.

## Non-Goals

- Dynamic network discovery of masters
- Full consensus algorithm such as Raft or Paxos
- Persistent disk-backed state recovery
- Arbitrary number of masters beyond the lab scenario
- Reworking the existing worker task protocol beyond the minimum needed for Sprint 3

## Proposed Architecture

### 1. Static Farm Registry
Each farm master knows a list of peer farms at startup.

Example structure:
- `MASTER_ID`
- `host`
- `port`
- `priority`
- `status`

This registry is used for:
- help requests
- capacity checks
- failover election

### 2. Master Saturation Monitor
The master calculates load as:
- `current_load = pending_tasks + in_progress_tasks`

A master is considered saturated when:
- `current_load >= CAPACITY`

A master is considered eligible to release borrowed workers when:
- `current_load <= RELEASE_THRESHOLD`

Both values are hardcoded constants.

### 3. Farm-to-Farm Negotiation
When saturated, a farm master sends a help request to a configured neighbor farm.

Request payload concept:
- sender master id
- current load
- number of workers needed
- optional saturation reason

Response options:
- accept with available worker count
- reject if the neighbor cannot help

If accepted, the requesting master receives temporary workers from the helper farm.

This path is only active when at least one peer farm is configured and reachable.
The lab failover scenario does not depend on it.

### 4. Dynamic Worker Redirection
Borrowed workers receive a redirect command from their original master.

The worker then:
- disconnects from the current master
- reconnects to the target master
- keeps its own `WORKER_UUID`
- continues reporting as the same worker identity
- retains `SERVER_UUID` as the original owner for traceability

### 5. Automatic Master Election
If the current master disappears, workers detect the absence of heartbeat responses.

Election rule:
- all alive workers compare a deterministic priority value
- the worker with the highest priority becomes the new master
- tie-breaker: lexical order of `WORKER_UUID`

The chosen worker transitions into master mode in the same process.

This election is intentionally simple and local because the project scope is a laboratory implementation, not a full distributed consensus system.

## Protocol Additions

### Farm-to-Farm Negotiation
Planned message families:
- `MASTER: REQUEST_HELP`
- `MASTER: RESPONSE_ACCEPTED`
- `MASTER: RESPONSE_REJECTED`

### Worker Redirection
Planned message families:
- `TASK: REDIRECT`
- `TASK: RELEASE`
- `TASK: REGISTER_TEMPORARY_WORKER`

### Election and Failover
Planned message families:
- `MASTER: HEARTBEAT`
- `MASTER: ELECTION`
- `MASTER: BECOME_MASTER`

The exact JSON shape can be minimal as long as the semantic meaning is preserved and the fields are stable.

## Data Model Impact

The existing models will be extended with:
- peer farm metadata
- temporary worker ownership metadata
- election state
- borrowed worker flags
- origin master / current master distinction

The task model remains unchanged in its core lifecycle.

## Runtime Flow

### Normal Load
1. Master accepts tasks.
2. Monitor computes load.
3. If under threshold, no negotiation occurs.

### Saturated Load
1. Master reaches saturation.
2. Master selects a neighbor from the hardcoded list.
3. Master sends `REQUEST_HELP`.
4. Neighbor responds with available workers.
5. Original master redirects workers.
6. Borrowed workers connect and execute tasks.
7. When load drops, workers are released.

### Master Failure
1. Workers stop receiving heartbeat responses.
2. Election starts locally among alive workers.
3. One worker becomes the new master.
4. Remaining workers reconnect to the new master.
5. Task processing continues using the existing task manager logic.
6. The promoted node becomes the new master for that farm.

## Failure Handling

### If a help request fails
- Try the next hardcoded neighbor.
- If no neighbor accepts, keep processing locally.

### If a redirected worker disconnects
- Treat it as a normal worker failure.
- Reassign its in-progress tasks using the existing task manager.

### If the elected worker fails during promotion
- Restart election among the remaining alive workers.

## Implementation Boundaries

The feature should be split so that each file has one main responsibility:
- `master.py`: load monitor, negotiation, redirection, election coordination
- `worker.py`: redirection handling and master promotion path
- `common/models.py`: metadata extensions
- `common/task_manager.py`: worker and task state utilities

## Testing Strategy

### Lab Scenario Tests
1. Start 1 farm with 1 master and 3 workers.
2. Fill the master until saturation.
3. Verify a help request is sent to a neighbor farm.
4. Verify a worker is redirected and begins working for the target farm.
5. Disconnect the master from the network.
6. Verify workers detect the failure.
7. Verify one worker becomes the new master.
8. Verify the cluster continues processing tasks.

### Regression Tests
- Existing ALIVE / HEARTBEAT flow still works.
- Existing task tracking still works.
- Existing worker failure reassignment still works.
- Borrowed worker release does not lose task history.

## Open Questions Resolved by Design

- Saturation uses pending + in progress.
- Farms are hardcoded neighbors.
- Worker promotion is automatic.
- Election is deterministic and local.
- The system is designed for the lab’s four-machine setup.

## Success Criteria

Sprint 3 is complete when:
- a saturated master can borrow workers from a neighbor
- borrowed workers are redirected successfully
- load returns below release threshold and workers are released
- the system detects master failure
- a worker is promoted to master automatically
- task execution continues without losing the already tracked state
