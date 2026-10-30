# Architecture Decision Records (ADR)

This document records the 9 core architectural and design decisions made for SmartDialer, their rationale, trade-offs, and explicit operational guarantees.

---

## 1. PostgreSQL as the Single Source of Truth

* **What I Chose**: All queue state, agent state, call lifecycles, and decision logs reside exclusively in relational PostgreSQL tables. Worker processes and threads are completely stateless.
* **Why**: Telephony and call centers require strict transactional consistency, row locking, and durable auditability. Distributing state across Redis or local memory introduces split-brain risks under failure.
* **What Problem It Solves**: Eliminates cache-invalidation bugs, out-of-sync agent availability counters, and lost state when workers crash.
* **What It Makes Harder**: Requires careful schema indexing, connection pooling management, and query optimization so PostgreSQL does not become an I/O bottleneck at massive scale.

---

## 2. `SELECT ... FOR UPDATE SKIP LOCKED` + Partial Unique Indexes

* **What I Chose**: Concurrency control uses row-level `FOR UPDATE SKIP LOCKED` for reads/claims, backed by database-enforced partial unique indexes (`uq_calls_active_agent`, `uq_calls_active_borrower`).
* **Why**: `SKIP LOCKED` allows concurrent worker threads to pull available agents and eligible borrowers concurrently without blocking each other or causing deadlocks.
* **What Problem It Solves**: Guarantees that two workers can never allocate or reserve the same agent or borrower at the same time, with zero deadlock retry loops.
* **What It Makes Harder**: Queries must be carefully written to ensure index eligibility; SQLite does not support `SKIP LOCKED`, necessitating real PostgreSQL even in test environments.

---

## 3. Dedicated Event Inbox Table

* **What I Chose**: Incoming telecom webhooks and poll events are written directly to a `call_events` inbox table via `INSERT ... ON CONFLICT (provider, event_id) DO NOTHING` before any business logic is executed.
* **Why**: Telephony providers deliver webhooks unpredictably, out-of-order, and often with duplicate transmissions under network retry conditions.
* **What Problem It Solves**: Decouples fast HTTP ingestion from downstream business logic and state transitions, guaranteeing idempotency at the database edge.
* **What It Makes Harder**: Adds a discrete asynchronous processing step; event processing latency depends on the processor polling interval.

---

## 4. Rank-Based Monotonic Finite State Machine (FSM)

* **What I Chose**: Call states are assigned strictly increasing integer ranks:
  $$\text{QUEUED}(0) < \text{RESERVED}(1) < \text{INITIATED}(2) < \text{RINGING}(3) < \text{ANSWERED}(4) < \text{CONNECTED}(5) < \text{TERMINAL}(6)$$
  An incoming event is applied only if $\text{Rank}(S_{\text{event}}) > \text{Rank}(S_{\text{current}})$. Terminal states absorb all subsequent events.
* **Why**: Network delays often cause later events (e.g., `COMPLETED`) to arrive at the server before earlier ones (e.g., `RINGING` or `ANSWERED`).
* **What Problem It Solves**: Completely eliminates out-of-order state corruption and prevents calls from moving backward into active states after finishing.
* **What It Makes Harder**: Requires explicit side-state handling for ambiguous states like `UNKNOWN` that have equal rank to `INITIATED`.

---

## 5. Telecom Provider Calls Outside Database Transactions (3-Step Flow)

* **What I Chose**: A strict 3-step execution pattern:
  1. **Txn 1 (DB)**: Atomically reserve agent/borrower and create call row with state `RESERVED` and a lease timestamp.
  2. **Outside DB**: Perform the network HTTP request to the telecom provider (`initiate_call`).
  3. **Txn 2 (DB)**: Record provider response (`INITIATED` on accept, `FAILED` on reject, `UNKNOWN` on timeout).
* **Why**: Principle 3: *Never call a telecom provider inside a DB transaction*.
* **What Problem It Solves**: Prevents slow or hung provider network calls from holding open database connections and table row locks, shielding Postgres from connection pool starvation.
* **What It Makes Harder**: If a worker crashes between Step 2 and Step 3, the database state remains `RESERVED` until the background Recovery Sweeper reconciles it via provider idempotency keys.

---

## 6. Answer-Time Agent Claim for Predictive Mode

* **What I Chose**: In predictive mode, calls are dispatched without reserving an agent upfront. When the borrower answers (`ANSWERED`), the event processor attempts an immediate `FOR UPDATE SKIP LOCKED` claim on an available agent; if none is free, the call enters `HELD` for up to `HOLD_TIMEOUT` (2.0s) before abandoning.
* **Why**: True predictive dialing derives efficiency from overdialing while agents are still talking or in wrap-up, banking on statistical completion times.
* **What Problem It Solves**: Maximizes agent talk time utilization by eliminating idle waiting during the 20–30s call setup and ringing phases.
* **What It Makes Harder**: Introduces regulatory abandonment risk if more borrowers answer simultaneously than free agents become available. This risk is mathematically bounded by the Safety Controller.

---

## 7. Beta-Bayesian Answer-Rate Estimator with Pessimistic $p_{hi}$

* **What I Chose**: Answer rates are modeled as Beta posterior distributions updated online with exponential decay across long-term and short-term windows. Pacing calculates $p_{hi} = \min(1.0, \mu + z\cdot\sigma)$ and takes $p_{hi} = \max(p_{hi,\text{long}}, p_{hi,\text{recent}})$. Pacing solves for $N^*$ such that $P(\text{Binomial}(N^*, p_{hi}) > \text{Capacity}) \le \epsilon$.
* **Why**: Standard point estimates ($\mu$) underestimate answer rate surges, leading to catastrophic regulatory abandonment spikes when answer rates jump.
* **What Problem It Solves**: The drift guard immediately clamps overdialing if answer rates spike, while the binomial tail bound guarantees with confidence $1 - \epsilon$ that answering calls will not overwhelm agent capacity.
* **What It Makes Harder**: Requires calculating binomial tail sums in pure Python stdlib math without scipy/numpy; requires fallback to progressive mode when samples are scarce.

---

## 8. Multi-Threading over Asyncio

* **What I Chose**: Standard library `threading` with one SQLAlchemy `Session` per worker thread, backed by standard blocking network/database calls.
* **Why**: Relational database operations and concurrency guarantees (row locks, advisory locks) are synchronous by nature. Multithreading provides clean error isolation: an unhandled exception or crash in one thread cannot corrupt the event loop or state of sibling threads.
* **What Problem It Solves**: Avoids async DB session leaks, hidden event-loop stalls, and complex cooperative multitasking bugs.
* **What It Makes Harder**: Python Global Interpreter Lock (GIL) caps single-process CPU scalability, requiring multi-process scaling for $>10,000$ agents.

---

## 9. No Alembic (Models + Idempotent `schema.sql`)

* **What I Chose**: Database schema is established via `Base.metadata.create_all()` followed by idempotent execution of `db/schema.sql` (using `CREATE INDEX IF NOT EXISTS` and `CREATE UNIQUE INDEX IF NOT EXISTS`).
* **Why**: For a standalone hiring prototype, migration histories and multi-version upgrade chains add unnecessary cognitive overhead and external tool dependencies.
* **What Problem It Solves**: Single-command, zero-configuration bootstrap (`make init-db` or `python -m smartdialer.db.init_db`) that runs identically in Docker, local dev, and CI.
* **What It Makes Harder**: Production column alterations or data transformations would require manual SQL migration scripts rather than automated migration generation.

---

## 10. Why No Kafka, Redis, or Microservices?

1. **Transactional Integrity & Dual Writes**: Distributed architectures split between Redis (counters) and Postgres (records) inevitably suffer from dual-write inconsistencies during network partitions or crashes. With PostgreSQL as the single engine, row locks and partial indexes guarantee atomic consistency.
2. **Operational Simplicity**: A single `docker-compose.yml` with PostgreSQL 16 eliminates the operational overhead of ZooKeeper/KRaft, Kafka topic partitioning, consumer group rebalancing, and Redis Sentinel failover.
3. **Auditability**: Post-incident auditing and compliance reporting (e.g., proving abandonment was below 3.0%) are trivial SQL queries on historical tables (`calls`, `call_events`, `safety_decisions`), rather than requiring distributed event reconstruction.
4. **Hiring Assignment Clarity**: Every architectural decision can be traced to a single relational transaction model that is straightforward to explain and reason about.

---

## 11. Guarantees We Provide & Guarantees We Don't Provide

### Guarantees Provided:
- **At-Most-Once Agent Booking**: Database partial unique index `uq_calls_active_agent` guarantees that no agent can ever be attached to more than one active call simultaneously.
- **At-Most-Once Borrower Dialing**: Database partial unique index `uq_calls_active_borrower` guarantees that no borrower can ever be dialed concurrently across multiple campaigns or workers.
- **Strict In-Flight Capacity Caps**: In-flight calls are hard-capped at $\text{Total Agents} \times \text{MAX\_OVERDIAL\_RATIO}$ by the deterministic Safety Controller.
- **Zero Event Duplication**: `ON CONFLICT (provider, event_id) DO NOTHING` prevents duplicate webhook replays from triggering multiple state updates.
- **Deterministic Crash Recovery**: The Recovery Sweeper reclaims all abandoned agent reservations and unacknowledged call leases within bounded TTL windows.

### Guarantees NOT Provided:
- **Zero Call Abandonment in Predictive Mode**: Predictive dialing is fundamentally probabilistic. While the binomial tail bound strictly limits risk to $\epsilon$ (e.g. 1%), random clusters of simultaneous answers can still trigger abandonments if agent calls run longer than anticipated.
- **Sub-Millisecond Webhook Latency**: Because incoming events are staged in an inbox table and processed in batches via polling workers, event propagation latency is bounded by the polling cycle (~100ms–500ms).
- **Cross-Region Active-Active Replication**: The architecture relies on row-level locking (`FOR UPDATE SKIP LOCKED`) and advisory locks, which require a single PostgreSQL primary cluster.
