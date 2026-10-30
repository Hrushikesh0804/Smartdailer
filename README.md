# SmartDialer: Deterministic & Safe Predictive Dialing Engine

A production-grade outbound call center dialer prototype built with Python 3.11+, SQLAlchemy 2.x, and PostgreSQL 16. It combines the high-utilization benefits of predictive dialing with the strict, deterministic safety guarantees of progressive dialing.

---

## 1. Prerequisites

- **Python 3.11+** (standard library math, threading, dataclasses)
- **Docker & Docker Compose** (for PostgreSQL 16)
- **Git**

---

## 2. Quickstart & Setup (In Order)

```bash
# 1. Start PostgreSQL 16 containers (creates smartdialer and smartdialer_test databases)
docker compose up -d

# 2. Create and activate a Python virtual environment
python -m venv .venv
# On Windows PowerShell:
.\.venv\Scripts\Activate.ps1
# On Linux/macOS:
# source .venv/bin/activate

# 3. Install pinned dependencies
pip install -r requirements.txt

# 4. Initialize database schema, partial indexes, and constraints idempotently
python -m smartdialer.db.init_db
# (or if make is installed: make init-db)
```

---

## 3. Running Test Suites

All tests run against real PostgreSQL (`smartdialer_test`), asserting real database state transitions and invariants:

```bash
# Run the entire test suite (215 tests across all modules)
pytest -v

# Run fast simulation tests
pytest tests/test_simulation.py -v

# Run failure injection test suite
pytest tests/test_simulation_failures.py -v
```

---

## 4. Running the Simulation Engine (Phase 5)

The simulation engine drives virtual time via `FakeClock` and seeded `random.Random`, evaluating the **real** orchestrator, safety controller, allocator, and event processor against live PostgreSQL.

```bash
# Run Scenario A (20% answer rate) with Predictive Adaptive pacing:
python -m smartdialer.sim.run --scenario A --strategy predictive_adaptive --providers mixed --seed 42 --agents 100 --duration 120 --step 5.0

# Run Scenario D (Sudden Answer Rate Shifts: 60% -> 15% -> 65%):
python -m smartdialer.sim.run --scenario D --strategy predictive_adaptive --providers mixed --seed 42 --agents 100 --duration 120 --step 5.0

# Run the full 12-scenario matrix (outputs docs/simulation_results.md and sim_results.csv):
python -m smartdialer.sim.run --all --duration 300 --step 5.0 --agents 100 --providers mixed --seed 42
```

---

## 5. Running the High-Concurrency Load Test (Phase 6)

Measures allocations/sec, event processing throughput, latency percentiles (p50/p95/p99), lock contention, and runs `EXPLAIN (ANALYZE, BUFFERS)` on hot queries:

```bash
# Run single scale (100 agents, 1,000 borrowers, 4 workers):
python -m smartdialer.loadtest.run --agents 100 --workers 4 --rounds 6

# Run full multi-scale load benchmark across 100, 1,000, and 10,000 agents:
python -m smartdialer.loadtest.run --all-scales --workers 8 --rounds 10
```

---

## 6. Running Reproducible Failure Demos

Six standalone failure injection demos demonstrating automated recovery against real PostgreSQL:

```bash
# 1. Worker crash during dial -> Sweeper lease recovery
python -m smartdialer.sim.failures.worker_crash

# 2. Provider outage -> Rolling window error rate & Circuit Breaker trip
python -m smartdialer.sim.failures.provider_outage

# 3. Mass agent drop -> Capacity drop guard cancels excess ringing calls
python -m smartdialer.sim.failures.agent_drop

# 4. Duplicate event storm -> In-flight idempotency filter absorbs duplicates
python -m smartdialer.sim.failures.duplicate_event_storm

# 5. Out-of-order event storm (COMPLETED before ANSWERED) -> State rank monotonic absorption
python -m smartdialer.sim.failures.out_of_order_storm

# 6. Sudden answer rate surge (15% -> 65%) -> Dual-window drift guard clamps overdialing
python -m smartdialer.sim.failures.answer_spike
```

---

## 7. Running Live Orchestrator Workers

Run live concurrent worker threads dialing campaigns against configured providers:

```bash
python -m smartdialer.orchestrator.run --workers 4 --campaign 1 --provider mock_a --interval 1.0
```

---

## 8. Running the Live Operator Dashboard (Phase 8)

Launch the real-time monitoring and control dashboard with virtual time acceleration:

```bash
# Start live simulation runner + FastAPI server on http://localhost:8000
python -m smartdialer.dashboard.live --scenario A --agents 100 --speed 10 --seed 42

# Run dashboard test suite (9 tests verifying read-only transactions, zero PII leaks, auth, latency)
pytest tests/test_dashboard.py -v
```

Features included:
- **Zero-Build Vanilla Frontend**: Single responsive HTML page with Chart.js line charts and table fallback.
- **Explainability Stream**: Real-time cards explaining each pacing decision and mathematical bound.
- **Interactive Operator Console**: Live kill-switch, provider outage simulator, agent drop, answer rate shift, duplicate webhook injection.
- **Sub-12ms Aggregation Latency**: Non-locking `SET TRANSACTION READ ONLY` queries scaling to 10,000 agents.

---

## 9. Requirements $\to$ Implementation $\to$ Test Checklist

| Phase | Assignment Requirement | Implementation Component | Verification / Test |
|---|---|---|---|
| **Phase 1** | PostgreSQL schema, typed SQLAlchemy 2.x models, partial indexes, transactional sessions | [`smartdialer/db/models.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/db/models.py)<br>[`smartdialer/db/schema.sql`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/db/schema.sql) | [`tests/test_call_fsm.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_call_fsm.py)<br>`tests/test_concurrency_progressive.py` |
| **Phase 1** | Monotonic integer-ranked FSM, terminal absorbing states | [`smartdialer/domain/call_fsm.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/domain/call_fsm.py) | 33 parametrized FSM unit tests in `tests/test_call_fsm.py` |
| **Phase 1** | Sweeper advisory lock, stale lease recovery, ambiguous UNKNOWN reconciliation | [`smartdialer/sweeper/sweeper.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/sweeper/sweeper.py) | 6 sweeper tests in [`tests/test_sweeper.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_sweeper.py) |
| **Phase 2** | Provider protocol, Mock Providers A & B, zero provider calls in DB transactions | [`smartdialer/providers/`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/providers/)<br>[`smartdialer/executor/`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/executor/) | [`tests/test_executor.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_executor.py)<br>`tests/test_provider_health.py` |
| **Phase 2** | Event inbox table, idempotent deduplication, quarantined out-of-order retries | [`smartdialer/events/inbox.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/events/inbox.py)<br>[`smartdialer/events/processor.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/events/processor.py) | 6 concurrency tests in [`tests/test_events.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_events.py) |
| **Phase 3** | Deterministic Safety Controller (Rules R1–R6), circuit breaker, rolling abandonment hysteresis | [`smartdialer/safety/controller.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/safety/controller.py) | 9 safety tests in [`tests/test_safety_controller.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_safety_controller.py) |
| **Phase 3** | Progressive dialer baseline: match available agents and eligible borrowers with `SKIP LOCKED` | [`smartdialer/allocator/allocator.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/allocator/allocator.py) | [`tests/test_pacing_progressive.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_pacing_progressive.py) |
| **Phase 4** | Beta-Bayesian answer-rate estimator with dual-window drift guard ($p_{hi} = \max(p_{hi,\text{long}}, p_{hi,\text{recent}})$) | [`smartdialer/pacing/estimator.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/pacing/estimator.py) | [`tests/test_estimator.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_estimator.py) |
| **Phase 4** | Binomial tail bound pacing solver in pure stdlib math without scipy/numpy | [`smartdialer/pacing/predictive.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/pacing/predictive.py) | [`tests/test_predictive_unit.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_predictive_unit.py) (including Hypothesis property tests) |
| **Phase 4** | Full predictive integration: hold timeout (2.0s), answer-time matching, capacity drop cancellation | [`smartdialer/orchestrator/tick.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/orchestrator/tick.py) | 4 integration tests in [`tests/test_predictive_integration.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_predictive_integration.py) |
| **Phase 5** | Deterministic clock-driven simulation engine across Scenarios A, B, C, D and 3 strategies | [`smartdialer/sim/engine.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/sim/engine.py)<br>[`smartdialer/sim/scenarios.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/sim/scenarios.py) | [`tests/test_simulation.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_simulation.py) |
| **Phase 5** | 6 loud invariant audits: agent singularity, borrower singularity, in-flight cap, hold timeout, resolved fate, terminal lock | [`smartdialer/sim/invariants.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/sim/invariants.py) | Audited on every simulation tick; 100% passing across all 12 matrix runs |
| **Phase 5** | 6 reproducible failure demos | [`smartdialer/sim/failures/`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/sim/failures/) | 6 tests in [`tests/test_simulation_failures.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_simulation_failures.py) |
| **Phase 6** | High-concurrency load testing, `EXPLAIN (ANALYZE, BUFFERS)` query profiler, GIL vs DB analysis | [`smartdialer/loadtest/run.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/loadtest/run.py)<br>[`docs/loadtest_results.md`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/docs/loadtest_results.md) | Evaluated across 100, 1,000, and 10,000 agents |
| **Phase 6** | Architecture reference, ADRs, scaling analysis, interview answer ($<250$ words), limitations | [`docs/ARCHITECTURE.md`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/docs/ARCHITECTURE.md)<br>[`docs/ADR.md`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/docs/ADR.md)<br>[`docs/SCALING.md`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/docs/SCALING.md)<br>[`docs/FINAL_ANSWER.md`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/docs/FINAL_ANSWER.md)<br>[`docs/LIMITATIONS.md`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/docs/LIMITATIONS.md) | Verified and committed |
| **Phase 7** | Hostile Architectural Audit: citations, race conditions, fail-safe proofs, and bug fixes | [`tests/test_audit_bugs.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_audit_bugs.py) | 2 bug repro tests passed & committed |
| **Phase 8** | Live Operator Dashboard & Control Console: FastAPI, read-only non-locking endpoints, audited console | [`smartdialer/dashboard/`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/smartdialer/dashboard/)<br>[`docs/DASHBOARD.md`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/docs/DASHBOARD.md) | 9 tests in [`tests/test_dashboard.py`](file:///c:/Users/siddu/OneDrive/Desktop/Akash/Smart%20Dial/tests/test_dashboard.py) |

---

## 9. Documentation Index

- **[Live Operator Dashboard & 90-Second Demo Script](docs/DASHBOARD.md)**: Panel-by-panel guide, security guarantees, explainability proofs, and live interactive demo walkthrough.
- **[System Architecture & State Machines](docs/ARCHITECTURE.md)**: Mermaid diagrams for component flow, agent FSM, call FSM, end-to-end call sequence, and `SKIP LOCKED` concurrency races.
- **[Architecture Decision Records (ADR)](docs/ADR.md)**: Rationale, trade-offs, and operational guarantees for all 9 architectural choices.
- **[Scaling Analysis](docs/SCALING.md)**: Bottleneck progression and structural fixes across 100 $\to$ 1,000 $\to$ 10,000 $\to$ 100,000 agents.
- **[Executive Architectural Answer](docs/FINAL_ANSWER.md)**: Concise 214-word response solving safe predictive dialing.
- **[Technical Limitations & Honest Appraisal](docs/LIMITATIONS.md)**: Telephony trade-offs, policy choices, what I'd do with another week, and least confident areas.
- **[Simulation Matrix Benchmark](docs/simulation_results.md)**: Full 12-scenario empirical results with 10-step decision trace sample.
- **[Load Test & Query Plans](docs/loadtest_results.md)**: Latency percentiles, throughput curves, and `EXPLAIN (ANALYZE, BUFFERS)` plans.
