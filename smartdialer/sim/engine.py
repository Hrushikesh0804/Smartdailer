"""
smartdialer.sim.engine — Deterministic simulation engine.

Runs the REAL orchestrator tick, safety controller, allocator, event inbox,
event processor, sweeper, and mock providers against real PostgreSQL.
Advances a FakeClock and uses a seeded random.Random to guarantee 100%
reproducibility.
"""

from __future__ import annotations

import logging
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from smartdialer.clock import FakeClock
from smartdialer.config import settings
from smartdialer.db.models import Agent, Borrower, Call, CallEvent
from smartdialer.db.session import transaction
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState
from smartdialer.events.inbox import ingest
from smartdialer.orchestrator.tick import run_tick
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.mock_b import MockProviderB
from smartdialer.providers.registry import clear as clear_registry, register as register_provider
from smartdialer.recovery.sweeper import Sweeper
from smartdialer.safety.controller import reset_hysteresis
from smartdialer.pacing.estimator import reset_estimator
from smartdialer.sim.scenarios import CallOutcome, ScenarioConfig
from smartdialer.sim.invariants import assert_all_invariants

logger = logging.getLogger(__name__)


@dataclass
class DecisionTrace:
    tick: int
    elapsed_seconds: float
    free_agents: int
    capacity: int
    in_flight: int
    requested_calls: int
    approved_calls: int
    advisory_action: str
    advisory_rule_ids: list[str]
    enforced_action: str
    why: str


@dataclass
class SimulationResult:
    scenario_name: str
    strategy: str
    seed: int
    total_agents: int
    duration_seconds: int
    agent_utilization_pct: float
    agent_connected_pct: float
    avg_agent_idle_time_seconds: float
    calls_initiated: int
    calls_answered: int
    calls_connected: int
    calls_abandoned: int
    abandonment_rate: float
    max_in_flight: int
    safety_actions: dict[str, int]
    safety_rules: dict[str, int]
    provider_timeouts: int
    provider_rejections: int
    circuit_trips: int
    reservation_conflicts: int
    sweeper_actions: dict[str, int]
    decision_traces: list[DecisionTrace]
    invariant_status: str


class SimulationEngine:
    """
    Clock-driven simulation engine driving the real SmartDialer system.
    """

    def __init__(
        self,
        scenario: ScenarioConfig,
        strategy: str,
        total_agents: int = 100,
        seed: int = 42,
        session_factory = None,
        campaign_id: int = 1,
        fixed_rate: float | None = None,
        start_time: datetime | None = None,
        answer_rate_override_fn: Any = None,
    ) -> None:
        self.scenario = scenario
        self.strategy = strategy.lower()  # progressive, predictive_fixed, predictive_adaptive
        self.total_agents = total_agents
        self.seed = seed
        self.session_factory = session_factory
        self.campaign_id = campaign_id
        self.fixed_rate = fixed_rate
        self.answer_rate_override_fn = answer_rate_override_fn

        self._rng = random.Random(seed)
        self.start_time = start_time or datetime(2025, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
        self.clock = FakeClock(self.start_time)

        # Simulation state
        self.elapsed_seconds: float = 0.0
        self.current_tick: int = 0
        self.max_inflight_observed: int = 0

        # Telemetry aggregators
        self.total_agent_seconds_connected: float = 0.0
        self.total_agent_seconds_wrap_up: float = 0.0
        self.total_agent_seconds_available: float = 0.0
        self.total_agent_seconds_paused: float = 0.0

        self.safety_action_counts: dict[str, int] = {}
        self.safety_rule_counts: dict[str, int] = {}
        self.sweeper_action_counts: dict[str, int] = {
            "expired_reserved": 0,
            "reconciled_unknown": 0,
            "stale_agent_released": 0,
        }

        self.decision_traces: list[DecisionTrace] = []
        self._provider_a: MockProviderA | None = None
        self._provider_b: MockProviderB | None = None
        self.sweeper: Sweeper | None = None

        # Pause tracker: agent_id -> resume_at
        self._agent_pause_until: dict[int, datetime] = {}

    def setup(self) -> None:
        """Reset estimator, clear state, populate agents and borrowers in DB."""
        reset_estimator()
        reset_hysteresis()
        clear_registry()

        # 1. Setup Providers
        provider_seed_a = self._rng.randint(1, 1_000_000)
        provider_seed_b = self._rng.randint(1, 1_000_000)

        self._provider_a = MockProviderA(
            seed=provider_seed_a,
            rejection_rate=0.01 + self.scenario.extra_failure_rate,
            min_latency_ms=50.0 + self.scenario.extra_latency_ms,
            max_latency_ms=150.0 + self.scenario.extra_latency_ms,
            clock=self.clock,
            outcome_fn=self._generate_call_outcome,
        )
        self._provider_b = MockProviderB(
            seed=provider_seed_b,
            rejection_rate=0.05 + self.scenario.extra_failure_rate,
            timeout_rate=0.05 + self.scenario.extra_failure_rate,
            min_latency_ms=200.0 + self.scenario.extra_latency_ms,
            max_latency_ms=600.0 + self.scenario.extra_latency_ms,
            clock=self.clock,
            outcome_fn=self._generate_call_outcome,
        )

        register_provider(self._provider_a)
        register_provider(self._provider_b)

        # 2. Setup Sweeper
        self.sweeper = Sweeper(self.session_factory, clock=self.clock)

        # 3. Populate Database Agents & Borrowers
        session = self.session_factory()
        try:
            with transaction(session):
                now = self.clock.now()
                session.execute(text("DELETE FROM call_events"))
                session.execute(text("DELETE FROM calls WHERE campaign_id = :cid"), {"cid": self.campaign_id})
                session.execute(text("DELETE FROM agents WHERE campaign_id = :cid"), {"cid": self.campaign_id})
                session.execute(text("DELETE FROM borrowers WHERE campaign_id = :cid"), {"cid": self.campaign_id})


                # Create agents
                for i in range(1, self.total_agents + 1):
                    agent = Agent(
                        campaign_id=self.campaign_id,
                        state=AgentState.AVAILABLE.value,
                        last_state_change_at=now,
                        updated_at=now,
                    )
                    session.add(agent)

                # Create ample borrowers pool
                total_borrowers = max(500, self.total_agents * 25)
                for j in range(1, total_borrowers + 1):
                    borrower = Borrower(
                        campaign_id=self.campaign_id,
                        phone_token=f"tok_{self.scenario.name}_{j}",
                        state="ELIGIBLE",
                        priority=j,
                        attempts=0,
                        next_eligible_at=now,
                    )
                    session.add(borrower)
        finally:
            session.close()


    def _generate_call_outcome(self, call_id: uuid.UUID, now: datetime) -> CallOutcome:
        """
        True borrower response model — completely separate from estimator.
        """
        if self.answer_rate_override_fn:
            override = self.answer_rate_override_fn()
            if override is not None:
                p_ans = float(override)
            else:
                p_ans = self.scenario.get_answer_rate(self.elapsed_seconds)
        else:
            p_ans = self.scenario.get_answer_rate(self.elapsed_seconds)

        answered = self._rng.random() < p_ans
        ring_time = self._rng.uniform(self.scenario.min_ring_seconds, self.scenario.max_ring_seconds)
        mean_talk = self.scenario.get_talk_time(self.elapsed_seconds)
        # Exponential talk time with mean mean_talk
        talk_time = self._rng.expovariate(1.0 / max(10.0, mean_talk))
        return CallOutcome(
            answered=answered,
            ring_time_seconds=round(ring_time, 2),
            talk_time_seconds=round(talk_time, 2),
        )

    def _select_provider_for_tick(self) -> str:
        if self.scenario.provider_mode == "mock_a":
            return "mock_a"
        elif self.scenario.provider_mode == "mock_b":
            return "mock_b"
        else:
            # Mixed: alternating or randomly weighted
            return "mock_a" if (self.current_tick % 2 == 0) else "mock_b"

    def step(self, step_seconds: float = 1.0) -> dict[str, Any]:
        """
        Execute a single virtual second/tick of simulation.
        """
        now = self.clock.now()

        # 1. Run sweeper periodically (every 5 virtual seconds)
        if self.current_tick % 5 == 0 and self.sweeper:
            sweep_res = self.sweeper.sweep_once()
            for k, v in sweep_res.items():
                if k in self.sweeper_action_counts:
                    self.sweeper_action_counts[k] += v

        # 2. Ingest ready telecom events from both providers
        for prov in (self._provider_a, self._provider_b):
            if prov:
                events = prov.poll_events(max_n=100, ready_only=True)
                if events:
                    s = self.session_factory()
                    try:
                        for ev in events:
                            ingest(s, prov.name, ev, clock=self.clock)
                    finally:
                        s.close()

        # 3. Determine pacing mode & fixed rate
        if self.strategy == "progressive":
            tick_mode = "PROGRESSIVE"
            fixed_r = None
        elif self.strategy == "predictive_fixed":
            tick_mode = "PREDICTIVE_FIXED"
            # If fixed_rate not specified, use base scenario answer rate
            fixed_r = self.fixed_rate if self.fixed_rate is not None else self.scenario.get_answer_rate(0)
        else:
            tick_mode = "PREDICTIVE"
            fixed_r = None

        selected_provider = self._select_provider_for_tick()

        # 4. Execute Orchestrator Tick
        tick_summary = run_tick(
            session_factory=self.session_factory,
            campaign_id=self.campaign_id,
            provider_name=selected_provider,
            mode=tick_mode,
            clock=self.clock,
            fixed_rate=fixed_r,
        )

        # 5. Track max in-flight
        curr_inflight = tick_summary.get("in_flight", 0)
        if curr_inflight > self.max_inflight_observed:
            self.max_inflight_observed = curr_inflight

        # 6. Record safety telemetry
        action = tick_summary.get("advisory_action", "")
        if action:
            self.safety_action_counts[action] = self.safety_action_counts.get(action, 0) + 1
        for rid in tick_summary.get("advisory_rule_ids", []):
            self.safety_rule_counts[rid] = self.safety_rule_counts.get(rid, 0) + 1

        # 7. Record decision trace (sample up to 20 decisions across run)
        if len(self.decision_traces) < 20 and self.current_tick % max(1, int(30 / step_seconds)) == 0:
            self.decision_traces.append(
                DecisionTrace(
                    tick=self.current_tick,
                    elapsed_seconds=self.elapsed_seconds,
                    free_agents=tick_summary.get("free_agents", 0),
                    capacity=tick_summary.get("capacity", 0),
                    in_flight=tick_summary.get("in_flight", 0),
                    requested_calls=tick_summary.get("requested_calls", 0),
                    approved_calls=tick_summary.get("approved_calls", 0),
                    advisory_action=tick_summary.get("advisory_action", ""),
                    advisory_rule_ids=tick_summary.get("advisory_rule_ids", []),
                    enforced_action=tick_summary.get("enforced_action", ""),
                    why=tick_summary.get("pacing_why", ""),
                )
            )

        # 8. Drive Agent Behavior (wrap-up completion & pauses)
        self._drive_agent_behavior(step_seconds)

        # 9. Advance Clock & Time
        self.clock.advance(seconds=step_seconds)
        self.elapsed_seconds += step_seconds
        self.current_tick += 1

        return tick_summary

    def _drive_agent_behavior(self, step_seconds: float) -> None:
        """
        Transition agents from WRAP_UP -> AVAILABLE after wrap-up time.
        Model agent pauses/breaks and track time spent in each state.
        """
        session = self.session_factory()
        now = self.clock.now()
        try:
            with transaction(session):
                agents = session.scalars(
                    select(Agent).where(Agent.campaign_id == self.campaign_id)
                ).all()

                for agent in agents:
                    # Accumulate state durations
                    if agent.state == AgentState.CONNECTED.value:
                        self.total_agent_seconds_connected += step_seconds
                    elif agent.state == AgentState.WRAP_UP.value:
                        self.total_agent_seconds_wrap_up += step_seconds
                    elif agent.state == AgentState.AVAILABLE.value:
                        self.total_agent_seconds_available += step_seconds
                    elif agent.state == AgentState.PAUSED.value:
                        self.total_agent_seconds_paused += step_seconds

                    # Transition from WRAP_UP to AVAILABLE
                    if agent.state == AgentState.WRAP_UP.value:
                        time_in_wrap = (now - agent.last_state_change_at).total_seconds()
                        if time_in_wrap >= self.scenario.wrap_up_seconds:
                            agent.state = AgentState.AVAILABLE.value
                            agent.last_state_change_at = now
                            agent.updated_at = now
                            agent.version += 1

                    # Resume from PAUSED to AVAILABLE
                    elif agent.state == AgentState.PAUSED.value:
                        resume_at = self._agent_pause_until.get(agent.id)
                        if resume_at and now >= resume_at:
                            agent.state = AgentState.AVAILABLE.value
                            agent.last_state_change_at = now
                            agent.updated_at = now
                            agent.version += 1
                            self._agent_pause_until.pop(agent.id, None)

                    # Trigger occasional break/pause from AVAILABLE
                    elif agent.state == AgentState.AVAILABLE.value:
                        # Probability of pause per second
                        prob = (self.scenario.pause_rate_per_hour / 3600.0) * step_seconds
                        if self._rng.random() < prob:
                            pause_duration = max(30.0, self._rng.expovariate(1.0 / self.scenario.mean_pause_duration_seconds))
                            agent.state = AgentState.PAUSED.value
                            agent.last_state_change_at = now
                            agent.updated_at = now
                            agent.version += 1
                            self._agent_pause_until[agent.id] = now + timedelta(seconds=pause_duration)
        finally:
            session.close()

    def run(self, duration_seconds: int = 3600, step_seconds: float = 1.0) -> SimulationResult:
        """
        Run the complete simulation for duration_seconds.
        """
        self.setup()
        total_steps = int(duration_seconds / step_seconds)

        for _ in range(total_steps):
            self.step(step_seconds=step_seconds)

        # Final sweep & event processing to settle in-flight states
        self.sweeper.sweep_once()

        # Audit invariants
        session = self.session_factory()
        try:
            assert_all_invariants(
                session=session,
                campaign_id=self.campaign_id,
                now=self.clock.now(),
                max_inflight_observed=self.max_inflight_observed,
                total_agents=self.total_agents,
            )
            inv_status = "PASSED"

            # Query Call Metrics from DB
            initiated_count = session.execute(
                select(func.count(Call.id)).where(Call.campaign_id == self.campaign_id)
            ).scalar() or 0

            answered_count = session.execute(
                select(func.count(CallEvent.id)).where(
                    CallEvent.event_type == "ANSWERED",
                    CallEvent.status == "APPLIED",
                )
            ).scalar() or 0

            connected_count = session.execute(
                select(func.count(Call.id)).where(
                    Call.campaign_id == self.campaign_id,
                    Call.state == CallState.COMPLETED.value,
                )
            ).scalar() or 0

            abandoned_count = session.execute(
                select(func.count(Call.id)).where(
                    Call.campaign_id == self.campaign_id,
                    Call.state == CallState.ABANDONED.value,
                )
            ).scalar() or 0
        finally:
            session.close()


        total_agent_time = self.total_agents * duration_seconds
        active_time = self.total_agent_seconds_connected + self.total_agent_seconds_wrap_up
        utilization_pct = round((active_time / total_agent_time) * 100.0, 2)
        connected_pct = round((self.total_agent_seconds_connected / total_agent_time) * 100.0, 2)
        avg_idle_time = round(self.total_agent_seconds_available / self.total_agents, 2)
        abandon_rate = round((abandoned_count / answered_count) if answered_count > 0 else 0.0, 4)

        return SimulationResult(
            scenario_name=self.scenario.name,
            strategy=self.strategy,
            seed=self.seed,
            total_agents=self.total_agents,
            duration_seconds=duration_seconds,
            agent_utilization_pct=utilization_pct,
            agent_connected_pct=connected_pct,
            avg_agent_idle_time_seconds=avg_idle_time,
            calls_initiated=initiated_count,
            calls_answered=answered_count,
            calls_connected=connected_count,
            calls_abandoned=abandoned_count,
            abandonment_rate=abandon_rate,
            max_in_flight=self.max_inflight_observed,
            safety_actions=self.safety_action_counts,
            safety_rules=self.safety_rule_counts,
            provider_timeouts=0,
            provider_rejections=0,
            circuit_trips=0,
            reservation_conflicts=0,
            sweeper_actions=self.sweeper_action_counts,
            decision_traces=self.decision_traces,
            invariant_status=inv_status,
        )
