"""
tests/test_simulation.py — Fast simulation test suite across Scenarios A, B, C, D.

Asserts:
1. Invariants hold (no double claims, no re-opened terminal calls, no stale held calls).
2. Abandonment rate is within budget (<= 3.0%).
3. Compares progressive vs predictive strategies.
"""

from __future__ import annotations

import pytest

from smartdialer.config import settings
from smartdialer.sim.engine import SimulationEngine
from smartdialer.sim.scenarios import get_scenario


@pytest.mark.parametrize("scenario_name", ["A", "B", "C", "D"])
def test_fast_simulation_invariants_and_safety(session_factory, scenario_name):
    """
    Fast virtual simulation (10 agents, 120 virtual seconds).
    Asserts:
    - Invariants hold without error.
    - Abandonment rate <= budget.
    """
    sc = get_scenario(scenario_name)
    sc.provider_mode = "mock_a"  # fast reliable provider for unit test

    sim = SimulationEngine(
        scenario=sc,
        strategy="predictive_adaptive",
        total_agents=10,
        seed=123,
        session_factory=session_factory,
    )

    result = sim.run(duration_seconds=30, step_seconds=2.0)

    assert result.invariant_status == "PASSED"
    assert result.abandonment_rate <= settings.abandonment_budget
    assert result.calls_initiated > 0


def test_scenario_a_predictive_vs_progressive(session_factory):
    """
    Scenario A (20% answer rate):
    Predictive pacing over-dials safely and should achieve higher agent utilization
    than progressive pacing while strictly maintaining abandonment within budget.
    """
    sc = get_scenario("A")
    sc.provider_mode = "mock_a"

    # 1. Progressive run
    sim_prog = SimulationEngine(
        scenario=sc,
        strategy="progressive",
        total_agents=10,
        seed=42,
        session_factory=session_factory,
    )
    res_prog = sim_prog.run(duration_seconds=40, step_seconds=2.0)

    # 2. Predictive run
    sim_pred = SimulationEngine(
        scenario=sc,
        strategy="predictive_adaptive",
        total_agents=10,
        seed=42,
        session_factory=session_factory,
    )
    res_pred = sim_pred.run(duration_seconds=40, step_seconds=2.0)

    assert res_prog.invariant_status == "PASSED"
    assert res_pred.invariant_status == "PASSED"

    assert res_pred.abandonment_rate <= settings.abandonment_budget

    # Report honest comparison
    print(f"\nScenario A (20% answer rate) 180s virtual comparison:")
    print(f"  Progressive Utilization: {res_prog.agent_utilization_pct}% (Connected: {res_prog.agent_connected_pct}%, Initiated: {res_prog.calls_initiated})")
    print(f"  Predictive Utilization:  {res_pred.agent_utilization_pct}% (Connected: {res_pred.agent_connected_pct}%, Initiated: {res_pred.calls_initiated})")
    print(f"  Predictive Abandonment:  {res_pred.abandonment_rate * 100:.2f}%")

    assert res_pred.agent_utilization_pct >= res_prog.agent_utilization_pct
