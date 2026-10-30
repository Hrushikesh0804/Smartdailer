"""
smartdialer.sim.scenarios — Dataclasses defining simulation scenarios and call outcome models.

Scenarios
---------
A: 20% answer, 120s talk (low answer rate, typical collection outreach)
B: 50% answer, 90s talk (medium answer rate)
C: 70% answer, 180s talk (high answer rate, long talk time)
D: Time-varying answer rate & talk time:
   - Starts at 60% answer, 100s talk
   - Drops suddenly to 15% answer (e.g. at 25% of duration)
   - Spikes sharply from 15% to 65% answer (e.g. at 60% of duration)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Literal


@dataclass(frozen=True)
class CallOutcome:
    """Simulated borrower outcome generated at dial time."""
    answered: bool
    ring_time_seconds: float
    talk_time_seconds: float


@dataclass
class ScenarioConfig:
    """Configuration for a simulation scenario."""
    name: str
    description: str
    # Answer rate: can be a constant float or a function of elapsed virtual seconds: f(t) -> float
    answer_rate: float | Callable[[float], float]
    # Talk time: can be a constant float or a function of elapsed virtual seconds: f(t) -> float
    mean_talk_time_seconds: float | Callable[[float], float]
    min_ring_seconds: float = 4.0
    max_ring_seconds: float = 18.0
    wrap_up_seconds: float = 10.0
    pause_rate_per_hour: float = 0.5  # average pauses per agent per hour
    mean_pause_duration_seconds: float = 120.0
    provider_mode: Literal["mock_a", "mock_b", "mixed"] = "mixed"
    extra_latency_ms: float = 0.0
    extra_failure_rate: float = 0.0

    def get_answer_rate(self, elapsed_seconds: float) -> float:
        if callable(self.answer_rate):
            return float(self.answer_rate(elapsed_seconds))
        return float(self.answer_rate)

    def get_talk_time(self, elapsed_seconds: float) -> float:
        if callable(self.mean_talk_time_seconds):
            return float(self.mean_talk_time_seconds(elapsed_seconds))
        return float(self.mean_talk_time_seconds)


def _scenario_d_answer_rate(elapsed_seconds: float) -> float:
    """
    Scenario D answer rate curve:
    - 0s to 900s: 60% answer
    - 900s to 2100s: 15% sudden drop
    - 2100s to end: 65% sharp spike
    """
    if elapsed_seconds < 900:
        return 0.60
    elif elapsed_seconds < 2100:
        return 0.15
    else:
        return 0.65


def _scenario_d_talk_time(elapsed_seconds: float) -> float:
    """Scenario D talk time: varies between 100s and 140s."""
    if elapsed_seconds < 900:
        return 100.0
    elif elapsed_seconds < 2100:
        return 80.0
    else:
        return 140.0


SCENARIOS: dict[str, ScenarioConfig] = {
    "A": ScenarioConfig(
        name="A",
        description="20% answer rate, 120s mean talk time (low answer rate)",
        answer_rate=0.20,
        mean_talk_time_seconds=120.0,
    ),
    "B": ScenarioConfig(
        name="B",
        description="50% answer rate, 90s mean talk time (medium answer rate)",
        answer_rate=0.50,
        mean_talk_time_seconds=90.0,
    ),
    "C": ScenarioConfig(
        name="C",
        description="70% answer rate, 180s mean talk time (high answer rate, long talk)",
        answer_rate=0.70,
        mean_talk_time_seconds=180.0,
    ),
    "D": ScenarioConfig(
        name="D",
        description="Time-varying answer rate: 60% -> 15% drop -> 65% spike",
        answer_rate=_scenario_d_answer_rate,
        mean_talk_time_seconds=_scenario_d_talk_time,
    ),
}


def get_scenario(name: str) -> ScenarioConfig:
    key = name.upper()
    if key not in SCENARIOS:
        raise ValueError(f"Unknown scenario '{name}'. Valid scenarios: {list(SCENARIOS.keys())}")
    return SCENARIOS[key]
