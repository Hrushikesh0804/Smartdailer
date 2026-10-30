"""
tests/test_simulation_failures.py — Automated regression tests for all 6 failure demos.
"""

from __future__ import annotations

import pytest

from smartdialer.sim.failures.worker_crash import run_demo as demo_worker_crash
from smartdialer.sim.failures.provider_outage import run_demo as demo_provider_outage
from smartdialer.sim.failures.agent_drop import run_demo as demo_agent_drop
from smartdialer.sim.failures.duplicate_event_storm import run_demo as demo_duplicate_storm
from smartdialer.sim.failures.out_of_order_storm import run_demo as demo_out_of_order
from smartdialer.sim.failures.answer_spike import run_demo as demo_answer_spike


def test_failure_demo_worker_crash():
    demo_worker_crash()


def test_failure_demo_provider_outage():
    demo_provider_outage()


def test_failure_demo_agent_drop():
    demo_agent_drop()


def test_failure_demo_duplicate_event_storm():
    demo_duplicate_storm()


def test_failure_demo_out_of_order_storm():
    demo_out_of_order()


def test_failure_demo_answer_spike():
    demo_answer_spike()
