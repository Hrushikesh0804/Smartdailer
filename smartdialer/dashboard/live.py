"""
smartdialer.dashboard.live — Live runner CLI for the Operator Dashboard.

Runs the real simulation engine against PostgreSQL with virtual time acceleration
while hosting the FastAPI dashboard server on Uvicorn.

CLI Usage:
    python -m smartdialer.dashboard.live --scenario A --agents 100 --speed 10 --seed 42
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import uvicorn

from smartdialer.dashboard.api import app, _live_demo_controls
from smartdialer.db.init_db import init_db
from smartdialer.db.session import make_engine, make_session_factory
from smartdialer.sim.engine import SimulationEngine
from smartdialer.sim.scenarios import get_scenario

logger = logging.getLogger("smartdialer.dashboard.live")


class LiveRunner:
    def __init__(
        self,
        scenario_name: str = "A",
        strategy: str = "predictive_adaptive",
        agents: int = 100,
        speed: float = 10.0,
        seed: int = 42,
        campaign_id: int = 1,
    ) -> None:
        self.scenario_name = scenario_name
        self.strategy = strategy
        self.agents = agents
        self.speed = max(0.1, float(speed))
        self.seed = seed
        self.campaign_id = campaign_id

        self.running = False
        self._sim_thread: threading.Thread | None = None
        self._engine = make_engine()
        self._session_factory = make_session_factory(self._engine)

        scenario = get_scenario(scenario_name)
        self.sim = SimulationEngine(
            scenario=scenario,
            strategy=strategy,
            total_agents=agents,
            seed=seed,
            session_factory=self._session_factory,
            campaign_id=campaign_id,
            answer_rate_override_fn=lambda: _live_demo_controls.get("sim_answer_rate"),
        )

    def start_sim(self) -> None:
        logger.info(
            f"Initializing simulation: Scenario={self.scenario_name}, Agents={self.agents}, "
            f"Speed={self.speed}x, Seed={self.seed}"
        )
        self.sim.setup()
        self.running = True

        def _loop() -> None:
            step_duration = 1.0  # 1 virtual second per tick
            real_sleep = step_duration / self.speed

            while self.running:
                t0 = time.time()
                try:
                    summary = self.sim.step(step_seconds=step_duration)
                    if self.sim.sweeper:
                        _live_demo_controls["last_sweeper_summary"] = self.sim.sweeper_action_counts
                except Exception as e:
                    logger.exception(f"Error during simulation step: {e}")

                elapsed = time.time() - t0
                sleep_time = max(0.001, real_sleep - elapsed)
                time.sleep(sleep_time)

        self._sim_thread = threading.Thread(target=_loop, name="SimLoopThread", daemon=True)
        self._sim_thread.start()
        logger.info("Simulation background loop started successfully.")

    def stop_sim(self) -> None:
        self.running = False
        if self._sim_thread and self._sim_thread.is_alive():
            self._sim_thread.join(timeout=2.0)
        logger.info("Simulation loop stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(description="SmartDialer Live Operator Dashboard Runner")
    parser.add_argument("--scenario", type=str, default="A", help="Simulation scenario (A, B, C, D)")
    parser.add_argument("--strategy", type=str, default="predictive_adaptive", help="Pacing strategy")
    parser.add_argument("--agents", type=int, default=100, help="Total number of agents to seed")
    parser.add_argument("--speed", type=float, default=10.0, help="Speed multiplier (virtual sec / real sec)")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed")
    parser.add_argument("--campaign-id", type=int, default=1, help="Campaign ID")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Dashboard listen host")
    parser.add_argument("--port", type=int, default=8000, help="Dashboard listen port")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # 1. Initialize DB schema
    init_db()

    # 2. Start simulation
    runner = LiveRunner(
        scenario_name=args.scenario,
        strategy=args.strategy,
        agents=args.agents,
        speed=args.speed,
        seed=args.seed,
        campaign_id=args.campaign_id,
    )
    runner.start_sim()

    print("\n" + "=" * 60)
    print(" SmartDialer Live Operator Dashboard running!")
    print(f" URL: http://{args.host}:{args.port}")
    print(f" Scenario: {args.scenario} | Speed: {args.speed}x | Agents: {args.agents}")
    print(" Press Ctrl+C to stop.")
    print("=" * 60 + "\n")

    try:
        uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    finally:
        runner.stop_sim()


if __name__ == "__main__":
    main()
