"""
smartdialer.orchestrator.run — CLI runner for worker threads.

Usage:
    python -m smartdialer.orchestrator.run --workers N --campaign X

Concurrency:
- Standard library threading.
- ONE SQLAlchemy Session per worker thread, never shared.
- Graceful shutdown on SIGINT/SIGTERM via threading.Event.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time

from smartdialer.db.session import get_session_factory
from smartdialer.orchestrator.tick import run_tick

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
)
logger = logging.getLogger("smartdialer.orchestrator")


def worker_loop(
    worker_id: int,
    session_factory,
    campaign_id: int,
    provider_name: str,
    interval_seconds: float,
    stop_event: threading.Event,
) -> None:
    """Worker thread execution loop."""
    logger.info(f"Worker-{worker_id} started for campaign {campaign_id}.")
    while not stop_event.is_set():
        try:
            summary = run_tick(
                session_factory=session_factory,
                campaign_id=campaign_id,
                provider_name=provider_name,
            )
            if summary.get("allocated_calls", 0) > 0:
                logger.info(f"Worker-{worker_id} ticked: {summary}")
        except Exception as e:
            logger.error(f"Worker-{worker_id} error in tick: {e}", exc_info=True)

        stop_event.wait(timeout=interval_seconds)

    logger.info(f"Worker-{worker_id} stopped cleanly.")


def main() -> None:
    parser = argparse.ArgumentParser(description="SmartDialer Orchestrator Worker CLI")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker threads.")
    parser.add_argument("--campaign", type=int, default=1, help="Campaign ID to dial for.")
    parser.add_argument("--provider", type=str, default="mock_a", help="Provider name to use.")
    parser.add_argument("--interval", type=float, default=1.0, help="Tick interval in seconds.")
    args = parser.parse_args()

    stop_event = threading.Event()

    def signal_handler(signum, frame):
        logger.info("Shutdown signal received. Stopping worker threads...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    session_factory = get_session_factory()

    threads = []
    for i in range(args.workers):
        t = threading.Thread(
            target=worker_loop,
            name=f"WorkerThread-{i}",
            args=(
                i,
                session_factory,
                args.campaign,
                args.provider,
                args.interval,
                stop_event,
            ),
            daemon=True,
        )
        threads.append(t)
        t.start()

    logger.info(f"Launched {args.workers} worker(s) for campaign {args.campaign}. Press Ctrl+C to stop.")

    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop_event.set()

    for t in threads:
        t.join(timeout=5.0)

    logger.info("All workers terminated. Goodbye.")


if __name__ == "__main__":
    main()
