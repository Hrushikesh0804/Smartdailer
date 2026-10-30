"""
smartdialer.config — updated with Phase 2 thresholds.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ------------------------------------------------------------------ #
    # Database                                                             #
    # ------------------------------------------------------------------ #

    db_url: str = Field(
        default="postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer",
        description="Primary database URL (postgresql+psycopg:// scheme).",
    )

    test_db_url: str = Field(
        default="postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer_test",
        description="Test database URL. Truncated between tests.",
    )

    db_pool_size: int = Field(
        default=10,
        description="SQLAlchemy pool size. Set >= worker thread count + 2.",
    )

    db_pool_max_overflow: int = Field(
        default=5,
        description="Extra connections allowed above pool_size under burst load.",
    )

    # ------------------------------------------------------------------ #
    # Agent reservation                                                    #
    # ------------------------------------------------------------------ #

    agent_reservation_ttl_seconds: int = Field(
        default=30,
        description=(
            "Seconds an agent can remain in RESERVED state before the recovery "
            "sweeper treats the reservation as stale and releases it."
        ),
    )

    # ------------------------------------------------------------------ #
    # Call lease                                                           #
    # ------------------------------------------------------------------ #

    call_lease_ttl_seconds: int = Field(
        default=120,
        description=(
            "Seconds a call row may remain in a non-terminal state without a "
            "heartbeat before the sweeper considers it lost."
        ),
    )

    # ------------------------------------------------------------------ #
    # Allocator                                                            #
    # ------------------------------------------------------------------ #

    max_allocations_per_tick: int = Field(
        default=10,
        description="Hard cap on call rows created per pacing tick.",
    )

    # ------------------------------------------------------------------ #
    # Pacing / Beta-Bayesian estimator (Phase 3)                          #
    # ------------------------------------------------------------------ #

    beta_prior_alpha: float = Field(
        default=1.0,
        description="Alpha (pseudo-successes) of the Beta prior for answer-rate.",
    )

    beta_prior_beta: float = Field(
        default=1.0,
        description="Beta (pseudo-failures) of the Beta prior for answer-rate.",
    )

    # ------------------------------------------------------------------ #
    # Safety controller (Phase 3)                                         #
    # ------------------------------------------------------------------ #

    max_lines_per_agent: float = Field(
        default=3.0,
        description="Maximum concurrent outbound calls per available agent.",
    )

    abandon_rate_threshold: float = Field(
        default=0.03,
        description="Rolling abandon rate ceiling before safety reduces dial rate.",
    )

    # ------------------------------------------------------------------ #
    # Provider health / circuit breaker                                    #
    # ------------------------------------------------------------------ #

    health_window_seconds: int = Field(
        default=60,
        description=(
            "Sliding window length (seconds) for rolling failure rate and "
            "latency statistics in the provider health tracker."
        ),
    )

    circuit_breaker_failure_threshold: float = Field(
        default=0.5,
        description=(
            "Fraction of calls that must fail within the health window before "
            "the circuit breaker trips to OPEN."
        ),
    )

    circuit_breaker_min_calls: int = Field(
        default=5,
        description=(
            "Minimum number of calls in the health window before the circuit "
            "breaker will trip.  Prevents false positives on cold start."
        ),
    )

    circuit_breaker_cooldown_seconds: int = Field(
        default=30,
        description=(
            "How long (seconds) the circuit stays OPEN before moving to "
            "HALF_OPEN and allowing a probe call."
        ),
    )

    circuit_breaker_probe_limit: int = Field(
        default=1,
        description="Number of probe calls allowed in HALF_OPEN before deciding.",
    )

    # ------------------------------------------------------------------ #
    # Event processor                                                      #
    # ------------------------------------------------------------------ #

    event_processor_batch_size: int = Field(
        default=50,
        description="Number of events claimed per process_pending() call.",
    )

    event_max_attempts: int = Field(
        default=5,
        description=(
            "Max processing attempts before an event is moved to DEAD_LETTER. "
            "Applies to QUARANTINED events (unknown call_id)."
        ),
    )

    event_quarantine_base_delay_seconds: int = Field(
        default=2,
        description=(
            "Base delay (seconds) for exponential backoff on QUARANTINED events. "
            "Delay = base * 2^(attempts-1), capped at max_delay."
        ),
    )

    event_quarantine_max_delay_seconds: int = Field(
        default=60,
        description="Maximum backoff delay (seconds) for QUARANTINED events.",
    )

    # ------------------------------------------------------------------ #
    # Sweeper / reconciliation                                             #
    # ------------------------------------------------------------------ #

    sweeper_advisory_lock_id: int = Field(
        default=42,
        description=(
            "PostgreSQL advisory lock ID used by the sweeper to ensure only one "
            "instance runs at a time.  Change if 42 conflicts with another app."
        ),
    )

    sweeper_interval_seconds: int = Field(
        default=10,
        description="How often the sweeper wakes up to scan for stale rows.",
    )

    reconcile_deadline_seconds: int = Field(
        default=300,
        description=(
            "Seconds after a TIMEOUT initiation result before the sweeper gives up "
            "reconciling and permanently marks the call FAILED/CANCELLED with "
            "a note about unresolved ambiguity."
        ),
    )

    # ------------------------------------------------------------------ #
    # Borrower retry backoff & limits                                      #
    # ------------------------------------------------------------------ #

    borrower_retry_backoff_seconds: int = Field(
        default=3600,
        description=(
            "Fixed backoff (seconds) fallback for borrower retry."
        ),
    )

    borrower_base_backoff_seconds: int = Field(
        default=300,
        description="Base delay (seconds) for exponential retry backoff.",
    )

    borrower_max_backoff_seconds: int = Field(
        default=86400,
        description="Maximum retry backoff delay (seconds, max 24 hours).",
    )

    max_borrower_attempts: int = Field(
        default=5,
        description="Maximum attempts before moving borrower to DONE or DO_NOT_CALL.",
    )

    borrower_terminal_state_on_max_attempts: str = Field(
        default="DONE",
        description="Terminal state for borrower upon reaching max attempts: DONE or DO_NOT_CALL.",
    )

    # ------------------------------------------------------------------ #
    # Safety Controller (Phase 3)                                         #
    # ------------------------------------------------------------------ #

    max_overdial_ratio: float = Field(
        default=3.0,
        description="Regulatory limit on (in_flight + approved) / capacity for predictive mode.",
    )

    abandonment_budget: float = Field(
        default=0.03,
        description="Maximum allowed rolling abandonment rate (e.g. 0.03 = 3%).",
    )

    abandonment_hysteresis_ratio: float = Field(
        default=0.5,
        description="Multiplier for budget to re-enter predictive (e.g. 0.5 * 0.03 = 0.015).",
    )

    abandonment_cooldown_seconds: int = Field(
        default=60,
        description="Cooldown period (seconds) before allowing re-entry to predictive mode.",
    )

    min_estimate_samples: int = Field(
        default=20,
        description="Minimum sample count required in estimate_meta before predictive dialing is approved.",
    )

    max_estimate_age_seconds: int = Field(
        default=300,
        description="Maximum age of answer rate estimate (seconds) before considered stale.",
    )

    # ------------------------------------------------------------------ #
    # Predictive Pacing & Estimator (Phase 4)                            #
    # ------------------------------------------------------------------ #

    binomial_epsilon: float = Field(
        default=0.02,
        description="Upper bound probability of answered calls exceeding capacity.",
    )

    estimator_z_score: float = Field(
        default=1.645,
        description="Z-score for Beta posterior upper bound p_hi (1.645 ~ 95% one-sided, 1.96 ~ 97.5%).",
    )

    hold_timeout_seconds: float = Field(
        default=5.0,
        description="Maximum seconds an answered call can wait in HELD before being abandoned.",
    )

    expected_ring_time_seconds: float = Field(
        default=6.0,
        description="Expected seconds from dial to answer.",
    )

    expected_talk_time_seconds: float = Field(
        default=60.0,
        description="Expected duration of a connected conversation in seconds.",
    )

    expected_wrap_up_seconds: float = Field(
        default=15.0,
        description="Expected duration of post-call wrap-up work in seconds.",
    )

    estimator_recent_decay: float = Field(
        default=0.90,
        description="Exponential decay factor per observation for the short-term window.",
    )

    estimator_long_decay: float = Field(
        default=0.99,
        description="Exponential decay factor per observation for the long-term window.",
    )

    priors_file_path: str = Field(
        default="data/priors.json",
        description="Path to JSON file containing seeded campaign priors.",
    )

    collections_dataset_path: str = Field(
        default="data/calls.csv",
        description="Path to CSV dataset for seeding Beta priors.",
    )


# Module-level singleton.
settings = Settings()
