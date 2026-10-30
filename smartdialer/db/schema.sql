-- db/schema.sql
-- Idempotent DDL applied AFTER SQLAlchemy create_all().
-- All statements use IF NOT EXISTS so this file is safe to re-run.

-- =========================================================================
-- calls table indexes
-- =========================================================================

-- Partial unique index: at most ONE non-terminal call per agent.
-- Hard constraint backstop in case FOR UPDATE SKIP LOCKED is bypassed.
CREATE UNIQUE INDEX IF NOT EXISTS uq_calls_active_agent
    ON calls (agent_id)
    WHERE agent_id IS NOT NULL
      AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'ABANDONED');

-- Partial unique index: at most ONE non-terminal call per borrower.
CREATE UNIQUE INDEX IF NOT EXISTS uq_calls_active_borrower
    ON calls (borrower_id)
    WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'ABANDONED');

-- Partial index: fast lookup of AVAILABLE agents by campaign for allocator.
CREATE INDEX IF NOT EXISTS ix_agents_available_by_campaign
    ON agents (campaign_id, last_state_change_at)
    WHERE state = 'AVAILABLE';

-- Partial index: fast lookup of ELIGIBLE borrowers for allocator.
CREATE INDEX IF NOT EXISTS ix_borrowers_eligible_by_campaign
    ON borrowers (campaign_id, priority DESC, next_eligible_at ASC)
    WHERE state = 'ELIGIBLE';

-- Index for sweeper: find UNKNOWN calls past their reconcile_deadline.
CREATE INDEX IF NOT EXISTS ix_calls_unknown_reconcile
    ON calls (reconcile_deadline)
    WHERE state = 'UNKNOWN' AND reconcile_deadline IS NOT NULL;

-- Index for sweeper: find expired RESERVED/INITIATED leases.
CREATE INDEX IF NOT EXISTS ix_calls_lease_expired
    ON calls (lease_expires_at)
    WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'ABANDONED');

-- Index for held calls:
CREATE INDEX IF NOT EXISTS ix_calls_held_since
    ON calls (held_since)
    WHERE state = 'HELD';

-- Index for active in-flight calls:
CREATE INDEX IF NOT EXISTS ix_calls_campaign_active
    ON calls (campaign_id, state)
    WHERE state IN ('INITIATED', 'RINGING', 'ANSWERED');

-- =========================================================================
-- call_events table indexes
-- =========================================================================

-- Primary dedup constraint: one event_id per provider.
-- INSERT ... ON CONFLICT (provider, event_id) DO NOTHING relies on this.
CREATE UNIQUE INDEX IF NOT EXISTS uq_call_events_provider_event
    ON call_events (provider, event_id);

-- Fast lookup of pending events for the processor.
-- Covers both RECEIVED rows and QUARANTINED rows eligible for retry.
CREATE INDEX IF NOT EXISTS ix_call_events_pending
    ON call_events (next_retry_at NULLS FIRST)
    WHERE status IN ('RECEIVED', 'QUARANTINED');

-- Lookup by provider_call_id for resolving call_id when a callback races the
-- executor's Txn 2.
CREATE INDEX IF NOT EXISTS ix_call_events_provider_call_id
    ON call_events (provider_call_id);

-- =========================================================================
-- safety_decisions table indexes
-- =========================================================================

CREATE INDEX IF NOT EXISTS ix_safety_decisions_campaign
    ON safety_decisions (campaign_id, created_at DESC);

-- =========================================================================
-- pacing_decisions table indexes
-- =========================================================================

CREATE INDEX IF NOT EXISTS ix_pacing_decisions_campaign
    ON pacing_decisions (campaign_id, created_at DESC);

-- =========================================================================
-- Phase 8: Dashboard & Operator Tables
-- =========================================================================

CREATE TABLE IF NOT EXISTS operator_actions (
    id BIGSERIAL PRIMARY KEY,
    action VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    operator_token_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_operator_actions_created
    ON operator_actions (created_at DESC);

CREATE TABLE IF NOT EXISTS metrics_snapshots (
    id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT NOT NULL,
    in_flight INTEGER NOT NULL,
    capacity INTEGER NOT NULL,
    connected INTEGER NOT NULL,
    abandoned INTEGER NOT NULL,
    utilization_pct NUMERIC(5, 2) NOT NULL,
    p_mean NUMERIC(6, 4) NOT NULL,
    p_hi NUMERIC(6, 4) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_metrics_snapshots_campaign_time
    ON metrics_snapshots (campaign_id, created_at DESC);
