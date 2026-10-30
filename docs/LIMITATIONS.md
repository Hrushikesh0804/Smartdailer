# Technical Limitations & Honest Appraisal

This document outlines the real-world limitations of the SmartDialer prototype, policy trade-offs, areas of technical ambiguity, and directions for future production hardening.

---

## 1. Core Architectural & Telephony Limitations

### A. $\epsilon$ (Risk Tolerance) and `HOLD_TIMEOUT` Are Business Policies, Not Mathematical Truths
* In predictive mode, the risk budget $\epsilon$ (e.g., 0.01 or 0.05) and `HOLD_TIMEOUT` (e.g., 2.0 seconds) are subjective policy choices mandated by regulatory compliance (e.g., FTC / FCC / Ofcom rules requiring abandonment under 3.0% with maximum 2-second hold after answer).
* While the Binomial tail bound strictly controls risk given a stationary distribution, real human borrowers do not answer uniformly: a flash crowd event or breaking news can cause an answer burst that breaches $\epsilon$.

### B. Answer-Time Claim Has Inherent Abandonment Risk (Bounded, Not Eliminated)
* Unlike progressive dialing (where an agent is reserved before the dial leaves the wire), predictive dialing over-dials based on statistical completion rates.
* If multiple borrowers answer within the same 500ms window while active agent conversations run unexpectedly long, some borrowers **will** experience hold time, and will be abandoned if hold exceeds 2.0s. The math bounds this probability; it cannot reduce it to zero.

### C. Provider B Timeouts Without Status Query Remain Irreducibly Ambiguous
* When a telecom provider initiation returns `TIMEOUT` and does **not** support out-of-band status queries (like our `MockProviderB`), the system cannot differentiate between:
  1. *The provider received the request, dialed the borrower, and our client timed out waiting for the HTTP response.*
  2. *The network dropped the packet before reaching the provider, and no call was ever made.*
* In this scenario, the dialer marks the call `UNKNOWN` with a lease deadline. If no webhook arrives before `reconcile_deadline`, the sweeper must eventually cancel/fail the call. If the borrower answers hours later, it is handled as an unexpected callback. No software design can eliminate this ambiguity without provider-side status query APIs.

### D. Synthetic Data & Mock Providers vs Real Telephony Networks
* Real SIP/PSTN networks exhibit carrier behavior that mock providers cannot simulate:
  - Asymmetric audio delays (dead air before ringback).
  - Answering Machine Detection (AMD) delays (taking 2–4 seconds after pickup to distinguish human speech from voicemail greetings).
  - SS7 / ISDN network congestion codes (SIP 503 Service Unavailable, SIP 486 Busy Here, SIP 487 Request Terminated).
  - Flapping Webhook HTTP Delivery: Webhook deliveries often arrive out of order, in bursts, or delayed by minutes under telecom outage conditions.

### E. Single-Region PostgreSQL Primary Blast Radius
* Because concurrency correctness relies on row-level locking (`SELECT ... FOR UPDATE SKIP LOCKED`), advisory locks, and partial unique indexes, the architecture requires a single primary write node.
* A catastrophic loss of the primary database node requires failover (e.g. via Patroni / AWS Aurora failover), during which active in-flight calls must be reconciled or terminated.

---

## 2. Least Confident Part of the Implementation

**The interplay between Answering Machine Detection (AMD) latency and the 2.0-second Hold Timeout**:
* In this prototype, the carrier event `ANSWERED` immediately starts the hold timer (`held_since = now`).
* In real-world telephony (Twilio, FreeSWITCH, Asterisk), Answering Machine Detection (AMD) takes 1.5 to 3.5 seconds of audio analysis *after* handset pickup to classify whether the respondent is a live human or an answering machine.
* If AMD analysis takes 2.5 seconds, a 2.0-second hold timer would cause the system to prematurely abandon live human calls before AMD has even classified the audio stream. Integrating realistic AMD pipelines requires decoupling the raw carrier `CONNECT` signal from the `HUMAN_SPEECH_DETECTED` event, and handling pre-connect audio buffering.

---

## 3. What I Would Do With Another Week

1. **True Multiprocessing / Process-Pool Runner**:
   - Replace the multithreaded orchestrator with a process pool (one worker process per CPU core) to completely circumvent the Python Global Interpreter Lock (GIL) and scale beyond 10,000 agents on a single server.
2. **Dynamic Holding Music & Audio Streaming Stream**:
   - Implement WebSocket audio bridge handling (e.g. via FreeSWITCH ESL or Twilio Media Streams) to stream compliant introductory messages or comfort tones while a call is in `HELD` state.
3. **Adaptive Prior Bayesian Updating**:
   - Instead of static campaign priors ($a_0, b_0$), dynamically initialize campaign priors based on historical time-of-day and day-of-week response curves (e.g., Tuesday 10am has a 35% baseline answer rate, whereas Friday 7pm has a 12% baseline).
4. **Dead Letter Queue (DLQ) Management Tooling**:
   - Provide administrative CLI tools (`python -m smartdialer.events.dlq`) to inspect, retry, or purge quarantined and dead-lettered events that failed reconciliation.
5. **Prometheus Metrics Exporter**:
   - Add a lightweight Prometheus metrics endpoint exposing real-time gauges for in-flight calls, rolling abandonment rate, p95 allocation latency, and circuit breaker states.
