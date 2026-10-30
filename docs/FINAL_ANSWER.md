# Architectural Solution: Safe Predictive Dialing

**Question**: *How would you build a SmartDialer that gets as much of the utilization benefit of predictive dialing as possible, while retaining the deterministic safety characteristics of progressive dialing?*

---

I architect the dialer around a strict separation of authority: **statistical pacing proposes, but a deterministic safety controller disposes**.

To capture predictive utilization, the pacing engine estimates answer rates using a dual-window Beta-Bayesian model. Instead of dialing against the expected mean, it computes a pessimistic upper-bound rate $p_{hi}$ with an asymmetric drift guard that catches surges immediately. It then solves a Binomial tail bound: find the maximum concurrent dials $N^*$ such that the probability of answering calls exceeding available agent capacity remains below risk tolerance $\epsilon$ (e.g. 1%). **The estimator decides how much safe headroom to use, never how much headroom exists.**

To retain progressive safety, dials are gated by a deterministic Safety Controller that knows nothing of statistical math. It enforces hard capacity caps, circuit breakers, and rolling abandonment budgets with hysteresis. If rolling abandonment exceeds regulatory limits (e.g. 3%), or if the answer-rate estimate is stale or low-sample, the controller immediately forces a fallback to progressive dialing (one dial per free agent).

Finally, every allocation is re-validated inside the atomic database transaction that commits it using PostgreSQL row locks (`SELECT ... FOR UPDATE SKIP LOCKED`) and partial unique indexes. Zero telecom calls occur inside database transactions, eliminating connection pool starvation. Under sudden agent drops, a capacity guard cancels excess ringing calls newest-first.
