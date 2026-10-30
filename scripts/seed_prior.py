"""
scripts/seed_prior.py — Seed Beta priors from historical dataset.

Rules & Compliance
------------------
1. Pure standard library (csv, json, os). No pandas, no numpy.
2. Only ANSWERED is counted as answered.
   VOICEMAIL / ANSWERING_MACHINE is counted as UNANSWERED (human not reached).
3. Contact details are never logged or persisted. Only aggregate (alpha, beta) counts.
4. Skips gracefully if the dataset file is missing.
"""

from __future__ import annotations

import csv
import json
import os
import sys
from pathlib import Path

# Ensure project root is in sys.path when executed directly
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from smartdialer.config import settings



def seed_priors(csv_path: str | None = None, output_path: str | None = None) -> bool:
    target_csv = csv_path or os.environ.get("COLLECTIONS_DATASET_PATH", settings.collections_dataset_path)
    target_out = output_path or settings.priors_file_path

    if not os.path.exists(target_csv):
        print(f"[seed_prior] Dataset not found at '{target_csv}'. Skipping gracefully.")
        return False

    print(f"[seed_prior] Reading dataset from '{target_csv}'...")

    # campaign_id -> {"answered": count, "unanswered": count}
    stats: dict[str, dict[str, int]] = {}

    with open(target_csv, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Map campaign or disposition
            campaign = row.get("campaign_id") or row.get("campaign") or "default"
            status = (row.get("disposition") or row.get("status") or "").strip().upper()

            if campaign not in stats:
                stats[campaign] = {"answered": 0, "unanswered": 0}

            # Only ANSWERED is treated as answered. VOICEMAIL is NOT answered.
            if status == "ANSWERED":
                stats[campaign]["answered"] += 1
            else:
                stats[campaign]["unanswered"] += 1

    priors: dict[str, dict[str, float]] = {}
    for camp, counts in stats.items():
        ans = counts["answered"]
        unans = counts["unanswered"]
        # Empirical Bayes prior with Laplace smoothing
        alpha = ans + settings.beta_prior_alpha
        beta = unans + settings.beta_prior_beta
        priors[camp] = {
            "alpha": round(alpha, 2),
            "beta": round(beta, 2),
            "empirical_rate": round(ans / max(1, ans + unans), 4),
            "total_calls": ans + unans,
        }

    os.makedirs(os.path.dirname(os.path.abspath(target_out)), exist_ok=True)
    with open(target_out, "w", encoding="utf-8") as f:
        json.dump(priors, f, indent=2)

    print(f"[seed_prior] Successfully computed priors for {len(priors)} campaign(s) -> '{target_out}'.")
    return True


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else None
    seed_priors(path)
