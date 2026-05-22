# Pipeline Run Order

## First-Time Setup (run once)

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

python create_tables.py       # Creates ai_service_behavior_memory + ai_detector_staging1
python create_run_log.py      # Creates ai_pattern_run_log
```

## Full Pipeline (run in this order)

```bash
# Step 1 — Baseline views
python baseline_view.py        # 14-day rolling baseline → ai_baseline_view_2
python baseline_stats_30d.py   # 30-day delta stats     → ai_baseline_stats_30d

# Step 2 — Staging candidates
python stagging.py             # Populates ai_detector_staging1

# Step 3 — Pattern detection
python daily.py
python weekly.py
python drift.py
python volume1.py
python sudden.py
```

## Schedules (for reference)

| Frequency | Scripts |
|---|---|
| Every hour | `drift.py`, `sudden.py` |
| Daily 00:00 UTC | `baseline_view.py` → `baseline_stats_30d.py` → `stagging.py` → `daily.py` |
| Daily 23:00 UTC | `volume1.py` |
| Weekly Sunday 00:00 UTC | `stagging.py` → `weekly.py` |

## Notes

- `drift.py` and `sudden.py` require **recent live data** (within the last 24h) — they will produce 0 patterns on historical/stale data.
- `weekly.py` requires at least 4 weeks of data to produce patterns reliably.
- Only **CHRONIC** and **AT_RISK** services (breach_ratio ≥ 0.3 over 14 days) are evaluated for daily/weekly patterns. HEALTHY services are intentionally skipped.
- Each pattern script runs `OPTIMIZE TABLE ai_service_behavior_memory FINAL` after inserting to prevent duplicates.
