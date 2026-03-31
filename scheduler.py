"""
Autonomous pipeline scheduler for AI Service Behavior Detector.
Run this as the entrypoint in a Docker container or pod:

    python scheduler.py

Schedule:
  - Every hour        : sudden.py, drift.py
  - Daily at 00:00    : baseline refresh -> stagging -> daily.py -> ai_Probability.py
  - Daily at 23:00    : volume1.py (end-of-day full-day data)
  - Weekly (Sunday)   : stagging.py -> weekly.py
"""

import subprocess
import sys
import time
import logging

import schedule

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

PYTHON = sys.executable  # Same interpreter that launched this script


def run_script(script: str) -> bool:
    """Run a pipeline script as a subprocess. Returns True on success."""
    logger.info(">> Running %s", script)
    result = subprocess.run([PYTHON, script], capture_output=True, text=True)

    if result.stdout:
        logger.info("[%s]\n%s", script, result.stdout.strip())
    if result.stderr:
        logger.warning("[%s stderr]\n%s", script, result.stderr.strip())

    if result.returncode != 0:
        logger.error("%s failed (exit %d)", script, result.returncode)
        return False

    logger.info("%s done", script)
    return True


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def hourly_job():
    """Every hour: detect sudden events and rolling-24h drift."""
    logger.info("=== Hourly job: sudden + drift ===")
    run_script("sudden.py")
    run_script("drift.py")


def daily_job():
    """Daily at 00:00: refresh baselines, stage candidates, run daily patterns, score risk."""
    logger.info("=== Daily job: baseline -> staging -> daily -> probability ===")
    ok = run_script("baseline_view.py")
    if not ok:
        logger.error("baseline_view.py failed - aborting daily job")
        return
    run_script("baseline_stats_30d.py")
    ok = run_script("stagging.py")
    if not ok:
        logger.error("stagging.py failed - skipping daily.py")
        return
    run_script("daily.py")
    run_script("ai_Probability.py")


def eod_job():
    """Daily at 23:00: volume detection (needs full day of data)."""
    logger.info("=== End-of-day job: volume ===")
    run_script("volume1.py")


def weekly_job():
    """Weekly on Sunday 00:00: re-stage and run weekly pattern detection."""
    logger.info("=== Weekly job: staging -> weekly ===")
    ok = run_script("stagging.py")
    if not ok:
        logger.error("stagging.py failed - skipping weekly.py")
        return
    run_script("weekly.py")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    logger.info("Scheduler starting - running initial hourly + daily jobs")

    # Run once immediately on startup so the container is useful right away
    hourly_job()
    daily_job()

    # Recurring schedule
    schedule.every().hour.do(hourly_job)
    schedule.every().day.at("00:00").do(daily_job)
    schedule.every().day.at("23:00").do(eod_job)
    schedule.every().sunday.at("00:00").do(weekly_job)

    logger.info(
        "Scheduled: hourly (sudden+drift) | 00:00 daily (baseline+staging+daily+prob) "
        "| 23:00 daily (volume) | Sunday 00:00 (weekly)"
    )

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
