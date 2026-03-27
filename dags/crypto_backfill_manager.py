"""
crypto_backfill_manager
=======================
Detects missing 30-min pipeline slots and triggers backfill dagRuns
for crypto_market_snapshots.

Tasks:
    detect_gaps   — finds missing slots, writes to orchestration.backfill_queue
    run_backfill  — reads backfill_queue, POSTs dagRuns via Airflow REST API

Trigger manually from the UI whenever you want to recover missed batches.
Schedule: None (manual only).
"""

import logging

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from crypto import backfill_utils

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def _detect_gaps(**ctx):
    """
    Queries pipeline_runs to find 30-min slots with no successful run,
    then inserts them into orchestration.backfill_queue with status='pending'.
    Slots already in the queue are skipped (idempotent).
    """
    inserted = backfill_utils.detect_gaps()
    log.info("detect_gaps finished — %d new slots queued", inserted)
    return f"{inserted} slots added to backfill_queue"


def _run_backfill(**ctx):
    """
    Reads all pending rows from backfill_queue and triggers a dagRun of
    crypto_market_snapshots for each one via the Airflow REST API.
    Marks each row as 'triggered' after a successful POST.
    Waits a few seconds between triggers to avoid overloading the worker.
    """
    triggered = backfill_utils.trigger_runs()
    log.info("run_backfill finished — %d dagRuns triggered", triggered)
    return f"{triggered} dagRuns triggered"


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with dag(
    dag_id="crypto_backfill_manager",
    description="Detects missing pipeline slots and triggers backfill runs",
    schedule_interval=None,      # manual only
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "backfill", "maintenance"],
) as dag_obj:

    t_detect = PythonOperator(
        task_id="detect_gaps",
        python_callable=_detect_gaps,
    )

    t_backfill = PythonOperator(
        task_id="run_backfill",
        python_callable=_run_backfill,
    )

    t_detect >> t_backfill
