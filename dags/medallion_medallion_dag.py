"""Airflow DAG that orchestrates the medallion pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

# pylint: disable=import-error,wrong-import-position

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from include.transformations import (
    clean_daily_transactions,
)  # pylint: disable=wrong-import-position

RAW_DIR = BASE_DIR / "data/raw"
CLEAN_DIR = BASE_DIR / "data/clean"
QUALITY_DIR = BASE_DIR / "data/quality"
DBT_DIR = BASE_DIR / "dbt"
PROFILES_DIR = BASE_DIR / "profiles"
WAREHOUSE_PATH = BASE_DIR / "warehouse/medallion.duckdb"

def _build_env(ds_nodash: str) -> dict[str, str]:
    """
    Builds environment variables needed by dbt commands.
    """
    env = os.environ.copy()
    env.update(
        {
            "DBT_PROFILES_DIR": str(PROFILES_DIR),
            "CLEAN_DIR": str(CLEAN_DIR),
            "DS_NODASH": ds_nodash,
            "DUCKDB_PATH": str(WAREHOUSE_PATH),
        }
    )
    return env


def _run_dbt_command(command: str, ds_nodash: str) -> subprocess.CompletedProcess:
    """
    Executes a dbt command and returns the completed process.
    """
    env = _build_env(ds_nodash)
    return subprocess.run(
        [
            "dbt",
            command,
            "--project-dir",
            str(DBT_DIR),
        ],
        cwd = DBT_DIR,
        env = env,
        capture_output = True,
        text = True,
        check = False,
    )

def bronze_clean(ds_nodash: str, **context) -> None:
    """
    Bronze stage: cleans the raw transactions file for the given DAG run date.
    """
    # ds_nodash comes from Airflow as "YYYYMMDD" (e.g. "20251208").
    # Convert that string into a Python date object, which is what clean_daily_transactions expects as execution_date
    execution_date = datetime.strptime(ds_nodash, "%Y%m%d").date()

    # Run the cleaning utility:
    # - reads data/raw/transactions_{YYYYMMDD}.csv
    # - applies all cleaning and normalization steps
    # - writes data/clean/transactions_{YYYYMMDD}_clean.parquet
    clean_daily_transactions(
        execution_date = execution_date,
        raw_dir = RAW_DIR,
        clean_dir = CLEAN_DIR,
    )

def silver_dbt_run(ds_nodash: str, **context) -> None:
    """
    Silver stage: runs dbt models using the cleaned parquet produced in Bronze.

    - Expects `ds_nodash` in format "YYYYMMDD" (example: "20251201").
    - Calls `_run_dbt_command("run", ds_nodash)` which:
        * Builds the env with CLEAN_DIR, DS_NODASH, DUCKDB_PATH, DBT_PROFILES_DIR
        * Executes: `dbt run --project-dir <DBT_DIR>`
    - Inside the dbt project, models read the cleaned parquet file
      from data/clean/transactions_<ds>_clean.parquet and materialize
      Silver tables/views into the DuckDB file `medallion.duckdb`.
    """
    # Execute `dbt run` for this logical date
    result = _run_dbt_command("run", ds_nodash)

    # Log dbt output to Airflow logs for debugging
    print("=== dbt run stdout ===")
    print(result.stdout)
    print("=== dbt run stderr ===")
    print(result.stderr)

    # If dbt failed (non-zero return code), fail the Airflow task
    if result.returncode != 0:
        raise AirflowException(
            f"dbt run failed for ds_nodash={ds_nodash} "
            f"with return code {result.returncode}"
        )

def gold_dbt_tests(ds_nodash: str, **context) -> None:
    """
    Gold stage: runs dbt tests and writes a simple JSON data quality report for this logical date.

    - Runs `dbt test` for the current project using the same environment as Silver (CLEAN_DIR, DS_NODASH, DUCKDB_PATH).
    - Writes data/quality/dq_results_<ds_nodash>.json with:
        {
          "ds_nodash": "...",
          "status": "passed" | "failed",
          "dbt_return_code": <int>
        }
    - If tests fail (non-zero return code), the file is still created but the Airflow task is marked as failed by raising AirflowException.
    """
    # Run `dbt test` with the correct env vars
    result = _run_dbt_command("test", ds_nodash)

    # Decide status based on dbt exit code
    status = "passed" if result.returncode == 0 else "failed"

    # Make sure the quality directory exists
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    # Build the path for this day's JSON report
    dq_path = QUALITY_DIR / f"dq_results_{ds_nodash}.json"

    # Minimal payload for observability
    payload = {
        "ds_nodash": ds_nodash,
        "status": status,
        "stdout": result.stdout,
        "stderr": result.stderr
    }

    # Write JSON file to disk
    with dq_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # Log dbt output to Airflow logs
    print("=== dbt test stdout ===")
    print(result.stdout)
    print("=== dbt test stderr ===")
    print(result.stderr)

    # If tests failed, mark the task as failed
    if result.returncode != 0:
        raise AirflowException(
            f"dbt test failed for ds_nodash = {ds_nodash} "
            f"with return code {result.returncode}"
        )

def build_dag() -> DAG:
    """
    Construct the medallion pipeline DAG with bronze/silver/gold tasks.
    """
    with DAG(
        description = "Bronze/Silver/Gold medallion demo with pandas, dbt, and DuckDB",
        dag_id = "medallion_pipeline",
        schedule = "0 6 * * *",
        start_date = pendulum.datetime(2025, 12, 1, tz = "UTC"),
        catchup = True,
        max_active_runs = 1,
    ) as medallion_dag:

        # Bronze: clean raw CSV into parquet
        bronze_task = PythonOperator(
            task_id = "bronze_task",
            python_callable = bronze_clean,
            op_kwargs = {"ds_nodash": "{{ ds_nodash }}"},
        )

        # Silver: run dbt models (staging + marts) into DuckDB
        silver_task = PythonOperator(
            task_id = "silver_dbt_run",
            python_callable = silver_dbt_run,
            op_kwargs = {"ds_nodash": "{{ ds_nodash }}"},
        )

        # Gold: run dbt tests and write dq_results_<ds>.json
        gold_task = PythonOperator(
            task_id = "gold_dbt_tests",
            python_callable = gold_dbt_tests,
            op_kwargs = {"ds_nodash": "{{ ds_nodash }}"},
        )

        # Orchestration: Bronze -> Silver -> Gold
        bronze_task >> silver_task >> gold_task

    return medallion_dag

dag = build_dag()