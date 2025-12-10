"""Utilities to clean daily transaction files for the medallion pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

RAW_FILE_TEMPLATE = "transactions_{ds_nodash}.csv"
CLEAN_FILE_TEMPLATE = "transactions_{ds_nodash}_clean.parquet"


def _coerce_amount(value: pd.Series) -> pd.Series:
    """Normalize numeric fields and drop non-coercible entries."""
    coerced = pd.to_numeric(value, errors="coerce")
    return coerced


def _normalize_status(value: pd.Series) -> pd.Series:
    normalized = value.fillna("").str.strip().str.lower()
    mapping = {
        "completed": "completed",
        "pending": "pending",
        "failed": "failed",
    }
    return normalized.map(mapping)


def clean_daily_transactions(
    execution_date: date,
    raw_dir: Path,
    clean_dir: Path,
    raw_template: str = RAW_FILE_TEMPLATE,
    clean_template: str = CLEAN_FILE_TEMPLATE,
) -> Path:
    """Read the raw CSV for the DAG date, clean it, and save a parquet file."""

    ds_nodash = execution_date.strftime("%Y%m%d")
    input_path = raw_dir / raw_template.format(ds_nodash=ds_nodash)
    output_path = clean_dir / clean_template.format(ds_nodash=ds_nodash)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Raw data not found for {execution_date}: {input_path}"
        )

    clean_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path)

    # Basic cleanup
    df.columns = [col.strip().lower() for col in df.columns]
    df = df.drop_duplicates()

    if "amount" in df.columns:
        df["amount"] = _coerce_amount(df["amount"])

    if "status" in df.columns:
        df["status"] = _normalize_status(df["status"])

    df = df.dropna(subset=["transaction_id", "customer_id", "amount", "status"])

    # Add simple derived fields for downstream dbt modeling
    if "transaction_ts" in df.columns:
        df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], errors="coerce")
        df = df.dropna(subset=["transaction_ts"])
        df["transaction_date"] = df["transaction_ts"].dt.date

    df.to_parquet(output_path, index=False)

    return output_path