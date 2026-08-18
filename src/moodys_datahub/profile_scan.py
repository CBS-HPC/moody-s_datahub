"""Bounded, privacy-safe aggregate profiling for complete DataHub tables."""

from __future__ import annotations

import os
import re
import sqlite3
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import fastavro
import pandas as pd
import pyarrow.parquet as pq

from .utils import _looks_like_bvd_id, profile_dataframe

DEFAULT_PROFILE_SAMPLE_ROWS = 250_000
DEFAULT_PROFILE_CHUNK_ROWS = 100_000


def _normalize_bvd_id(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    normalized = re.sub(r"[\s\-_./]", "", str(value).strip().upper())
    return normalized or None


def _read_profile_sample(path: Path, *, sample_rows: int) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, nrows=sample_rows)
    if suffix == ".parquet":
        parquet_file = pq.ParquetFile(path)
        batches = parquet_file.iter_batches(batch_size=sample_rows)
        try:
            return next(batches).to_pandas()
        except StopIteration:
            return pd.DataFrame(columns=parquet_file.schema.names)
    if suffix == ".avro":
        with path.open("rb") as handle:
            records = []
            for index, record in enumerate(fastavro.reader(handle)):
                if index >= sample_rows:
                    break
                records.append(record)
        return pd.DataFrame(records)
    raise ValueError(
        "all_files profiling supports .csv, .parquet, and .avro files; "
        f"received {path.name!r}."
    )


def _iter_profile_chunks(path: Path, *, chunk_rows: int) -> Iterator[pd.DataFrame]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        yield from pd.read_csv(path, chunksize=chunk_rows)
        return
    if suffix == ".parquet":
        parquet_file = pq.ParquetFile(path)
        for batch in parquet_file.iter_batches(batch_size=chunk_rows):
            yield batch.to_pandas()
        return
    if suffix == ".avro":
        with path.open("rb") as handle:
            records: list[dict[str, Any]] = []
            for record in fastavro.reader(handle):
                records.append(record)
                if len(records) >= chunk_rows:
                    yield pd.DataFrame(records)
                    records = []
            if records:
                yield pd.DataFrame(records)
        return
    raise ValueError(
        "all_files profiling supports .csv, .parquet, and .avro files; "
        f"received {path.name!r}."
    )


def _column_names(frame: pd.DataFrame) -> list[str]:
    return [str(column) for column in frame.columns]


def _empty_column_statistics(profile: pd.DataFrame) -> dict[str, dict[str, int]]:
    return {
        str(column): {
            "non_null_count": 0,
            "missing_count": 0,
            "bvd_id_like_count": 0,
            "date_parseable_count": 0,
            "date_invalid_count": 0,
        }
        for column in profile["column"].astype(str).tolist()
    }


def _safe_timestamp(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def _summary_columns(profile: pd.DataFrame) -> tuple[list[str], list[str], str | None]:
    date_columns = (
        profile.loc[profile["can_date_filter"].fillna(False).astype(bool), "column"]
        .astype(str)
        .tolist()
    )
    bvd_columns = (
        profile.loc[
            profile["logical_type"].isin(["identifier", "string", "categorical"]),
            "column",
        ]
        .astype(str)
        .tolist()
    )
    canonical = (
        "bvd_id_number"
        if "bvd_id_number" in set(profile["column"].astype(str))
        else None
    )
    if canonical is not None and canonical not in bvd_columns:
        bvd_columns.append(canonical)
    return date_columns, bvd_columns, canonical


def _write_unique_bvd_ids(connection: sqlite3.Connection, values: pd.Series) -> None:
    normalized = [_normalize_bvd_id(value) for value in values]
    rows = [(value,) for value in set(normalized) if value is not None]
    if rows:
        connection.executemany(
            "INSERT OR IGNORE INTO canonical_bvd_ids(value) VALUES (?)", rows
        )


def profile_all_files(
    files: list[str],
    *,
    data_product: str | None,
    table: str | None,
    resolve_file: Callable[[str], tuple[Path, bool]],
    operation_hints: bool,
    sample_rows: int = DEFAULT_PROFILE_SAMPLE_ROWS,
    chunk_rows: int = DEFAULT_PROFILE_CHUNK_ROWS,
    canonical_bvd_column: str | None = None,
    strict_schema: bool = True,
    scratch_dir: str | os.PathLike[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Profile every file sequentially and return a sample profile plus totals.

    ``resolve_file`` returns a local path and whether it existed before this
    scan. Newly downloaded paths are removed as soon as their scan completes.
    The returned values are aggregate counts and column names only.
    """
    if not files:
        raise ValueError("No files were selected for all_files profiling.")
    if sample_rows < 1 or chunk_rows < 1:
        raise ValueError("sample_rows and chunk_rows must both be positive integers.")

    first_path, first_preexisting = resolve_file(files[0])
    try:
        sample = _read_profile_sample(first_path, sample_rows=sample_rows)
        profile = profile_dataframe(
            sample,
            data_product=data_product,
            table=table,
            file_name=first_path.name,
            sample_strategy="first_file_sample",
            operation_hints=operation_hints,
        )
    except Exception:
        if not first_preexisting:
            first_path.unlink(missing_ok=True)
        raise
    if profile.empty:
        if not first_preexisting:
            first_path.unlink(missing_ok=True)
        raise ValueError("The first source file has no columns to profile.")

    expected_columns = _column_names(sample)
    date_columns, bvd_columns, default_canonical = _summary_columns(profile)
    canonical_bvd_column = canonical_bvd_column or default_canonical
    if (
        canonical_bvd_column is not None
        and canonical_bvd_column not in expected_columns
    ):
        if not first_preexisting:
            first_path.unlink(missing_ok=True)
        raise ValueError(
            f"canonical_bvd_column {canonical_bvd_column!r} is not present in the first file."
        )

    column_statistics = _empty_column_statistics(profile)
    date_statistics: dict[str, dict[str, object]] = {
        column: {"minimum": None, "maximum": None} for column in date_columns
    }
    source_file_count = len(files)
    scanned_file_count = 0
    row_count = 0
    schema_drift_files = 0

    scratch_root = Path(scratch_dir) if scratch_dir is not None else first_path.parent
    scratch_root.mkdir(parents=True, exist_ok=True)
    database_path = scratch_root / f".profile-{uuid.uuid4().hex}.sqlite3"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("CREATE TABLE canonical_bvd_ids(value TEXT PRIMARY KEY)")
        for index, remote_file in enumerate(files):
            if index == 0:
                local_path, preexisting = first_path, first_preexisting
            else:
                local_path, preexisting = resolve_file(remote_file)
            try:
                file_columns: list[str] | None = None
                for chunk in _iter_profile_chunks(local_path, chunk_rows=chunk_rows):
                    if file_columns is None:
                        file_columns = _column_names(chunk)
                        if file_columns != expected_columns:
                            schema_drift_files += 1
                            if strict_schema:
                                raise ValueError(
                                    "Schema drift detected while profiling all files: "
                                    f"expected {expected_columns!r}, received {file_columns!r}."
                                )
                    rows = int(len(chunk))
                    row_count += rows
                    for column, statistics in column_statistics.items():
                        if column not in chunk.columns:
                            statistics["missing_count"] += rows
                            continue
                        series = chunk[column]
                        non_null = int(series.notna().sum())
                        statistics["non_null_count"] += non_null
                        statistics["missing_count"] += rows - non_null

                    for column in bvd_columns:
                        if column not in chunk.columns:
                            continue
                        values = chunk[column].dropna().astype(str).str.strip()
                        values = values[values != ""]
                        matches = values.map(_looks_like_bvd_id)
                        column_statistics[column]["bvd_id_like_count"] += int(
                            matches.sum()
                        )

                    if (
                        canonical_bvd_column is not None
                        and canonical_bvd_column in chunk.columns
                    ):
                        _write_unique_bvd_ids(
                            connection, chunk[canonical_bvd_column].dropna()
                        )

                    for column in date_columns:
                        if column not in chunk.columns:
                            continue
                        parsed = pd.to_datetime(
                            chunk[column], errors="coerce", utc=True
                        )
                        parseable = int(parsed.notna().sum())
                        non_null = int(chunk[column].notna().sum())
                        column_statistics[column]["date_parseable_count"] += parseable
                        column_statistics[column]["date_invalid_count"] += (
                            non_null - parseable
                        )
                        if parseable:
                            minimum = parsed.min()
                            maximum = parsed.max()
                            previous_minimum = date_statistics[column]["minimum"]
                            previous_maximum = date_statistics[column]["maximum"]
                            if previous_minimum is None or minimum < previous_minimum:
                                date_statistics[column]["minimum"] = minimum
                            if previous_maximum is None or maximum > previous_maximum:
                                date_statistics[column]["maximum"] = maximum
                scanned_file_count += 1
            finally:
                if not preexisting:
                    local_path.unlink(missing_ok=True)
        connection.commit()
        unique_bvd_ids = (
            int(
                connection.execute("SELECT COUNT(*) FROM canonical_bvd_ids").fetchone()[
                    0
                ]
            )
            if canonical_bvd_column is not None
            else None
        )
    finally:
        connection.close()
        database_path.unlink(missing_ok=True)

    for index, row in profile.iterrows():
        column = str(row["column"])
        statistics = column_statistics[column]
        non_null_count = statistics["non_null_count"]
        profile.loc[index, "full_non_null_count"] = non_null_count
        profile.loc[index, "full_missing_count"] = statistics["missing_count"]
        profile.loc[index, "full_missing_pct"] = (
            statistics["missing_count"] / row_count if row_count else 0.0
        )
        profile.loc[index, "full_bvd_id_like_count"] = statistics["bvd_id_like_count"]
        profile.loc[index, "full_bvd_id_like_pct"] = (
            statistics["bvd_id_like_count"] / non_null_count if non_null_count else 0.0
        )
        profile.loc[index, "full_date_parseable_count"] = statistics[
            "date_parseable_count"
        ]
        profile.loc[index, "full_date_invalid_count"] = statistics["date_invalid_count"]
        if column in date_statistics:
            profile.loc[index, "full_date_min"] = _safe_timestamp(
                date_statistics[column]["minimum"]
            )
            profile.loc[index, "full_date_max"] = _safe_timestamp(
                date_statistics[column]["maximum"]
            )

    summary = {
        "scan_scope": "all_files",
        "status": "passed",
        "data_product": data_product,
        "table": table,
        "source_file_count": source_file_count,
        "scanned_file_count": scanned_file_count,
        "row_count": row_count,
        "schema_drift_files": schema_drift_files,
        "canonical_bvd_column": canonical_bvd_column,
        "unique_canonical_bvd_ids": unique_bvd_ids,
        "date_columns": [
            {
                "column": column,
                "minimum": _safe_timestamp(values["minimum"]),
                "maximum": _safe_timestamp(values["maximum"]),
                "parseable_count": column_statistics[column]["date_parseable_count"],
                "invalid_count": column_statistics[column]["date_invalid_count"],
            }
            for column, values in date_statistics.items()
        ],
        "bvd_id_columns": [
            {
                "column": column,
                "bvd_id_like_count": column_statistics[column]["bvd_id_like_count"],
                "non_null_count": column_statistics[column]["non_null_count"],
            }
            for column in bvd_columns
            if column_statistics[column]["bvd_id_like_count"]
        ],
    }
    return profile, summary
