"""File-only Parquet extraction with no full-result materialization."""

from __future__ import annotations

import os
import tempfile
import warnings
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from .preflight import (
    PreflightReport,
    _as_list,
    _resolve_destination,
    build_process_preflight,
)
from .utils import _build_pl_query


def _validate_export_schemas(files: list[str]) -> None:
    """Check every local footer, including columns omitted from the result."""
    expected = None
    for file in files:
        schema = pl.read_parquet_schema(file)
        signature = list(schema.items())
        if expected is None:
            expected = signature
        elif signature != expected:
            raise ValueError(f"Parquet schema drift in source '{Path(file).name}'.")


def _export_preflight(
    obj, *, files, destination, select_cols, date_query, bvd_query, query
):
    report = build_process_preflight(
        obj,
        files=files,
        destination=destination,
        select_cols=select_cols,
        date_query=date_query,
        bvd_query=bvd_query,
        query=query,
        engine="polars",
        explicit_polars=True,
    )
    report.reason = "streaming_export"
    errors = report.errors
    if isinstance(query, pl.Expr):
        report.required_columns = (
            list(
                dict.fromkeys(
                    [*(report.required_columns or []), *query.meta.root_names()]
                )
            )
            or None
        )
    if any(Path(file).suffix.lower() != ".parquet" for file in files):
        errors.append("export_all() accepts Parquet sources only.")
    if query is not None and not isinstance(query, pl.Expr):
        errors.append(
            "export_all() query must be a Polars expression, not a string or callable."
        )
    if getattr(obj, "query_args", None) is not None:
        errors.append("export_all() does not accept inherited query_args.")
    if select_cols is not None and not select_cols:
        errors.append("export_all() select_cols must be non-empty or None.")
    if not isinstance(date_query, (list, tuple)) or len(date_query) != 4:
        errors.append(
            "date_query must contain start year, end year, column and null action."
        )
    generated = destination is None
    destination, destination_warnings, _ = _resolve_destination(
        obj,
        destination,
        write_required=True,
    )
    report.warnings.extend(
        warning for warning in destination_warnings if warning not in report.warnings
    )
    target = Path(destination if destination is not None else "")
    # A generated destination is a prefix; an explicit export destination is a filename.
    if generated:
        target = Path(str(target) + ".parquet")
    report.destination = str(target)
    report.would_write = True
    if target.suffix.lower() != ".parquet":
        errors.append("export_all() destination must be a .parquet filename.")
    if os.path.lexists(target):
        errors.append("Export destination already exists; overwriting is not allowed.")
    if any(target.resolve() == Path(file).resolve() for file in report.files):
        errors.append("Export destination must not be an input source or cache path.")
    if any(parent.exists() and not parent.is_dir() for parent in target.parents):
        errors.append("An export destination parent is not a directory.")
    report.warnings.append(
        "Streaming export writes one Parquet file and retains all inputs; output_format, "
        "file_size_mb, concat_files and delete_files do not control this method. "
        "Write permissions, disk/spill capacity and filesystem hard-link support are not verified by dry-run."
    )
    if not errors:
        try:
            local = [file for file in report.files if Path(file).is_file()]
            _validate_export_schemas(local)
            if len(local) == len(files):
                _, polars_bvd = obj._compose_bvd_filters(bvd_query)
                _build_pl_query(
                    local, select_cols, date_query, polars_bvd, query
                ).collect_schema()
            else:
                report.warnings.append(
                    "Remote schemas and query validity are deferred until sources are downloaded."
                )
        except Exception as exc:
            errors.append(
                f"Export source schema/query validation failed: {str(exc).splitlines()[0]}"
            )
    report.ok = not errors
    report.engine = "polars" if report.ok else "blocked"
    return report


def _sink_export(query: pl.LazyFrame, destination: str) -> None:
    """Publish a closed, readable file atomically without replacing another output."""
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".partial", dir=target.parent
    )
    os.close(fd)
    temporary = Path(name)
    try:
        query.sink_parquet(temporary, engine="streaming", maintain_order=True)
        # Own the handle even if Arrow rejects the footer during construction;
        # otherwise Windows can keep the failed reader's file locked.
        with temporary.open("rb") as source, pq.ParquetFile(source) as parquet_file:
            _ = parquet_file.metadata.num_rows
        # Hard linking is atomic and fails if the final name already exists on both OSes.
        # Do not fall back to copy/replace: those expose partial data or overwrite races.
        os.link(temporary, target)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            warnings.warn(
                f"Could not remove owned export temporary file '{temporary}'.",
                RuntimeWarning,
                stacklevel=2,
            )


def _export_all(
    obj,
    *,
    files=None,
    destination=None,
    num_workers=-1,
    select_cols=None,
    date_query=None,
    bvd_query=None,
    query=None,
    dry_run=False,
) -> list[str] | PreflightReport:
    files = _as_list(getattr(obj, "_remote_files", []) if files is None else files)
    select_cols = obj._select_cols if select_cols is None else select_cols
    date_query = obj.time_period if date_query is None else date_query
    query = obj.query if query is None else query
    report = _export_preflight(
        obj,
        files=files,
        destination=destination,
        select_cols=select_cols,
        date_query=date_query,
        bvd_query=bvd_query,
        query=query,
    )
    if dry_run:
        return report
    if not report.ok:
        raise ValueError("Export preflight failed: " + "; ".join(report.errors))
    if not obj._check_download(files):
        raise TimeoutError(
            "Files have not finished downloading within the expected timeout."
        )
    obj.download_all(files=files, num_workers=num_workers, async_mode=False)
    local_files = [obj._file_exist(file)[0] for file in files]
    if any(not obj._local_file_ready(file) for file in local_files):
        raise ValueError(
            "Missing or incomplete local files after download; export aborted."
        )
    _validate_export_schemas(local_files)
    _, polars_bvd = obj._compose_bvd_filters(bvd_query)
    query_plan = _build_pl_query(
        local_files, select_cols, date_query, polars_bvd, query
    )
    _sink_export(query_plan, report.destination)
    return [report.destination]
