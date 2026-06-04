from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .utils import _collect_bvd_clause_columns, _normalize_bvd_clause_group


@dataclass
class PreflightReport:
    ok: bool
    engine: str
    reason: str | None
    files: list[str]
    missing_files: list[str]
    warnings: list[str]
    errors: list[str]
    required_columns: list[str] | None
    resolved_date_column: str | None
    resolved_bvd_columns: list[str] | None
    destination: str | None
    would_prompt: bool
    would_download: bool
    would_write: bool


def _as_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, (str, Path)):
        return [str(values)]
    if isinstance(values, list):
        return [str(value) for value in values]
    if isinstance(values, tuple):
        return [str(value) for value in values]
    return [str(values)]


def _current_time_stamp() -> str:
    return datetime.now().strftime("%y%m%d%H%M")


def _candidate_local_path(obj) -> str | None:
    local_path = getattr(obj, "_local_path", None)
    if local_path is not None:
        return str(local_path)

    remote_path = getattr(obj, "_remote_path", None)
    if remote_path is None:
        return None

    set_data_product = getattr(obj, "_set_data_product", None)
    set_table = getattr(obj, "_set_table", None)
    if set_data_product is None or set_table is None:
        return None

    time_stamp = getattr(obj, "_time_stamp", None)
    if time_stamp:
        folder_name = f"{set_data_product}_exported {_format_timestamp(time_stamp)}"
    else:
        folder_name = set_data_product

    return str(Path("Data Products") / folder_name / set_table)


def _format_timestamp(timestamp: str) -> str:
    return str(timestamp).replace(" ", "_").replace(":", "-")


def _resolve_destination(
    obj,
    destination: str | None,
    *,
    write_required: bool,
) -> tuple[str | None, list[str], bool]:
    warnings: list[str] = []
    if not write_required:
        return destination, warnings, False

    if destination is None:
        if getattr(obj, "_remote_path", None) is not None:
            suffix = Path(str(obj._remote_path)).name
        elif getattr(obj, "_local_path", None) is not None:
            suffix = Path(str(obj._local_path)).name
        else:
            suffix = "output"

        destination = str(Path.cwd() / f"{_current_time_stamp()}_{suffix}")
        warnings.append(
            "Destination was not provided. Dry-run generated the same default "
            "destination the runtime path would use."
        )

    return destination, warnings, True


def _resolve_files(obj, files: list[str]) -> tuple[list[str], list[str], list[str], list[str]]:
    normalized_files = _as_list(files)
    resolved_files: list[str] = []
    missing_files: list[str] = []
    warnings: list[str] = []
    errors: list[str] = []

    local_candidate_root = _candidate_local_path(obj)
    known_local_files = set(_as_list(getattr(obj, "_local_files", [])))
    known_remote_files = set(_as_list(getattr(obj, "_remote_files", [])))

    for file in normalized_files:
        file_path = Path(file)
        base_name = file_path.name

        if file_path.exists():
            resolved_files.append(str(file_path.resolve()))
            continue

        if local_candidate_root is not None:
            candidate = Path(local_candidate_root) / base_name
            if candidate.exists():
                resolved_files.append(str(candidate.resolve()))
                continue

        if file in known_local_files or base_name in known_local_files:
            if local_candidate_root is None:
                resolved_files.append(str(file_path))
                warnings.append(
                    f"Local file '{file}' is known, but no local path is configured. "
                    "Dry-run will preserve the provided path."
                )
            else:
                candidate = Path(local_candidate_root) / base_name
                resolved_files.append(str(candidate))
                if not candidate.exists():
                    warnings.append(
                        f"'{file}' is not present locally and would be read from "
                        f"'{candidate}'."
                    )
            continue

        if file in known_remote_files or base_name in known_remote_files:
            if local_candidate_root is None:
                resolved_files.append(str(file_path))
                warnings.append(
                    f"'{file}' is remote and would be downloaded before processing."
                )
            else:
                candidate = Path(local_candidate_root) / base_name
                resolved_files.append(str(candidate))
                warnings.append(
                    f"'{file}' is not present locally and would be downloaded before processing."
                )
            continue

        missing_files.append(file)

    return resolved_files, missing_files, warnings, errors


def _resolve_bvd_state(obj, bvd_query=None) -> tuple[str | None, list[str] | None, list[str], bool]:
    warnings: list[str] = []
    would_prompt = False

    base_clause = None
    and_clauses = []
    or_clauses = []

    if bvd_query is None:
        compose = getattr(obj, "_compose_bvd_filters", None)
        if callable(compose):
            pandas_query, polars_query = compose(None)
        else:
            pandas_query, polars_query = None, None
    else:
        compose = getattr(obj, "_compose_bvd_filters", None)
        if callable(compose):
            pandas_query, polars_query = compose(bvd_query)
        else:
            pandas_query, polars_query = None, None

    if isinstance(polars_query, dict):
        base_clause = polars_query.get("base")
        and_clauses = _normalize_bvd_clause_group(polars_query.get("and"))
        or_clauses = _normalize_bvd_clause_group(polars_query.get("or"))
    elif isinstance(polars_query, (list, tuple)):
        base_clause = _normalize_bvd_clause(polars_query)

    resolved_columns = _collect_bvd_clause_columns(base_clause, and_clauses, or_clauses)
    if resolved_columns is None:
        resolved_columns = None
    else:
        resolved_columns = list(resolved_columns)

    if base_clause is not None and not base_clause.get("columns"):
        warnings.append("BvD filter is present but has no active columns.")

    if getattr(obj, "_interactive", True) is False:
        would_prompt = False
    else:
        would_prompt = False

    return pandas_query, resolved_columns, warnings, would_prompt


def _resolve_required_columns(obj, select_cols=None, date_query=None, bvd_query=None):
    required_columns = []
    resolved_date_column = None
    warnings: list[str] = []
    would_prompt = False

    current_select_cols = select_cols if select_cols is not None else getattr(obj, "_select_cols", None)
    if current_select_cols is not None:
        required_columns.extend(_as_list(current_select_cols))

    if date_query is None:
        date_query = getattr(obj, "_time_period", [None, None, None, "remove"])
    if isinstance(date_query, (list, tuple)) and len(date_query) >= 3:
        resolved_date_column = date_query[2]
        if resolved_date_column is not None:
            required_columns.append(resolved_date_column)

    pandas_bvd_query, resolved_bvd_columns, bvd_warnings, bvd_prompt = _resolve_bvd_state(
        obj, bvd_query
    )
    warnings.extend(bvd_warnings)
    would_prompt = would_prompt or bvd_prompt

    if resolved_bvd_columns is not None:
        required_columns.extend(resolved_bvd_columns)

    if required_columns:
        required_columns = list(dict.fromkeys(required_columns))
    else:
        required_columns = None

    return required_columns, resolved_date_column, resolved_bvd_columns, warnings, would_prompt, pandas_bvd_query


def validate_backend_compatibility(
    obj,
    *,
    files: list[str],
    query=None,
    query_args=None,
    engine: str = "auto",
    n_batches: int | None = None,
    pool_method=None,
    raw_bvd_query=None,
    polars_bvd_query=None,
) -> tuple[str, str | None, list[str], list[str]]:
    warnings: list[str] = []
    errors: list[str] = []

    choose = getattr(obj, "_choose_process_engine", None)
    if callable(choose):
        chosen_engine, reason = choose(
            files=files,
            query=query,
            pool_method=pool_method,
            n_batches=n_batches,
            raw_bvd_query=raw_bvd_query,
            polars_bvd_query=polars_bvd_query,
        )
    else:
        chosen_engine, reason = "pandas", "unsupported"
        warnings.append("Backend chooser is unavailable; defaulting to pandas.")

    if engine == "pandas" and isinstance(query, pl.Expr):
        errors.append("Polars expressions are not supported by the pandas backend.")

    if engine == "polars" and chosen_engine != "polars":
        errors.append(
            "Requested Polars backend is incompatible with the provided files/query."
        )

    if engine == "auto" and chosen_engine == "pandas" and isinstance(query, pl.Expr):
        errors.append(
            "Auto backend selection would fall back to pandas, but the query is a Polars expression."
        )

    return chosen_engine, reason, warnings, errors


def build_process_preflight(
    obj,
    *,
    files=None,
    destination: str | None = None,
    select_cols=None,
    date_query=None,
    bvd_query=None,
    query=None,
    query_args=None,
    engine: str = "auto",
    n_batches: int | None = None,
    pool_method=None,
    row_limit: int | None = None,
) -> PreflightReport:
    effective_files = _as_list(files if files is not None else getattr(obj, "_remote_files", []))

    resolved_files, missing_files, file_warnings, file_errors = _resolve_files(obj, effective_files)
    required_columns, resolved_date_column, resolved_bvd_columns, filter_warnings, would_prompt, _pandas_bvd_query = _resolve_required_columns(
        obj,
        select_cols=select_cols,
        date_query=date_query,
        bvd_query=bvd_query,
    )

    compose = getattr(obj, "_compose_bvd_filters", None)
    composed_polars_query = None
    if callable(compose):
        _, composed_polars_query = compose(bvd_query)

    chosen_engine, reason, engine_warnings, engine_errors = validate_backend_compatibility(
        obj,
        files=effective_files,
        query=query if query is not None else getattr(obj, "query", None),
        query_args=query_args if query_args is not None else getattr(obj, "query_args", None),
        engine=engine,
        n_batches=n_batches,
        pool_method=pool_method,
        raw_bvd_query=bvd_query,
        polars_bvd_query=composed_polars_query,
    )

    write_required = bool(getattr(obj, "output_format", None))
    destination, destination_warnings, would_write = _resolve_destination(
        obj,
        destination,
        write_required=write_required,
    )

    warnings = [
        *file_warnings,
        *filter_warnings,
        *engine_warnings,
        *destination_warnings,
    ]
    errors = [*file_errors, *missing_files, *engine_errors]

    if not effective_files:
        errors.append("No files were provided or available to process.")

    if (
        getattr(obj, "_set_data_product", None) is None
        or getattr(obj, "_set_table", None) is None
    ) and (select_cols is not None or date_query is not None or bvd_query is not None):
        would_prompt = True
        warnings.append(
            "The current selection state is incomplete and would normally prompt for data product/table selection."
        )

    ok = not errors
    if not ok:
        chosen_engine = "blocked"

    return PreflightReport(
        ok=ok,
        engine=chosen_engine,
        reason=reason if ok else (errors[0] if errors else reason),
        files=resolved_files or effective_files,
        missing_files=missing_files,
        warnings=warnings,
        errors=errors,
        required_columns=required_columns,
        resolved_date_column=resolved_date_column,
        resolved_bvd_columns=resolved_bvd_columns,
        destination=destination,
        would_prompt=would_prompt,
        would_download=any("would be downloaded" in warning for warning in warnings),
        would_write=would_write,
    )


def build_download_preflight(
    obj,
    *,
    files=None,
) -> PreflightReport:
    effective_files = _as_list(files if files is not None else getattr(obj, "_remote_files", []))
    resolved_files, missing_files, file_warnings, file_errors = _resolve_files(obj, effective_files)

    warnings = list(file_warnings)
    errors = list(file_errors)
    would_prompt = False

    if not effective_files:
        errors.append("No files were provided or available to download.")

    if getattr(obj, "_set_data_product", None) is None or getattr(obj, "_set_table", None) is None:
        would_prompt = True
        warnings.append(
            "Downloading from remote storage would normally prompt for data product/table selection."
        )

    ok = not errors and bool(effective_files)

    return PreflightReport(
        ok=ok,
        engine="download",
        reason=None if ok else (errors[0] if errors else "No files available for download"),
        files=resolved_files or effective_files,
        missing_files=missing_files,
        warnings=warnings,
        errors=errors,
        required_columns=None,
        resolved_date_column=None,
        resolved_bvd_columns=None,
        destination=None,
        would_prompt=would_prompt,
        would_download=any("would be downloaded" in warning for warning in warnings),
        would_write=False,
    )
