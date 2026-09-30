"""Country-aware candidate selection for company-name matching."""

from collections import defaultdict

import pandas as pd
import polars as pl

from .load_data import _country_codes
from .utils import CompanyNameFuzzyMatcher


def resolve_country_filters(names, country=None, countries_by_name=None):
    """Resolve country names/codes to one code per input name."""
    if country is None and countries_by_name is None:
        return None, set()
    if country is not None and countries_by_name is not None:
        raise ValueError("Use either country or countries_by_name, not both.")

    catalog = _country_codes()
    code_lookup = {}
    known_codes = set()
    for country_name, code in catalog[["Country", "Code"]].itertuples(
        index=False, name=None
    ):
        code = str(code).strip().upper()
        if len(code) != 2 or not code.isalpha():
            continue
        known_codes.add(code)
        code_lookup[code.casefold()] = code
        code_lookup[str(country_name).strip().casefold()] = code
    if not known_codes:
        raise ValueError("No country codes are available for company-name matching.")

    if country is not None:
        requested = [country] * len(names)
    else:
        if isinstance(countries_by_name, (str, bytes)):
            raise ValueError("countries_by_name must align with the names list.")
        try:
            requested = list(countries_by_name)
        except TypeError as exc:
            raise ValueError(
                "countries_by_name must align with the names list."
            ) from exc
        if len(requested) != len(names):
            raise ValueError("countries_by_name must have one value per name.")

    resolved = []
    for value in requested:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                "Every requested country must be a name or two-letter code."
            )
        code = code_lookup.get(value.strip().casefold())
        if code is None:
            raise ValueError(f"Unknown country for company-name matching: {value!r}")
        resolved.append(code)
    return resolved, known_codes


def country_filter_expr(allowed_codes, known_codes):
    """Exclude IDs beginning with another recognised two-letter country code."""
    excluded = sorted(set(known_codes) - set(allowed_codes))
    prefix = (
        pl.col("bvd_id_number")
        .cast(pl.Utf8, strict=False)
        .str.slice(0, 2)
        .str.to_uppercase()
    )
    return ~(prefix.is_in(excluded).fill_null(False))


def _country_subset(frame, code, known_codes):
    if isinstance(frame, pl.DataFrame):
        return frame.filter(country_filter_expr({code}, known_codes)).to_pandas()
    excluded = set(known_codes) - {code}
    prefix = frame["bvd_id_number"].astype("string").str.slice(0, 2).str.upper()
    return frame.loc[~prefix.isin(excluded)]


def match_country_candidates(
    frame,
    names,
    country_codes,
    known_codes,
    cut_off,
    company_suffixes,
    scorer,
    num_workers=1,
):
    """Match each input against its country subset and preserve input positions."""
    grouped_inputs = defaultdict(list)
    for index, (name, code) in enumerate(zip(names, country_codes)):
        grouped_inputs[code].append((index, name))

    results = []
    for code, indexed_names in grouped_inputs.items():
        source = _country_subset(frame, code, known_codes)
        matcher = CompanyNameFuzzyMatcher(
            source,
            match_column="name",
            return_column="bvd_id_number",
            remove_str=company_suffixes,
            scorer=scorer,
        )
        unique_names = {}
        input_rows = []
        for index, name in indexed_names:
            normalized = matcher.normalize_company_name(name)
            if normalized is None:
                raise ValueError(f"Company name at position {index} is empty.")
            unique_names.setdefault(normalized, name)
            input_rows.append((normalized, index, name))

        matches = matcher.search(
            names=list(unique_names.values()),
            cut_off=cut_off,
            num_workers=num_workers,
        )
        input_frame = pd.DataFrame(
            input_rows, columns=["Search_string", "Input_index", "Input_name"]
        )
        matched = matches.merge(input_frame, on="Search_string", how="inner")
        matched["Requested_country"] = code

        identifiers = matched["bvd_id_number"].astype("string")
        prefixes = identifiers.str.slice(0, 2).str.upper()
        status = pd.Series("unrecognized_prefix", index=matched.index)
        status.loc[
            identifiers.isna() | identifiers.str.strip().eq("").fillna(False)
        ] = "missing_id"
        status.loc[prefixes.eq(code).fillna(False)] = "requested_prefix"
        matched["BvD_prefix_status"] = status
        results.append(matched)

    if not results:
        return pd.DataFrame(
            columns=[
                "Search_string",
                "BestMatch",
                "Score",
                "name",
                "bvd_id_number",
                "Input_index",
                "Input_name",
                "Requested_country",
                "BvD_prefix_status",
            ]
        )
    return pd.concat(results, ignore_index=True)


def country_match_query(
    frame, names, country_codes, known_codes, cut_off, company_suffixes, scorer
):
    """Pandas per-file query used when the Polars loading path fails."""
    return match_country_candidates(
        frame,
        names,
        country_codes,
        known_codes,
        cut_off,
        company_suffixes,
        scorer,
        num_workers=1,
    )
