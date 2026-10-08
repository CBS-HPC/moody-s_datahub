"""Regression coverage for helper ownership, selection, and non-UI flows."""

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from moodys_datahub.extra import national_identifer
from moodys_datahub.tools import Sftp


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Unexpected SFTP connection")

    monkeypatch.setattr(Sftp, "_connect", forbidden)
    monkeypatch.setattr(Sftp, "_remote_file_sizes", lambda self, files=None: {})


def make_sftp(tmp_path):
    obj = object.__new__(Sftp)
    obj._object_defaults()
    obj._interactive = False
    obj._offline = False
    obj._local_repo = None
    obj._download_root = str(tmp_path / "cache")
    obj._output_root = None
    obj._max_path_length = 10000
    obj._pool_method = "threading"
    obj.output_format = None
    obj.concat_files = True
    obj.delete_files = False
    obj.file_size_mb = 100
    obj.allow_invalid_bvd_ids = True
    obj._table_dictionary = None
    obj._table_dates = None
    obj._tables_backup = pd.DataFrame()
    obj._tables_available = pd.DataFrame()
    return obj


def profile_sftp(tmp_path, names):
    obj = make_sftp(tmp_path)
    obj._set_data_product = "Product"
    obj._set_table = "table"
    obj._remote_path = "/exports/table"
    obj._remote_files = names
    cache = tmp_path / "cache"
    cache.mkdir()
    obj._local_path = str(cache)
    return obj, cache


def fake_downloads(monkeypatch, payloads, *, fail_on=None):
    class Remote:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, remote, local):
            Path(local).write_bytes(payloads[Path(remote).name])
            if Path(remote).name == fail_on:
                raise OSError("fixture download failed")

        def stat(self, remote):
            return SimpleNamespace(st_size=len(payloads[Path(remote).name]), st_mtime=0)

    monkeypatch.setattr(Sftp, "_connect", lambda self: Remote())


@pytest.mark.parametrize("failure", [None, "canonical", "schema"])
def test_all_files_preserves_preexisting_managed_cache(tmp_path, failure):
    obj, cache = profile_sftp(tmp_path, ["first.csv", "second.csv"])
    payloads = {
        "first.csv": b"bvd_id_number\nDK123\n",
        "second.csv": (
            b"different_column\nSE200\n"
            if failure == "schema"
            else b"bvd_id_number\nSE200\n"
        ),
    }
    for name, content in payloads.items():
        (cache / name).write_bytes(content)

    if failure:
        with pytest.raises(ValueError, match="canonical_bvd_column|Schema drift"):
            obj.profile_table(
                file_scope="all_files",
                canonical_bvd_column="missing" if failure == "canonical" else None,
                scratch_dir=tmp_path,
            )
    else:
        profile = obj.profile_table(file_scope="all_files", scratch_dir=tmp_path)
        assert profile.attrs["table_summary"]["row_count"] == 2

    assert obj.delete_files is False
    assert {name: (cache / name).read_bytes() for name in payloads} == payloads
    assert not list(tmp_path.glob(".profile-*.sqlite3*"))


@pytest.mark.parametrize(
    "failure", [None, "sample", "canonical", "scratch", "download"]
)
def test_all_files_cleans_only_newly_staged_files(tmp_path, monkeypatch, failure):
    obj, cache = profile_sftp(tmp_path, ["first.csv"])
    obj._download_retries = 1
    payload = b"not parquet" if failure == "sample" else b"bvd_id_number\nDK123\n"
    name = "first.parquet" if failure == "sample" else "first.csv"
    obj._remote_files = [name]
    fake_downloads(
        monkeypatch, {name: payload}, fail_on=name if failure == "download" else None
    )
    scratch = tmp_path / "scratch"
    if failure == "scratch":
        scratch.write_text("not a directory", encoding="ascii")

    kwargs = {
        "file_scope": "all_files",
        "scratch_dir": scratch,
        "canonical_bvd_column": "missing" if failure == "canonical" else None,
    }
    if failure:
        with pytest.raises((ValueError, OSError)):
            obj.profile_table(**kwargs)
    else:
        assert obj.profile_table(**kwargs).attrs["table_summary"]["row_count"] == 1

    assert not (cache / name).exists()
    assert not list(tmp_path.rglob(".profile-*.sqlite3*"))


@pytest.mark.parametrize("strict", [True, False])
@pytest.mark.parametrize("first_empty", [True, False])
def test_all_files_checks_schema_of_empty_parquet_shards(tmp_path, strict, first_empty):
    obj, cache = profile_sftp(tmp_path, ["first.parquet", "empty.parquet"])
    pd.DataFrame({"bvd_id_number": [] if first_empty else ["DK123"]}).to_parquet(
        cache / "first.parquet", index=False
    )
    pd.DataFrame({"different_column": pd.Series(dtype="string")}).to_parquet(
        cache / "empty.parquet", index=False
    )
    if strict:
        with pytest.raises(ValueError, match="Schema drift"):
            obj.profile_table(file_scope="all_files", scratch_dir=tmp_path)
    else:
        summary = obj.profile_table(
            file_scope="all_files", strict_schema=False, scratch_dir=tmp_path
        ).attrs["table_summary"]
        assert summary["schema_drift_files"] == 1
        assert summary["row_count"] == (0 if first_empty else 1)
        assert summary["scanned_file_count"] == 2

    assert (cache / "first.parquet").exists()
    assert (cache / "empty.parquet").exists()
    assert not list(tmp_path.glob(".profile-*.sqlite3*"))


def selection_sftp(tmp_path, selections, frame):
    obj = make_sftp(tmp_path)
    obj._local_repo = str(tmp_path)
    inventory = []
    dictionary = []
    for product, table in selections:
        directory = tmp_path / product / table
        directory.mkdir(parents=True)
        frame.to_parquet(directory / "part.parquet", index=False)
        inventory.append(
            {
                "Data Product": product,
                "Table": table,
                "Base Directory": str(directory),
                "Export": str(directory.parent),
                "Timestamp": None,
                "Top-level Directory": product,
            }
        )
        dictionary.extend(
            {
                "Data Product": product,
                "Table": table,
                "Column": column,
                "Definition": column,
            }
            for column in frame.columns
        )
    obj._tables_backup = pd.DataFrame(inventory)
    obj._tables_available = obj._tables_backup.copy()
    obj._table_dictionary = pd.DataFrame(dictionary)
    return obj


@pytest.mark.parametrize("engine", ["pandas", "polars"])
def test_batch_bvd_filters_survive_real_selection_setters(
    tmp_path, monkeypatch, engine
):
    monkeypatch.chdir(tmp_path)
    frame = pd.DataFrame(
        {
            "bvd_id_number": ["DK123", "DK123", "SE200", "US999"],
            "owner_id": ["DK900", "NO300", "NO300", "DK900"],
            "previous_id": ["FR400", "FR400", "SE888", "FR400"],
            "marker": ["base_and", "base_only", "or_only", "neither"],
        }
    )
    selections = [
        ("Product A", "first"),
        ("Product A", "second"),
        ("Product B", "third"),
    ]
    obj = selection_sftp(tmp_path, selections, frame)
    workbook = tmp_path / "batch.xlsx"
    pd.DataFrame(
        [
            {
                "Data Product": product,
                "Table": table,
                "Column": "bvd_id_number",
                "Run": True,
            }
            for product, table in selections
        ]
    ).to_excel(workbook, index=False)
    ids = tmp_path / "ids.txt"
    ids.write_text("DK123\n", encoding="ascii")
    results = []
    process_all = Sftp.process_all

    def run(self, **kwargs):
        result, _ = process_all(
            self,
            files=[str(Path(self.remote_path) / self.remote_files[0])],
            num_workers=1,
            engine=engine,
            **kwargs,
        )
        results.append((self.set_data_product, self.set_table, result))
        return result, []

    monkeypatch.setattr(Sftp, "process_all", run)
    obj.batch_bvd_search(
        products=str(workbook),
        bvd_numbers=str(ids),
        AND_bvd_list=[[["DK900"], "owner_id", "exact"]],
        OR_bvd_list=[[["SE888"], "previous_id", "exact"]],
    )

    assert [(product, table) for product, table, _ in results] == selections
    for _, _, result in results:
        assert result["marker"].tolist() == ["base_and", "or_only"]
    assert obj.set_data_product is None
    assert obj.AND_bvd_list == []
    assert obj.OR_bvd_list == []


@pytest.mark.parametrize("missing", ["products", "ids", "both"])
def test_missing_batch_templates_use_requested_paths_without_overwriting_defaults(
    tmp_path, monkeypatch, missing
):
    monkeypatch.chdir(tmp_path)
    templates = tmp_path / "templates"
    templates.mkdir()
    (templates / "products.xlsx").write_bytes(b"product template")
    (templates / "bvd_numbers.txt").write_bytes(b"ID template")
    monkeypatch.setattr("moodys_datahub.tools.pkg_resources.files", lambda _: templates)
    defaults = {
        "products.xlsx": b"unrelated workbook",
        "bvd_numbers.txt": b"unrelated IDs",
    }
    for name, content in defaults.items():
        (tmp_path / name).write_bytes(content)
    requested = tmp_path / "custom"
    requested.mkdir()
    workbook = requested / "requested.xlsx"
    ids = requested / "requested.txt"
    if missing == "ids":
        workbook.write_bytes(b"existing requested workbook")
    if missing == "products":
        ids.write_bytes(b"existing requested IDs")

    Sftp.batch_bvd_search(object(), products=str(workbook), bvd_numbers=str(ids))

    assert workbook.read_bytes() == (
        b"existing requested workbook" if missing == "ids" else b"product template"
    )
    assert ids.read_bytes() == (
        b"existing requested IDs" if missing == "products" else b"ID template"
    )
    assert {name: (tmp_path / name).read_bytes() for name in defaults} == defaults


@pytest.mark.parametrize("privatekey", [None, "unused-key.pem"])
@pytest.mark.parametrize("invalid", ["missing", "file"])
@pytest.mark.parametrize("offline", [True, False])
def test_invalid_local_repo_raises_before_connection_or_discovery(
    tmp_path, monkeypatch, privatekey, invalid, offline
):
    local_repo = tmp_path / "invalid-repository"
    if invalid == "file":
        local_repo.write_text("not a directory", encoding="ascii")

    def forbidden(*args, **kwargs):
        pytest.fail("Invalid local_repo must be rejected before discovery")

    monkeypatch.setattr(Sftp, "tables_available", forbidden)
    with pytest.raises(ValueError, match="local_repo.*directory"):
        Sftp(
            local_repo=str(local_repo),
            privatekey=privatekey,
            offline=offline,
            interactive=False,
        )


@pytest.mark.parametrize("interactive", [True, False])
def test_national_identifier_clones_without_ui_or_mutating_original(
    tmp_path, monkeypatch, interactive
):
    frame = pd.DataFrame(
        {
            "bvd_id_number": ["DK123", "SE200"],
            "national_id_number": ["123", "456"],
        }
    )
    obj = selection_sftp(
        tmp_path,
        [("Key Financials (Monthly)", "key_financials_eur"), ("Original", "original")],
        frame,
    )
    obj._interactive = interactive
    obj.set_data_product = "Original"
    obj.set_table = "original"
    obj.AND_bvd_list = [[["SE200"], "bvd_id_number", "exact"]]
    process_all = Sftp.process_all

    def run(self, **kwargs):
        return process_all(
            self,
            files=[str(Path(self.remote_path) / self.remote_files[0])],
            engine="polars",
            **kwargs,
        )

    def forbidden(*args, **kwargs):
        pytest.fail("national_identifer must not launch selection UI")

    monkeypatch.setattr(Sftp, "process_all", run)
    monkeypatch.setattr(Sftp, "select_data", forbidden)
    result = national_identifer(obj, national_ids=[123], num_workers=1)

    assert result["bvd_id_number"].tolist() == ["DK123"]
    assert result["national_id_number"].tolist() == ["123"]
    assert obj.set_data_product == "Original"
    assert obj.set_table == "original"
    assert obj.AND_bvd_list[0]["values"] == ["SE200"]


def test_public_copy_obj_keeps_interactive_selection_contract(tmp_path, monkeypatch):
    obj = make_sftp(tmp_path)
    obj._interactive = True
    obj._set_data_product = "Original"
    obj._set_table = "original"
    selected = []
    monkeypatch.setattr(Sftp, "select_data", lambda self: selected.append(self))

    cloned = obj.copy_obj()

    assert cloned is not obj
    assert selected == [cloned]
    assert cloned.set_data_product is None
    assert cloned.set_table is None
    assert obj.set_data_product == "Original"


@pytest.mark.parametrize("interactive", [True, False])
def test_profile_dry_run_resolves_inventory_without_setters_or_network(
    tmp_path, monkeypatch, interactive
):
    obj = make_sftp(tmp_path)
    obj._interactive = interactive
    obj._tables_backup = pd.DataFrame(
        {
            "Data Product": ["Remote Product"],
            "Table": ["remote_table"],
            "Base Directory": ["/exports/remote_table"],
            "Export": ["/exports"],
            "Timestamp": ["2025-01-02 03:04:05"],
        }
    )

    def forbidden(*args, **kwargs):
        pytest.fail("Dry-run must not select, deep-copy, stage, or read sources")

    for name in (
        "select_data",
        "_check_path",
        "_get_file",
        "_check_args",
        "_read_profile_file",
    ):
        monkeypatch.setattr(Sftp, name, forbidden)
    monkeypatch.setattr("moodys_datahub.tools.copy.deepcopy", forbidden)
    report_path = tmp_path / "report.xlsx"

    plan = obj.profile_table(
        data_product="Remote Product",
        table="remote_table",
        file_scope="all_files",
        dry_run=True,
        report_path=str(report_path),
    )

    assert plan.loc[0, "data_product"] == "Remote Product"
    assert plan.loc[0, "table"] == "remote_table"
    assert plan.loc[0, "would_list_remote"]
    assert pd.isna(plan.loc[0, "source_file_count"])
    assert pd.isna(plan.loc[0, "would_download"])
    assert plan.loc[0, "would_write_report"]
    assert obj.set_data_product is None
    assert obj.set_table is None
    assert not (tmp_path / "cache").exists()
    assert not report_path.exists()


@pytest.mark.parametrize("file_scope", ["first_file", "all_files"])
def test_profile_dry_run_download_plan_uses_physical_cache_presence(
    tmp_path, file_scope
):
    obj, cache = profile_sftp(tmp_path, ["first.csv", "missing.csv"])
    first = cache / "first.csv"
    first.write_text("bvd_id_number\nDK123\n", encoding="ascii")
    original = first.read_bytes()

    plan = obj.profile_table(file_scope=file_scope, dry_run=True)

    assert bool(plan.loc[0, "would_download"]) == (file_scope == "all_files")
    assert plan.loc[0, "source_file_count"] == (2 if file_scope == "all_files" else 1)
    assert first.read_bytes() == original
    assert obj.remote_files == ["first.csv", "missing.csv"]
    assert not (cache / "missing.csv").exists()


def test_all_files_cleans_partial_staging_if_resolver_raises(tmp_path, monkeypatch):
    obj, cache = profile_sftp(tmp_path, ["first.csv", "second.csv", "untouched.csv"])
    original = b"bvd_id_number\nDK123\n"
    (cache / "first.csv").write_bytes(original)
    (cache / "untouched.csv").write_bytes(original)
    get_file = Sftp._get_file

    def interrupted(self, file):
        if file == "second.csv":
            Path(self.resolve_cache_file(file)).write_bytes(b"partial download")
            raise OSError("fixture staging failed")
        return get_file(self, file)

    monkeypatch.setattr(Sftp, "_get_file", interrupted)
    with pytest.raises(OSError, match="fixture staging failed"):
        obj.profile_table(file_scope="all_files", scratch_dir=tmp_path)

    assert (cache / "first.csv").read_bytes() == original
    assert (cache / "untouched.csv").read_bytes() == original
    assert not (cache / "second.csv").exists()
    assert not list(tmp_path.glob(".profile-*.sqlite3*"))


def test_all_files_cleans_staging_and_scratch_if_sqlite_open_fails(
    tmp_path, monkeypatch
):
    obj, cache = profile_sftp(tmp_path, ["first.csv"])
    fake_downloads(monkeypatch, {"first.csv": b"bvd_id_number\nDK123\n"})

    def interrupted(path):
        Path(path).write_bytes(b"partial database")
        raise OSError("fixture scratch failed")

    monkeypatch.setattr("moodys_datahub.profile_scan.sqlite3.connect", interrupted)
    with pytest.raises(OSError, match="fixture scratch failed"):
        obj.profile_table(file_scope="all_files", scratch_dir=tmp_path)

    assert not (cache / "first.csv").exists()
    assert not list(tmp_path.glob(".profile-*.sqlite3*"))


def test_all_files_cleans_staged_empty_parquet_on_schema_drift(tmp_path, monkeypatch):
    obj, cache = profile_sftp(tmp_path, ["first.parquet", "empty.parquet"])
    source = tmp_path / "source"
    source.mkdir()
    pd.DataFrame({"bvd_id_number": ["DK123"]}).to_parquet(source / "first.parquet")
    pd.DataFrame({"different_column": pd.Series(dtype="string")}).to_parquet(
        source / "empty.parquet"
    )
    fake_downloads(
        monkeypatch,
        {name: (source / name).read_bytes() for name in obj.remote_files},
    )

    with pytest.raises(ValueError, match="Schema drift"):
        obj.profile_table(file_scope="all_files", scratch_dir=tmp_path)

    assert not list(cache.iterdir())
    assert not list(tmp_path.glob(".profile-*.sqlite3*"))


def test_profile_dry_run_can_plan_local_repo_inventory(tmp_path):
    obj = selection_sftp(tmp_path, [("Product", "table")], pd.DataFrame({"value": [1]}))
    plan = obj.profile_table(data_product="Product", table="table", dry_run=True)

    assert plan.loc[0, "file_name"] == "part.parquet"
    assert plan.loc[0, "source_file_count"] == 1
    assert not plan.loc[0, "would_download"]
    assert not plan.loc[0, "would_list_remote"]
    assert obj.set_data_product is None
    assert obj.set_table is None


@pytest.mark.parametrize("invalid", ["missing", "ambiguous"])
def test_profile_dry_run_rejects_unresolved_cached_inventory(tmp_path, invalid):
    obj = make_sftp(tmp_path)
    obj._tables_backup = pd.DataFrame(
        [
            {"Data Product": "Product", "Table": "table", "Base Directory": path}
            for path in ("/export/first", "/export/second")
        ]
    )
    with pytest.raises(ValueError, match="unambiguous cached inventory"):
        obj.profile_table(
            data_product="Product",
            table="missing" if invalid == "missing" else "table",
            dry_run=True,
        )
    assert obj.set_data_product is None


def test_profile_dry_run_rejects_unknown_file_without_staging(tmp_path):
    obj, cache = profile_sftp(tmp_path, ["known.csv"])
    with pytest.raises(ValueError, match="unknown.csv"):
        obj.profile_table(file="unknown.csv", dry_run=True)
    assert not list(cache.iterdir())


def test_profile_dry_run_does_not_parse_explicit_local_source(tmp_path):
    obj, _ = profile_sftp(tmp_path, ["known.parquet"])
    source = tmp_path / "source.parquet"
    source.write_bytes(b"no schema or data should be read")

    plan = obj.profile_table(file=str(source), dry_run=True)

    assert plan.loc[0, "file_name"] == source.name
    assert not plan.loc[0, "would_download"]
    assert not plan.loc[0, "would_list_remote"]
    assert source.read_bytes() == b"no schema or data should be read"
