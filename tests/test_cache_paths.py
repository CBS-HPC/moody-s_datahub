from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from moodys_datahub import Sftp
from moodys_datahub.preflight import _candidate_local_path


@pytest.fixture
def client(tmp_path, monkeypatch):
    obj = Sftp(offline=True, interactive=False, download_root=tmp_path / "cache")
    obj.output_format = None
    obj._max_path_length = 10_000

    def unexpected(*args, **kwargs):
        pytest.fail("Cache resolution must not connect, prompt, or spawn workers")

    monkeypatch.setattr(obj, "_connect", unexpected)
    monkeypatch.setattr(obj, "select_data", unexpected)
    monkeypatch.setattr("moodys_datahub.process._run_parallel", unexpected)
    return obj


def select_synthetic_table(client):
    client._set_data_product = "Synthetic Product"
    client._set_table = "synthetic_table"
    client._remote_path = "/synthetic/export/synthetic_table"
    client._time_stamp = "2026-10-08 00:00:00"
    client._remote_files = ["partition.csv"]


def test_early_lookup_agrees_with_read_only_preflight(client):
    select_synthetic_table(client)
    expected = (
        Path(client._download_root)
        / "Synthetic Product_exported 2026-10-08_00-00-00"
        / "synthetic_table"
        / "partition.csv"
    )
    state = client.__dict__.copy()

    for _ in range(2):
        for method in (client.download_all, client.process_all):
            report = method(files=["partition.csv"], dry_run=True)
            assert report.ok
            assert report.files == [str(expected)]
            assert report.would_download
        assert client._file_exist("partition.csv") == (str(expected), False)
        assert client.resolve_cache_file("partition.csv") == str(expected)

    assert client.__dict__ == state
    assert not Path(client._download_root).exists()


def test_early_lookup_preserves_existing_cache_contents_and_flag(client):
    select_synthetic_table(client)
    cache = Path(_candidate_local_path(client))
    cache.mkdir(parents=True)
    source = cache / "partition.csv"
    source.write_bytes(b"review this suspicious cache file")
    before = source.stat()

    resolved, supplied_local_file = client._file_exist("partition.csv")

    assert resolved == str(source)
    # This flag protects directly supplied local files during processing;
    # discovering a managed cache file must not change its meaning.
    assert supplied_local_file is False
    assert Path(client.resolve_cache_file("partition.csv")).exists()
    assert source.read_bytes() == b"review this suspicious cache file"
    assert source.stat().st_mtime_ns == before.st_mtime_ns
    assert client.local_path is None


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("selected", [False, True])
def test_explicit_cache_path_takes_precedence(client, tmp_path, existing, selected):
    if selected:
        select_synthetic_table(client)
    cache = tmp_path / "explicit-cache"
    client.local_path = str(cache)
    source = cache / "partition.csv"
    if existing:
        source.write_bytes(b"original")

    assert client.resolve_cache_file("partition.csv") == str(source)
    assert client._file_exist("partition.csv") == (str(source), False)
    assert source.exists() is existing
    assert not Path(client._download_root).exists()


@pytest.mark.parametrize("relative", [False, True])
def test_existing_explicit_file_needs_no_selection(
    client, tmp_path, monkeypatch, relative
):
    source = tmp_path / "source.csv"
    source.write_bytes(b"value\n1\n")
    client.local_path = str(tmp_path / "other-cache")
    monkeypatch.chdir(tmp_path)
    supplied = "source.csv" if relative else str(source)

    assert client._file_exist(supplied) == (str(source), True)
    assert client.resolve_cache_file(supplied) == str(source)
    assert source.read_bytes() == b"value\n1\n"


@pytest.mark.parametrize("missing", ["_remote_path", "_set_data_product", "_set_table"])
def test_incomplete_selection_reports_missing_configuration(client, missing):
    select_synthetic_table(client)
    setattr(client, missing, None)

    for lookup in (client._file_exist, client.resolve_cache_file):
        with pytest.raises(ValueError, match=missing.removeprefix("_")) as error:
            lookup("partition.csv")
        assert "local_path" in str(error.value)
    assert not Path(client._download_root).exists()


@pytest.mark.parametrize("local_path", [None, ""])
def test_fresh_object_requires_cache_path_or_selection(client, local_path):
    client._local_path = local_path
    with pytest.raises(ValueError, match="local_path") as error:
        client._file_exist("partition.csv")

    for field in ("remote_path", "set_data_product", "set_table"):
        assert field in str(error.value)
    assert not Path(client._download_root).exists()


@pytest.mark.parametrize("custom_root", [False, True])
@pytest.mark.parametrize("timestamp", [None, "2026-10-08 00:00:00"])
def test_lookup_and_runtime_download_share_path_policy(
    client, tmp_path, monkeypatch, custom_root, timestamp
):
    select_synthetic_table(client)
    monkeypatch.chdir(tmp_path)
    client._time_stamp = timestamp
    if not custom_root:
        client._download_root = None
    root = Path(client._download_root or "Data Products").absolute()
    product = "Synthetic Product"
    if timestamp:
        product += "_exported 2026-10-08_00-00-00"
    expected = root / product / "synthetic_table" / "partition.csv"

    report = client.download_all(files=["partition.csv"], dry_run=True)
    assert report.ok
    assert Path(report.files[0]).absolute() == expected
    assert client.resolve_cache_file("partition.csv") == str(expected)
    assert not root.exists()

    def fake_transfer(**kwargs):
        assert client._file_exist("partition.csv")[0] == str(expected)
        assert expected.parent.is_dir()
        expected.write_bytes(b"value\n1\n")

    monkeypatch.setattr("moodys_datahub.process.os.fork", lambda: None, raising=False)
    monkeypatch.setattr(client, "_remote_file_sizes", lambda files: {})
    monkeypatch.setattr("moodys_datahub.process._run_parallel", fake_transfer)
    client.download_all(files=["partition.csv"], num_workers=1, async_mode=False)

    assert Path(client.local_path).absolute() == expected.parent
    assert client.download_finished is True
    assert expected.read_bytes() == b"value\n1\n"


def test_public_selection_switches_table_and_export_cache(client, monkeypatch):
    inventory = pd.DataFrame(
        [
            {
                "Data Product": "Synthetic Product",
                "Table": table,
                "Export": f"/export/{version}",
                "Base Directory": f"/export/{version}/{table}",
                "Top-level Directory": "/export",
                "Timestamp": timestamp,
            }
            for table, version, timestamp in (
                ("table_old", "old", "2025-01-01 00:00:00"),
                ("table_new", "new", "2026-10-08 00:00:00"),
            )
        ]
    )
    client._tables_backup = inventory.copy()
    client._tables_available = inventory.copy()

    class FakeSftp:
        def exists(self, path):
            return path in inventory["Base Directory"].values

        def listdir(self, path):
            assert path in inventory["Base Directory"].values
            return ["partition.csv"]

        def stat(self, path):
            pytest.fail("Known export timestamps must not need a file stat")

        def get(self, *args):
            pytest.fail("Selection/cache lookup must not transfer files")

    monkeypatch.setattr(client, "_connect", lambda: FakeSftp())
    paths = []
    for table, timestamp in (
        ("table_old", "2025-01-01_00-00-00"),
        ("table_new", "2026-10-08_00-00-00"),
    ):
        client.set_table = table
        assert client.local_path is None
        paths.append(client.resolve_cache_file("partition.csv"))
        assert Path(paths[-1]) == (
            Path(client._download_root)
            / f"Synthetic Product_exported {timestamp}"
            / table
            / "partition.csv"
        )
        assert client.download_all(dry_run=True).files == [paths[-1]]
        # Runtime initialization for the old table must be invalidated when
        # the next table is selected through the public setter.
        client._check_args(["partition.csv"])
        assert Path(client.local_path) == Path(paths[-1]).parent

    assert paths[0] != paths[1]


def test_remote_path_switch_uses_new_export_timestamp(client, monkeypatch):
    inventory = pd.DataFrame(
        [
            {
                "Data Product": "Synthetic Product",
                "Table": "synthetic_table",
                "Export": f"/export/{version}",
                "Base Directory": f"/export/{version}/synthetic_table",
                "Timestamp": timestamp,
            }
            for version, timestamp in (
                ("old", "2025-01-01 00:00:00"),
                ("new", "2026-10-08 00:00:00"),
            )
        ]
    )
    client._tables_backup = inventory.copy()
    # Real workflows may have narrowed the active inventory to the old export.
    client._tables_available = inventory.iloc[:1].copy()
    client._set_data_product = "Synthetic Product"
    client._set_table = "synthetic_table"
    client._time_stamp = "2025-01-01 00:00:00"
    client._remote_path = "/export/old/synthetic_table"
    old_cache = Path(_candidate_local_path(client))
    old_cache.mkdir(parents=True)
    old_source = old_cache / "partition.csv"
    old_source.write_bytes(b"old version")
    client.local_path = str(old_cache)

    class FakeSftp:
        def exists(self, path):
            return path in inventory["Base Directory"].values

        def listdir(self, path):
            return ["partition.csv"]

        def stat(self, path):
            pytest.fail("Known export timestamps must not need a file stat")

        def get(self, *args):
            pytest.fail("Changing export and resolving a path must not download")

    monkeypatch.setattr(client, "_connect", lambda: FakeSftp())
    client.remote_path = "/export/new/synthetic_table"

    expected = (
        Path(client._download_root)
        / "Synthetic Product_exported 2026-10-08_00-00-00"
        / "synthetic_table"
        / "partition.csv"
    )
    assert client.local_path is None
    assert client._time_stamp == "2026-10-08 00:00:00"
    assert client._file_exist("partition.csv") == (str(expected), False)
    assert client.resolve_cache_file("partition.csv") == str(expected)
    assert client.download_all(dry_run=True).files == [str(expected)]
    assert not expected.parent.exists()
    assert old_source.read_bytes() == b"old version"


def test_lookup_preserves_path_length_guard(client):
    select_synthetic_table(client)
    client._max_path_length = 1

    with pytest.raises(ValueError, match="max path length"):
        client._file_exist("partition.csv")
    assert not Path(client._download_root).exists()


def test_required_candidate_path_is_read_only_with_explicit_cache(tmp_path):
    obj = SimpleNamespace(_local_path=tmp_path / "uncreated")

    assert _candidate_local_path(obj, required=True) == str(obj._local_path)
    assert not obj._local_path.exists()
