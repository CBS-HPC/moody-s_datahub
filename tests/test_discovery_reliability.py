import asyncio
import json
import posixpath
import warnings
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from moodys_datahub import Sftp
from moodys_datahub.load_data import _table_match

CATALOG_COLUMNS = [
    "Data Product",
    "Table",
    "Base Directory",
    "Timestamp",
    "Export",
    "Top-level Directory",
]


class FakeSftp:
    def __init__(self):
        self.directories = {
            ".": ["product"],
            "product": ["tnfs", "old_export", "export"],
            "product/tnfs": ["old.tnf", "new.tnf"],
            "product/old_export": ["main"],
            "product/old_export/main": ["old.csv"],
            "product/export": ["main", "history"],
            "product/export/main": ["part-1.csv", "part-2.csv", "main.parquet"],
            "product/export/history": ["history.csv"],
        }
        self.contents = {
            "product/tnfs/old.tnf": json.dumps({"DataFolder": "old_export"}),
            "product/tnfs/new.tnf": json.dumps({"DataFolder": "export"}),
        }
        self.mtimes = {"product/tnfs/old.tnf": 100, "product/tnfs/new.tnf": 200}
        self.downloads = []
        self.removed = []
        self.download_error = None
        self.listing_error = None
        self.connections = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    @staticmethod
    def _path(path):
        path = "." if path is None else str(path)
        assert "\\" not in path, f"Remote path is not POSIX: {path}"
        return posixpath.normpath(path)

    def listdir(self, path=None):
        if self.listing_error is not None:
            raise self.listing_error
        return list(self.directories[self._path(path)])

    def exists(self, path):
        path = self._path(path)
        return path in self.directories or path in self.contents

    def stat(self, path):
        path = self._path(path)
        return SimpleNamespace(st_mtime=self.mtimes.get(path, 200))

    def get(self, remote_path, local_path):
        remote_path = self._path(remote_path)
        local_path = Path(local_path)
        self.downloads.append((remote_path, local_path))
        local_path.write_text(self.contents[remote_path], encoding="utf-8")
        if self.download_error is not None:
            raise self.download_error

    def remove(self, path):
        self.removed.append(self._path(path))


@pytest.fixture
def server(monkeypatch, tmp_path):
    server = FakeSftp()

    def connect(**kwargs):
        server.connections += 1
        return server

    monkeypatch.setattr("moodys_datahub.connection.pysftp.Connection", connect)
    monkeypatch.chdir(tmp_path)
    return server


def remote_client(
    tmp_path,
    server,
    product_names=None,
    interactive=False,
    catalog_roots=None,
    **kwargs,
):
    template = tmp_path / "products.csv"
    product_names = product_names or {}
    roots = server.directories["."] if catalog_roots is None else catalog_roots
    pd.DataFrame(
        {
            "Data Product": [
                product_names.get(root, "Synthetic Product") for root in roots
            ],
            "Top-level Directory": roots,
        }
    ).to_csv(template, index=False)
    return Sftp(
        hostname="fake.invalid",
        username="test",
        privatekey="fake-key",
        data_product_template=str(template),
        interactive=interactive,
        server_cleanup=False,
        **kwargs,
    )


@pytest.fixture
def unresolved_server(server, monkeypatch):
    server.directories["."].append("unmapped")
    server.directories.update(
        {
            "unmapped": ["export"],
            "unmapped/export": ["mystery"],
            "unmapped/export/mystery": ["part.csv"],
        }
    )
    monkeypatch.setattr(
        "moodys_datahub.connection._table_match",
        lambda tables: ("Multiple_Options: ['Candidate Product']", tables),
    )
    return server


@pytest.mark.parametrize("candidates", [[], ["Candidate Product"], ["First", "Second"]])
def test_unknown_export_does_not_block_noninteractive_construction(
    unresolved_server, tmp_path, monkeypatch, candidates
):
    marker = f"Multiple_Options: {candidates!r}" if candidates else "Unknown"
    monkeypatch.setattr(
        "moodys_datahub.connection._table_match", lambda tables: (marker, tables)
    )

    with pytest.warns(RuntimeWarning, match="unresolved") as emitted:
        client = remote_client(tmp_path, unresolved_server, catalog_roots=["product"])

    assert len(emitted) == 1
    assert client.unresolved_exports == [
        {
            "top_level_directory": "unmapped",
            "candidate_products": candidates,
            "tables": ["mystery"],
        }
    ]
    inventory, _ = client.tables_available()
    assert set(inventory["Top-level Directory"]) == {"product"}
    assert set(client._tables_backup["Top-level Directory"]) == {"product"}


def test_unresolved_export_does_not_block_a_known_product_with_same_candidate(
    unresolved_server, tmp_path
):
    with pytest.warns(RuntimeWarning, match="unresolved"):
        client = remote_client(
            tmp_path,
            unresolved_server,
            product_names={"product": "Candidate Product"},
            catalog_roots=["product"],
        )

    client.set_data_product = "Candidate Product"
    client.set_table = "history"
    assert client.remote_path == "product/export/history"
    assert client.remote_files == ["history.csv"]


def test_superseded_ownership_tables_are_not_accepted_as_detailed_ownership(
    unresolved_server, tmp_path, monkeypatch
):
    unresolved_server.directories["unmapped/export"] = ["entities", "links_current"]
    monkeypatch.setattr("moodys_datahub.connection._table_match", _table_match)

    with pytest.warns(RuntimeWarning, match="unresolved"):
        client = remote_client(tmp_path, unresolved_server, catalog_roots=["product"])

    assert client.unresolved_exports[0]["candidate_products"] == [
        "Detailed Ownership (Monthly)"
    ]
    assert client.unresolved_exports[0]["tables"] == ["entities", "links_current"]
    with pytest.raises(ValueError, match="unresolved"):
        client.set_data_product = "Detailed Ownership (Monthly)"
    client.set_data_product = "Synthetic Product"
    assert client.set_data_product == "Synthetic Product"


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("set_data_product", "Candidate Product"),
        ("set_table", "mystery"),
        ("remote_path", "unmapped/export"),
        ("remote_path", "unmapped/export/mystery"),
        ("remote_path", r"\unmapped\export\mystery"),
    ],
)
def test_unresolved_export_selection_raises_without_changing_valid_selection(
    unresolved_server, tmp_path, attribute, value
):
    with pytest.warns(RuntimeWarning, match="unresolved"):
        client = remote_client(tmp_path, unresolved_server, catalog_roots=["product"])
    client.set_data_product = "Synthetic Product"
    client.set_table = "history"
    previous = (client.set_data_product, client.set_table, client.remote_path)
    previous_connections = unresolved_server.connections

    with pytest.raises(ValueError, match="unresolved.*data_product_template"):
        setattr(client, attribute, value)

    assert (client.set_data_product, client.set_table, client.remote_path) == previous
    assert unresolved_server.connections == previous_connections


def test_unresolved_export_report_survives_cached_inventory_calls_and_is_a_copy(
    unresolved_server, tmp_path
):
    with pytest.warns(RuntimeWarning, match="unresolved"):
        client = remote_client(tmp_path, unresolved_server, catalog_roots=["product"])
    snapshot = client.unresolved_exports
    assert json.loads(json.dumps(snapshot)) == snapshot
    client.unresolved_exports[0]["candidate_products"].append("Not Verified")
    client.unresolved_exports[0]["tables"].clear()

    with warnings.catch_warnings(record=True) as emitted:
        warnings.simplefilter("always")
        for reset in (False, True, False):
            inventory, _ = client.tables_available(reset=reset)
            assert set(inventory["Top-level Directory"]) == {"product"}
    assert emitted == []
    assert client.unresolved_exports == snapshot


def test_multiple_unresolved_exports_emit_one_warning(unresolved_server, tmp_path):
    unresolved_server.directories["."].append("unmapped_second")
    unresolved_server.directories.update(
        {"unmapped_second": ["export"], "unmapped_second/export": ["another_table"]}
    )
    with pytest.warns(RuntimeWarning, match="unresolved") as emitted:
        client = remote_client(tmp_path, unresolved_server, catalog_roots=["product"])
    assert len(emitted) == 1
    assert {item["top_level_directory"] for item in client.unresolved_exports} == {
        "unmapped",
        "unmapped_second",
    }


def test_all_exports_can_be_unresolved_without_blocking_construction(
    unresolved_server, tmp_path
):
    unresolved_server.directories["."] = ["unmapped"]
    with pytest.warns(RuntimeWarning, match="unresolved"):
        client = remote_client(tmp_path, unresolved_server, catalog_roots=["product"])
    inventory, _ = client.tables_available(reset=True)
    assert inventory.empty
    assert inventory.columns.tolist() == CATALOG_COLUMNS
    with pytest.raises(ValueError, match="unresolved"):
        client.set_data_product = "Candidate Product"


@pytest.mark.parametrize("stage", ["empty_root", "empty_export", "no_markers"])
def test_incomplete_unknown_export_is_reported_with_no_guessed_tables(
    unresolved_server, tmp_path, stage
):
    if stage == "empty_root":
        unresolved_server.directories["unmapped"] = []
    elif stage == "empty_export":
        unresolved_server.directories["unmapped/export"] = []
    else:
        unresolved_server.directories["unmapped"] = ["tnfs"]
        unresolved_server.directories["unmapped/tnfs"] = []

    with pytest.warns(RuntimeWarning, match="unresolved"):
        client = remote_client(tmp_path, unresolved_server, catalog_roots=["product"])

    assert client.unresolved_exports == [
        {"top_level_directory": "unmapped", "candidate_products": [], "tables": []}
    ]
    client.set_data_product = "Synthetic Product"
    assert client.set_data_product == "Synthetic Product"


def test_unresolved_exports_are_excluded_from_automatic_cleanup(
    unresolved_server, tmp_path, monkeypatch
):
    unresolved_server.directories.update(
        {
            "unmapped": ["tnfs", "old_export", "export"],
            "unmapped/tnfs": ["old.tnf", "new.tnf"],
        }
    )
    unresolved_server.contents.update(
        {
            "unmapped/tnfs/old.tnf": json.dumps({"DataFolder": "old_export"}),
            "unmapped/tnfs/new.tnf": json.dumps({"DataFolder": "export"}),
        }
    )
    unresolved_server.mtimes.update(
        {"unmapped/tnfs/old.tnf": 100, "unmapped/tnfs/new.tnf": 200}
    )
    cleanup = []
    monkeypatch.setattr(
        Sftp, "_server_clean_up", lambda self, paths, **kwargs: cleanup.extend(paths)
    )
    with pytest.warns(RuntimeWarning, match="unresolved"):
        remote_client(tmp_path, unresolved_server, catalog_roots=["product"])
    assert cleanup == ["product/old_export"]
    assert unresolved_server.removed == []


def test_explicit_template_resolves_export_without_accepting_heuristics(
    unresolved_server, tmp_path, monkeypatch
):
    def do_not_guess(tables):
        raise AssertionError("A mapped export must not use heuristic matching")

    monkeypatch.setattr("moodys_datahub.connection._table_match", do_not_guess)
    client = remote_client(
        tmp_path,
        unresolved_server,
        product_names={"unmapped": "Confirmed Product"},
    )
    assert client.unresolved_exports == []
    client.set_data_product = "Confirmed Product"
    client.set_table = "mystery"
    assert client.remote_path == "unmapped/export/mystery"


def test_interactive_unknown_export_still_requires_widget_confirmation(
    unresolved_server, tmp_path, monkeypatch
):
    prompts = []

    class FakeDropdown:
        def __init__(self, values, directories, title):
            prompts.append((values, directories))

        async def display_widgets(self):
            return ["Candidate Product"]

    monkeypatch.setattr("moodys_datahub.connection._Multi_dropdown", FakeDropdown)
    monkeypatch.setattr(
        "moodys_datahub.connection.asyncio.ensure_future",
        lambda coro: asyncio.run(coro),
    )
    client = remote_client(
        tmp_path, unresolved_server, catalog_roots=["product"], interactive=True
    )
    assert prompts == [([["Candidate Product"]], ["unmapped"])]
    assert client.unresolved_exports == []
    assert "Candidate Product" in client.tables_available()[0]["Data Product"].tolist()
    assert len(list(tmp_path.glob("*_data_products.csv"))) == 1


def test_discovery_preserves_unrelated_marker_and_uses_owned_unique_temps(
    server, tmp_path
):
    unrelated = tmp_path / "temp.tnf"
    unrelated.write_text("unrelated user data", encoding="utf-8")

    remote_client(tmp_path, server)
    remote_client(tmp_path, server)

    assert unrelated.read_text(encoding="utf-8") == "unrelated user data"
    downloaded_paths = [path for _, path in server.downloads]
    assert len(downloaded_paths) == len(set(downloaded_paths)) == 2
    assert unrelated not in downloaded_paths
    assert all(not path.exists() for path in downloaded_paths)


@pytest.mark.parametrize("failure", ["download", "json"])
def test_discovery_cleans_owned_temp_when_marker_loading_fails(
    server, tmp_path, failure
):
    unrelated = tmp_path / "temp.tnf"
    unrelated.write_text("unrelated user data", encoding="utf-8")
    if failure == "download":
        server.download_error = OSError("interrupted marker download")
        expected_error = OSError
    else:
        server.contents["product/tnfs/new.tnf"] = "invalid json"
        expected_error = json.JSONDecodeError

    with pytest.raises(expected_error):
        remote_client(tmp_path, server)

    assert unrelated.read_text(encoding="utf-8") == "unrelated user data"
    assert len(server.downloads) == 1
    assert all(not path.exists() for _, path in server.downloads)


@pytest.mark.parametrize("newest_first", [False, True])
def test_discovery_with_cleanup_disabled_never_removes_remote_markers(
    server, tmp_path, newest_first
):
    if newest_first:
        server.directories["product/tnfs"].reverse()

    client = remote_client(tmp_path, server)
    catalog, _ = client.tables_available()

    assert set(catalog["Export"]) == {"product/export"}
    assert [path for path, _ in server.downloads] == ["product/tnfs/new.tnf"]
    assert server.removed == []


def test_local_discovery_skips_root_files(server, tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    (root / "notes.txt").write_text("not an export", encoding="utf-8")
    export = root / "Synthetic Product_exported 2024-02-03_04-05-06"
    export.mkdir()
    (export / "main.csv").write_text("value\n1\n", encoding="utf-8")

    client = Sftp(local_repo=str(root), interactive=False, server_cleanup=False)
    catalog, cleanup = client.tables_available()

    assert catalog["Table"].tolist() == ["main"]
    assert catalog["Export"].tolist() == [str(export)]
    assert cleanup == []
    assert server.connections == 0
    with pytest.raises(ValueError, match="Data Product"):
        client.set_data_product = "Missing Product"


@pytest.mark.parametrize("contents", ["empty", "file_only", "empty_export"])
def test_local_discovery_handles_empty_catalog(server, tmp_path, contents):
    root = tmp_path / "repo"
    root.mkdir()
    if contents == "file_only":
        (root / "notes.txt").write_text("not an export", encoding="utf-8")
    elif contents == "empty_export":
        (root / "Synthetic Product_exported 2024-02-03_04-05-06").mkdir()

    client = Sftp(local_repo=str(root), interactive=False, server_cleanup=False)
    catalog, cleanup = client.tables_available()

    assert catalog.empty
    assert catalog.columns.tolist() == CATALOG_COLUMNS
    assert cleanup == []
    assert server.connections == 0


@pytest.mark.parametrize("path_kind", ["export", "table"])
def test_remote_path_switch_keeps_chosen_export_and_full_catalog_backup(
    server, tmp_path, path_kind
):
    server.directories.update(
        {
            ".": ["product", "second_product"],
            "second_product": ["export"],
            "second_product/export": ["main", "history"],
            "second_product/export/main": ["part.csv"],
            "second_product/export/history": ["history.csv"],
        }
    )
    client = remote_client(tmp_path, server)
    original, _ = client.tables_available()
    client.remote_path = "product/export/main"

    chosen_export = "second_product/export"
    chosen_path = chosen_export if path_kind == "export" else f"{chosen_export}/main"
    client.remote_path = chosen_path.replace("/", "\\")
    active, _ = client.tables_available()

    assert client.set_data_product == "Synthetic Product"
    assert set(active["Export"]) == {chosen_export}
    assert set(active["Table"]) == {"main", "history"}
    client.set_table = "history"
    assert client.remote_path == f"{chosen_export}/history"
    assert client.remote_files == ["history.csv"]
    client.set_table = "main"
    assert client.remote_path == f"{chosen_export}/main"
    assert client.remote_files == ["part.csv"]

    client.remote_path = "product/export/history"
    client.set_table = "main"
    assert client.remote_path == "product/export/main"
    restored, _ = client.tables_available(reset=True)
    pd.testing.assert_frame_equal(restored, original)


@pytest.mark.parametrize("unknown", ["Missing Product", "Synthetic", "Product'Unknown"])
def test_noninteractive_unknown_product_raises_without_changing_selection(
    server, tmp_path, unknown
):
    server.directories.update(
        {
            ".": ["product", "other_product"],
            "other_product": ["export"],
            "other_product/export": ["other_table"],
        }
    )
    client = remote_client(
        tmp_path, server, product_names={"other_product": "Other Product"}
    )
    client.set_data_product = "Synthetic Product"
    client.set_table = "history"
    previous_catalog, _ = client.tables_available()
    previous_path = client.remote_path
    previous_files = client.remote_files.copy()

    with pytest.raises(ValueError, match="[Dd]ata [Pp]roduct"):
        client.set_data_product = unknown

    assert client.set_data_product == "Synthetic Product"
    assert client.set_table == "history"
    assert client.remote_path == previous_path
    assert client.remote_files == previous_files
    active, _ = client.tables_available()
    pd.testing.assert_frame_equal(active, previous_catalog)


def test_interactive_unknown_product_keeps_selection_guidance(server, tmp_path, capsys):
    client = remote_client(tmp_path, server, interactive=True)
    client.set_data_product = "Synthetic Product"
    client.set_table = "history"

    client.set_data_product = "Missing Product"

    assert client.set_data_product == "Synthetic Product"
    assert client.set_table == "history"
    assert "No such Data Product" in capsys.readouterr().out


def test_noninteractive_multi_export_product_preserves_previous_selection(
    server, tmp_path
):
    server.directories.update(
        {
            ".": ["product", "second_product", "other_product"],
            "second_product": ["export"],
            "second_product/export": ["main"],
            "other_product": ["export"],
            "other_product/export": ["other_table"],
            "other_product/export/other_table": ["partition.csv"],
        }
    )
    client = remote_client(
        tmp_path, server, product_names={"other_product": "Other Product"}
    )
    client.set_data_product = "Other Product"
    client.set_table = "other_table"
    cached = tmp_path / "cached"
    cached.mkdir()
    (cached / "partition.csv").write_text("value\n1\n", encoding="utf-8")
    client.local_path = str(cached)
    previous_catalog, _ = client.tables_available()
    previous_cache = client.resolve_cache_file("partition.csv")

    with pytest.raises(ValueError, match="Multiple.*Synthetic Product"):
        client.set_data_product = "Synthetic Product"

    assert client.set_data_product == "Other Product"
    assert client.set_table == "other_table"
    assert client.remote_path == "other_product/export/other_table"
    assert client.remote_files == ["partition.csv"]
    assert client.local_path == str(cached)
    assert client.local_files == ["partition.csv"]
    assert client.resolve_cache_file("partition.csv") == previous_cache
    active, _ = client.tables_available()
    pd.testing.assert_frame_equal(active, previous_catalog)


@pytest.mark.parametrize("invalid_path", [r"missing\export", "", "   "])
@pytest.mark.parametrize("has_cache", [False, True])
def test_noninteractive_invalid_remote_path_preserves_previous_selection(
    server, tmp_path, invalid_path, has_cache
):
    client = remote_client(tmp_path, server)
    client.set_data_product = "Synthetic Product"
    client.set_table = "history"
    if has_cache:
        cached = tmp_path / "cached"
        cached.mkdir()
        (cached / "partition.csv").write_text("value\n1\n", encoding="utf-8")
        client.local_path = str(cached)
    previous_local_path = client.local_path
    previous_local_files = client.local_files.copy()
    previous_cache = client.resolve_cache_file("partition.csv")
    previous_catalog, _ = client.tables_available()

    with pytest.raises(ValueError, match="[Rr]emote path"):
        client.remote_path = invalid_path

    assert client.set_data_product == "Synthetic Product"
    assert client.set_table == "history"
    assert client.remote_path == "product/export/history"
    assert client.remote_files == ["history.csv"]
    assert client.local_path == previous_local_path
    assert client.local_files == previous_local_files
    assert client.resolve_cache_file("partition.csv") == previous_cache
    active, _ = client.tables_available()
    pd.testing.assert_frame_equal(active, previous_catalog)


def test_remote_path_lookup_error_preserves_previous_selection(server, tmp_path):
    client = remote_client(tmp_path, server)
    client.set_data_product = "Synthetic Product"
    client.set_table = "history"
    cached = tmp_path / "cached"
    cached.mkdir()
    (cached / "partition.csv").write_text("value\n1\n", encoding="utf-8")
    client.local_path = str(cached)
    previous_catalog, _ = client.tables_available()
    server.directories["unavailable/export"] = ["partition.csv"]
    server.listing_error = OSError("export lookup failed")

    with pytest.raises(OSError, match="export lookup failed"):
        client.remote_path = "unavailable/export"

    assert client.set_data_product == "Synthetic Product"
    assert client.set_table == "history"
    assert client.remote_path == "product/export/history"
    assert client.remote_files == ["history.csv"]
    assert client.local_path == str(cached)
    assert client.local_files == ["partition.csv"]
    active, _ = client.tables_available()
    pd.testing.assert_frame_equal(active, previous_catalog)


@pytest.mark.parametrize("unknown", ["Missing Table", "hist", "Table'Unknown"])
def test_noninteractive_unknown_table_raises_without_changing_selection(
    server, tmp_path, unknown
):
    client = remote_client(tmp_path, server)
    client.set_data_product = "Synthetic Product"
    client.set_table = "main"
    cached = tmp_path / "cached"
    cached.mkdir()
    (cached / "partition.csv").write_text("value\n1\n", encoding="utf-8")
    client.local_path = str(cached)
    previous_catalog, _ = client.tables_available()
    previous_path = client.remote_path
    previous_files = client.remote_files.copy()

    with pytest.raises(ValueError, match="[Tt]able"):
        client.set_table = unknown

    assert client.set_data_product == "Synthetic Product"
    assert client.set_table == "main"
    assert client.remote_path == previous_path
    assert client.remote_files == previous_files
    assert client.local_path == str(cached)
    assert client.local_files == ["partition.csv"]
    active, _ = client.tables_available()
    pd.testing.assert_frame_equal(active, previous_catalog)


def test_table_selection_does_not_implicitly_switch_exports(server, tmp_path):
    server.directories.update(
        {
            ".": ["product", "second_product"],
            "second_product": ["export"],
            "second_product/export": ["new_table"],
            "second_product/export/new_table": ["partition.csv"],
        }
    )
    client = remote_client(tmp_path, server)
    client.remote_path = "product/export/history"

    with pytest.raises(ValueError, match="[Tt]able"):
        client.set_table = "new_table"

    assert client.remote_path == "product/export/history"
    assert client.set_table == "history"
    client.remote_path = "second_product/export"
    client.set_table = "new_table"
    assert client.remote_path == "second_product/export/new_table"
    assert client.remote_files == ["partition.csv"]


def test_noninteractive_ambiguous_table_requires_explicit_export(server, tmp_path):
    server.directories.update(
        {
            ".": ["product", "second_product"],
            "second_product": ["export"],
            "second_product/export": ["main"],
        }
    )
    client = remote_client(tmp_path, server)

    with pytest.raises(ValueError, match="Multiple tables"):
        client.set_table = "main"

    assert client.set_table is None
    assert client.remote_path is None


@pytest.mark.parametrize("source", ["local", "remote"])
@pytest.mark.parametrize("same_stem_file", [False, True])
def test_selecting_partitioned_table_preserves_all_files(
    server, tmp_path, source, same_stem_file
):
    expected_files = ["part-1.csv", "part-2.csv"]
    if same_stem_file:
        expected_files.append("main.parquet")
    if source == "remote":
        server.directories["product/export/main"] = expected_files
        client = remote_client(tmp_path, server)
    else:
        export = tmp_path / "repo" / "Synthetic Product_exported 2024-02-03_04-05-06"
        table = export / "main"
        table.mkdir(parents=True)
        for filename in expected_files:
            (table / filename).write_text("value\n1\n", encoding="utf-8")
        client = Sftp(
            local_repo=str(export.parent), interactive=False, server_cleanup=False
        )

    client.set_data_product = "Synthetic Product"
    client.set_table = "main"

    assert set(client.remote_files) == set(expected_files)
    if source == "local":
        client.local_path = str(table)
        assert set(client.local_files) == set(expected_files)


@pytest.mark.parametrize("source", ["local", "remote"])
def test_flat_csv_selection_narrows_only_to_named_csv(server, tmp_path, source):
    files = ["main.parquet", "main.csv", "history.csv"]
    if source == "remote":
        server.directories["product/export"] = files
        client = remote_client(tmp_path, server)
    else:
        export = tmp_path / "repo" / "Synthetic Product_exported 2024-02-03_04-05-06"
        export.mkdir(parents=True)
        for filename in files:
            (export / filename).write_text("value\n1\n", encoding="utf-8")
        client = Sftp(
            local_repo=str(export.parent), interactive=False, server_cleanup=False
        )

    client.set_data_product = "Synthetic Product"
    client.set_table = "main"

    assert client.remote_files == ["main.csv"]
    if source == "local":
        client.local_path = str(export)
        assert client.local_files == ["main.csv"]


@pytest.mark.parametrize("path_kind", ["export", "table"])
def test_local_path_switch_scopes_active_catalog_to_chosen_export(
    server, tmp_path, path_kind
):
    root = tmp_path / "repo"
    exports = [
        root / "Synthetic Product_exported 2024-02-03_04-05-06",
        root / "Synthetic Product_exported 2024-03-04_05-06-07",
    ]
    for export in exports:
        for name in ["main", "history"]:
            table = export / name
            table.mkdir(parents=True)
            (table / f"{name}.csv").write_text("value\n1\n", encoding="utf-8")
    client = Sftp(local_repo=str(root), interactive=False, server_cleanup=False)
    original, _ = client.tables_available()
    client.remote_path = str(exports[0] / "main")

    chosen = exports[1] if path_kind == "export" else exports[1] / "main"
    client.remote_path = str(chosen)
    active, _ = client.tables_available()

    assert set(active["Export"]) == {str(exports[1])}
    assert set(active["Table"]) == {"main", "history"}
    assert client.set_data_product == "Synthetic Product"
    client.set_table = "history"
    assert client.remote_path == str(exports[1] / "history")
    assert client.remote_files == ["history.csv"]
    restored, _ = client.tables_available(reset=True)
    pd.testing.assert_frame_equal(restored, original)
    assert server.connections == 0


def test_absolute_marker_datafolder_takes_precedence_over_relative_shadow(
    server, tmp_path
):
    server.contents["product/tnfs/new.tnf"] = json.dumps(
        {"DataFolder": "/shared/export"}
    )
    server.directories.update(
        {
            "/shared/export": ["absolute_table"],
            "product/shared/export": ["shadow_table"],
        }
    )

    client = remote_client(tmp_path, server)
    catalog, _ = client.tables_available()

    assert catalog["Export"].tolist() == ["/shared/export"]
    assert catalog["Table"].tolist() == ["absolute_table"]
