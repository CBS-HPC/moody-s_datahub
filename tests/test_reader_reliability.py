from concurrent.futures import ThreadPoolExecutor
from multiprocessing.dummy import Pool as ThreadPool
from types import SimpleNamespace

import fastavro
import pandas as pd
import polars as pl
import pytest
from pyarrow.lib import ArrowInvalid

from moodys_datahub import Sftp, utils
from moodys_datahub.utils import (
    _create_chunks,
    _create_workers,
    _join_remote_path,
    _load_csv_table,
    _load_pd,
    _load_pl,
    _save_chunks,
)


@pytest.mark.parametrize("failure", ["missing_column", "corruption"])
def test_arrow_read_failure_preserves_source_and_siblings(tmp_path, failure):
    source_dir = tmp_path / "export"
    source_dir.mkdir()
    source = source_dir / "source.parquet"
    sibling = source_dir / "sibling.parquet"
    nested = source_dir / "nested" / "notes.txt"
    nested.parent.mkdir()
    sibling.write_bytes(b"sibling data")
    nested.write_bytes(b"nested data")
    if failure == "missing_column":
        pd.DataFrame({"value": [1, 2]}).to_parquet(source, index=False)
        select_cols = ["missing"]
    else:
        source.write_bytes(b"not a parquet file")
        select_cols = None
    original = {path: path.read_bytes() for path in (source, sibling, nested)}

    with pytest.raises(ValueError) as caught:
        _load_pd(str(source), select_cols=select_cols)

    assert isinstance(caught.value.__cause__, ArrowInvalid)
    assert source_dir.is_dir(), "A read error must never delete its source directory"
    assert {path: path.read_bytes() for path in original} == original
    assert source.name in str(caught.value)
    assert "removed" not in str(caught.value)


@pytest.mark.parametrize("failure", ["missing_column", "corruption"])
@pytest.mark.parametrize("num_workers", [1, 2])
def test_public_pandas_all_read_failure_preserves_source_directory(
    tmp_path, monkeypatch, failure, num_workers
):
    source_dir = tmp_path / "export"
    source_dir.mkdir()
    source = source_dir / "source.parquet"
    sibling = source_dir / "sibling.parquet"
    nested = source_dir / "nested" / "notes.txt"
    nested.parent.mkdir()
    pd.DataFrame({"value": [3]}).to_parquet(sibling, index=False)
    nested.write_bytes(b"nested data")
    if failure == "missing_column":
        pd.DataFrame({"value": [1, 2]}).to_parquet(source, index=False)
        select_cols = ["missing"]
    else:
        source.write_bytes(b"not a parquet file")
        select_cols = ["value"]
    original = {path: path.read_bytes() for path in (source, sibling, nested)}
    client = Sftp(offline=True, interactive=False, output_root=tmp_path / "results")
    client.output_format = None
    client.delete_files = False
    client._max_path_length = 10000

    def forbidden(*args, **kwargs):
        pytest.fail("Reading explicit local sources must not connect or prompt")

    monkeypatch.setattr(client, "_connect", forbidden)
    monkeypatch.setattr(client, "select_data", forbidden)

    with pytest.raises((ValueError, RuntimeError)) as caught:
        client.pandas_all(
            files=[str(source), str(sibling)],
            select_cols=select_cols,
            num_workers=num_workers,
            pool_method="threading",
        )

    assert source_dir.is_dir()
    assert {path: path.read_bytes() for path in original} == original
    assert source.name in str(caught.value)


@pytest.mark.parametrize(
    ("parts", "expected"),
    [
        (("/", "part.csv"), "/part.csv"),
        ((None, " / ", "part.csv"), "/part.csv"),
        (("/", "/", "part.csv"), "/part.csv"),
        (("/", "folder", "part.csv"), "/folder/part.csv"),
        (("/",), "/"),
        (("folder", "/", "part.csv"), "folder/part.csv"),
    ],
)
def test_join_remote_path_preserves_initial_root(parts, expected):
    assert _join_remote_path(*parts) == expected


@pytest.mark.parametrize(
    "contents",
    [
        "id,value\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n",
        "id,value\n1,10\n2,20\n3,30\n4,40\n",
        "id,value\n1,10\n2,20\n3,30",
        'id,value\n1,"first\nsecond"\n2,"quoted, comma"\n3,"a ""quote"""',
        "id,value\n1,10\n",
        "id,value\n",
        "id,value\n\n1,10\n\n2,20\n\n3,30\n",
        '\ufeffid,value\r\n1,"first\r\nsecond"\r\n2,last',
    ],
    ids=[
        "remainder",
        "headers",
        "no_final_newline",
        "multiline_quoted",
        "fewer_rows_than_workers",
        "header_only",
        "blank_lines",
        "bom_crlf",
    ],
)
@pytest.mark.parametrize("num_workers", [2, 8])
def test_csv_reader_retains_every_logical_record(
    tmp_path, monkeypatch, contents, num_workers
):
    source = tmp_path / "records.csv"
    source.write_text(contents, encoding="utf-8", newline="")
    expected = pd.read_csv(source, low_memory=False)
    # Exercise real readers without Windows process-spawn overhead in the red run.
    monkeypatch.setattr(utils, "Pool", ThreadPool)

    sequential = _load_csv_table(str(source), num_workers=1)
    parallel = _load_csv_table(str(source), num_workers=num_workers)

    pd.testing.assert_frame_equal(sequential, expected)
    pd.testing.assert_frame_equal(parallel, expected)


@pytest.mark.parametrize("engine", ["pandas", "polars"])
def test_avro_reader_selects_columns_and_filters_real_records(tmp_path, engine):
    source = tmp_path / "records.avro"
    schema = {
        "type": "record",
        "name": "Record",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "value", "type": "string"},
            {"name": "unused", "type": "boolean"},
        ],
    }
    records = [
        {"id": 1, "value": "first", "unused": False},
        {"id": 2, "value": "second", "unused": True},
    ]
    with source.open("wb") as stream:
        fastavro.writer(stream, schema, records)

    if engine == "pandas":
        result = _load_pd(str(source), select_cols=["value", "id"], query="id > 1")
        assert result.columns.tolist() == ["value", "id"]
        assert result.to_dict("records") == [{"value": "second", "id": 2}]
    else:
        result = _load_pl(
            [str(source)], select_cols=["value", "id"], query=pl.col("id") > 1
        )
        assert result.columns == ["value", "id"]
        assert result.to_dicts() == [{"value": "second", "id": 2}]


@pytest.mark.parametrize("engine", ["pandas", "polars"])
def test_xlsx_chunk_sizing(engine):
    source = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    frame = source if engine == "pandas" else pl.from_pandas(source)

    assert _create_chunks(frame, [".xlsx"]) == (1, 3, 1_000_000)
    large = pd.DataFrame({"id": range(1_000_001)})
    frame = large if engine == "pandas" else pl.from_pandas(large)
    assert _create_chunks(frame, [".xlsx"]) == (2, 1_000_001, 1_000_000)


def test_xlsx_save_round_trip(tmp_path):
    source = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    _, files = _save_chunks(source, str(tmp_path / "records"), [".xlsx"])

    assert len(files) == 1
    pd.testing.assert_frame_equal(pd.read_excel(files[0]), source)


def test_ipc_chunk_sizing_and_save_round_trip(tmp_path):
    source = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})

    assert _create_chunks(source, [".ipc"]) == (1, 3, 3)
    _, files = _save_chunks(source, str(tmp_path / "records"), [".ipc"])

    assert len(files) == 1
    assert pl.read_ipc(files[0]).equals(source)


@pytest.mark.parametrize("memory_gib", [1, 4, 8, 11])
def test_auto_workers_use_one_worker_for_nonempty_low_memory_jobs(
    monkeypatch, memory_gib
):
    monkeypatch.setattr(
        utils.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(total=memory_gib * 1024**3),
    )
    pool, method = _create_workers(num_workers=-1, n_total=3, pool_method="threading")

    with pool:
        assert list(pool.map(lambda value: value + 1, [1, 2, 3])) == [2, 3, 4]
    assert method == "thread"


@pytest.mark.parametrize("num_workers", [-1, 0, 1, 2, 8])
def test_csv_reader_is_bounded_and_does_not_create_nested_pools(
    tmp_path, monkeypatch, num_workers
):
    source = tmp_path / "large.csv"
    expected = pd.DataFrame({"id": range(100_003), "value": "quoted,\nvalue"})
    expected.to_csv(source, index=False)
    read_csv = pd.read_csv
    batch_sizes = []
    contexts_closed = []

    def tracked_read_csv(*args, **kwargs):
        if kwargs.get("nrows") == 0:
            return read_csv(*args, **kwargs)
        assert 0 < kwargs["chunksize"] <= 100_000
        assert "skiprows" not in kwargs
        reader = read_csv(*args, **kwargs)

        class TrackedReader:
            def __enter__(self):
                reader.__enter__()
                return self

            def __iter__(self):
                for batch in reader:
                    batch_sizes.append(len(batch))
                    yield batch

            def __exit__(self, *exc):
                result = reader.__exit__(*exc)
                contexts_closed.append(True)
                return result

        return TrackedReader()

    def forbidden_pool(*args, **kwargs):
        pytest.fail("CSV readers must not create nested worker pools")

    monkeypatch.setattr(pd, "read_csv", tracked_read_csv)
    monkeypatch.setattr(utils, "Pool", forbidden_pool)
    monkeypatch.setattr(utils, "ThreadPoolExecutor", forbidden_pool)
    monkeypatch.setattr(utils, "_run_parallel", forbidden_pool)

    # Simulate an already-parallel caller, with a reader budget per outer worker.
    with ThreadPoolExecutor(max_workers=1) as outer_pool:
        result = outer_pool.submit(
            _load_csv_table, str(source), num_workers=num_workers
        ).result()

    pd.testing.assert_frame_equal(result, expected)
    assert batch_sizes == [100_000, 3]
    assert contexts_closed == [True]


def _keep_global_maximum(frame, minimum):
    return frame.loc[
        (frame["value"] == frame["value"].max()) & (frame["id"] >= minimum)
    ]


@pytest.mark.parametrize("query", ["value >= 99998", _keep_global_maximum])
def test_csv_filters_and_callable_arguments_are_independent_of_worker_budget(
    tmp_path, query
):
    source = tmp_path / "filtered.csv"
    frame = pd.DataFrame(
        {
            "id": range(100_003),
            "value": range(100_003),
            "bvd_id": "DK123",
            "date": "2020-06-01",
            "unused": "unused",
        }
    )
    frame.to_csv(source, index=False)
    options = {
        "select_cols": ["value", "id", "date", "bvd_id"],
        "date_query": [2020, 2020, "date", "remove"],
        "bvd_query": "bvd_id == 'DK123'",
        "query": query,
        "query_args": [99998] if callable(query) else None,
    }
    expected = _load_pd(str(source), **options).loc[:, options["select_cols"]]

    for num_workers in (1, 2, 8):
        result = _load_csv_table(str(source), num_workers=num_workers, **options)
        pd.testing.assert_frame_equal(result, expected)


def test_csv_projects_columns_and_filters_before_concatenating(tmp_path, monkeypatch):
    source = tmp_path / "selective.csv"
    frame = pd.DataFrame(
        {
            "id": range(100_003),
            "payload": "not needed",
            "value": range(100_003),
            "date": ["2019-01-01"] * 100_000 + ["2020-06-01"] * 3,
            "bvd_id": ["DK123"] * 100_000 + ["DK123", "US123", "DK123"],
        }
    )
    frame.to_csv(source, index=False)
    options = {
        "select_cols": ["value", "id", "date", "bvd_id"],
        "date_query": [2020, 2020, "date", "remove"],
        "bvd_query": "bvd_id == 'DK123'",
        "query": _keep_global_maximum,
        "query_args": [99998],
    }
    expected = _load_pd(str(source), **options).loc[:, options["select_cols"]]
    read_csv = pd.read_csv
    concat = pd.concat
    concat_sizes = []

    def projected_read_csv(*args, **kwargs):
        if kwargs.get("nrows") != 0:
            assert kwargs["usecols"] == options["select_cols"]
        return read_csv(*args, **kwargs)

    def selective_concat(frames, *args, **kwargs):
        batches = list(frames)
        assert all(
            set(batch.columns) <= set(options["select_cols"]) for batch in batches
        )
        assert all(
            batch.empty or batch.columns.tolist() == options["select_cols"]
            for batch in batches
        )
        concat_sizes.append([len(batch) for batch in batches])
        return concat(batches, *args, **kwargs)

    monkeypatch.setattr(pd, "read_csv", projected_read_csv)
    monkeypatch.setattr(pd, "concat", selective_concat)

    result = _load_csv_table(str(source), num_workers=4, **options)

    pd.testing.assert_frame_equal(result, expected)
    assert concat_sizes == [[0, 2]]
