# tests/test_csv_cleanup.py

import gzip
import os
from pathlib import Path

import pytest

from mario.mapping import rewrite_csv_header_with_fieldmapping
from mario.utils import gzip_file


class DummyMapping:
    as_logical = {
        "col1": "Column 1",
        "col2": "Column 2",
    }


def test_rewrite_csv_header_success(tmp_path):
    csv_file = tmp_path / "test.csv"

    csv_file.write_text(
        "col1,col2\n"
        "a,b\n"
        "c,d\n",
        encoding="utf-8",
    )

    rewrite_csv_header_with_fieldmapping(
        str(csv_file),
        DummyMapping(),
    )

    lines = csv_file.read_text(encoding="utf-8").splitlines()

    assert lines[0] == "Column 1,Column 2"
    assert lines[1] == "a,b"
    assert lines[2] == "c,d"


def test_rewrite_csv_header_removes_temp_file_on_failure(
    tmp_path,
    monkeypatch,
):
    csv_file = tmp_path / "test.csv"

    csv_file.write_text(
        "col1,col2\n"
        "a,b\n",
        encoding="utf-8",
    )

    import mario.mapping as mapping_module

    original_mkstemp = mapping_module.tempfile.mkstemp
    temp_file_path = None

    def tracked_mkstemp(*args, **kwargs):
        nonlocal temp_file_path
        fd, path = original_mkstemp(*args, **kwargs)
        temp_file_path = path
        return fd, path

    monkeypatch.setattr(
        mapping_module.tempfile,
        "mkstemp",
        tracked_mkstemp,
    )

    monkeypatch.setattr(
        mapping_module.os,
        "replace",
        lambda *_: (_ for _ in ()).throw(RuntimeError("replace failed"))
    )

    with pytest.raises(RuntimeError, match="replace failed"):
        rewrite_csv_header_with_fieldmapping(
            str(csv_file),
            DummyMapping(),
        )

    assert temp_file_path is not None
    assert not os.path.exists(temp_file_path)

    # Original file should still exist
    assert csv_file.exists()


def test_gzip_file_success(tmp_path):
    source = tmp_path / "test.csv"

    source.write_text(
        "hello\nworld\n",
        encoding="utf-8",
    )

    gz_path = gzip_file(str(source))

    assert os.path.exists(gz_path)

    with gzip.open(gz_path, "rt") as f:
        assert f.read() == "hello\nworld\n"


def test_gzip_file_removes_partial_output_on_failure(
    tmp_path,
    monkeypatch,
):
    source = tmp_path / "test.csv"

    source.write_text(
        "hello\nworld\n",
        encoding="utf-8",
    )

    import mario.utils as utils_module

    output_path = str(source) + ".gz"

    def broken_copyfileobj(src, dst, *args, **kwargs):
        dst.write(b"partial")
        raise RuntimeError("copy failed")

    monkeypatch.setattr(
        utils_module.shutil,
        "copyfileobj",
        broken_copyfileobj,
    )

    with pytest.raises(RuntimeError, match="copy failed"):
        gzip_file(str(source))

    assert not os.path.exists(output_path)


def test_save_data_as_csv_removes_source_after_successful_gzip(
    tmp_path,
    monkeypatch,
):
    import mario.athena as athena_module

    # Create an instance without calling __init__
    extractor = athena_module.AthenaDataExtractor.__new__(
        athena_module.AthenaDataExtractor
    )
    extractor.mapping = DummyMapping()

    monkeypatch.setattr(
        extractor,
        "run_query",
        lambda: "s3://dummy/result.csv",
    )

    monkeypatch.setattr(
        athena_module.wr.s3,
        "download",
        lambda path, local_file: Path(local_file).write_text(
            "col1,col2\n"
            "a,b\n",
            encoding="utf-8",
        ),
    )

    csv_path = tmp_path / "result.csv"

    gz_path = extractor.save_data_as_csv(
        file_path=str(csv_path),
        compress_using_gzip=True,
    )

    assert os.path.exists(gz_path)

    # Original should have been deleted
    assert not csv_path.exists()

    with gzip.open(gz_path, "rt") as f:
        contents = f.read()

    assert "Column 1,Column 2" in contents


def test_save_data_as_csv_removes_downloaded_file_if_header_rewrite_fails(
    tmp_path,
    monkeypatch,
):
    import mario.athena as athena_module

    extractor = athena_module.AthenaDataExtractor.__new__(
        athena_module.AthenaDataExtractor
    )
    extractor.mapping = DummyMapping()

    monkeypatch.setattr(
        extractor,
        "run_query",
        lambda: "s3://dummy/result.csv",
    )

    monkeypatch.setattr(
        athena_module.wr.s3,
        "download",
        lambda path, local_file: Path(local_file).write_text(
            "col1,col2\n"
            "a,b\n",
            encoding="utf-8",
        ),
    )

    def explode(*args, **kwargs):
        raise RuntimeError("rewrite failed")

    monkeypatch.setattr(
        athena_module,
        "rewrite_csv_header_with_fieldmapping",
        explode,
    )

    csv_path = tmp_path / "result.csv"

    with pytest.raises(RuntimeError, match="rewrite failed"):
        extractor.save_data_as_csv(
            file_path=str(csv_path),
            compress_using_gzip=False,
        )

    # Downloaded file should have been cleaned up
    assert not csv_path.exists()


def test_save_data_as_csv_removes_partial_gzip_if_compression_fails(
    tmp_path,
    monkeypatch,
):
    import mario.athena as athena_module

    extractor = athena_module.AthenaDataExtractor.__new__(
        athena_module.AthenaDataExtractor
    )
    extractor.mapping = DummyMapping()

    monkeypatch.setattr(
        extractor,
        "run_query",
        lambda: "s3://dummy/result.csv",
    )

    monkeypatch.setattr(
        athena_module.wr.s3,
        "download",
        lambda path, local_file: Path(local_file).write_text(
            "col1,col2\n"
            "a,b\n",
            encoding="utf-8",
        ),
    )

    def fail_gzip(*args, **kwargs):
        output_path = str(tmp_path / "result.csv.gz")

        # simulate partially-written gzip
        Path(output_path).write_bytes(b"partial")

        if os.path.exists(output_path):
            os.remove(output_path)

        raise RuntimeError("gzip failed")

    monkeypatch.setattr(
        athena_module,
        "gzip_file",
        fail_gzip,
    )

    csv_path = tmp_path / "result.csv"

    with pytest.raises(RuntimeError, match="gzip failed"):
        extractor.save_data_as_csv(
            file_path=str(csv_path),
            compress_using_gzip=True,
        )

    # Original CSV should still exist because compression failed
    assert csv_path.exists()

    # No stray gzip should remain
    assert not (tmp_path / "result.csv.gz").exists()



