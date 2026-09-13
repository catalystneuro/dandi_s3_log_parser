"""Tests for the assertion helpers that the extraction test suites rely on."""

import pathlib

import pytest

from s3_log_extraction.testing import assert_expected_extraction_content, assert_filetree_matches

_EXTRACTOR_NAME = "S3LogAccessExtractor"
_RECORD_FILE_NAMES = (
    f"records/{_EXTRACTOR_NAME}_file-processing-start.txt",
    f"records/{_EXTRACTOR_NAME}_file-processing-end.txt",
)


def _write(file_path: pathlib.Path, content: str) -> None:
    """Write `content` to `file_path`, creating any missing parent directories."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)


def _build_extraction_tree(directory: pathlib.Path, *, record_lines: list[str], timestamps: str) -> None:
    """Lay out the extraction output and records of a single dandiset within `directory`."""
    _write(directory / "extraction" / "ds000001" / "timestamps.txt", timestamps)
    for record_file_name in _RECORD_FILE_NAMES:
        _write(directory / record_file_name, "".join(f"{line}\n" for line in record_lines))


@pytest.fixture
def relative_file_paths() -> set[pathlib.Path]:
    """The relative paths of the extraction tree built by `_build_extraction_tree`."""
    return {pathlib.Path("extraction/ds000001/timestamps.txt"), *(pathlib.Path(name) for name in _RECORD_FILE_NAMES)}


@pytest.mark.ai_generated
def test_assert_expected_extraction_content_matching(
    tmp_path: pathlib.Path, relative_file_paths: set[pathlib.Path]
) -> None:
    """Identical output and expected trees should pass."""
    output_directory = tmp_path / "output"
    expected_output_directory = tmp_path / "expected"
    for directory in (output_directory, expected_output_directory):
        _build_extraction_tree(directory, record_lines=["log_a", "log_b"], timestamps="200101\n")

    assert_expected_extraction_content(
        extractor_name=_EXTRACTOR_NAME,
        output_directory=output_directory,
        expected_output_directory=expected_output_directory,
        relative_output_files=relative_file_paths,
        relative_expected_files=relative_file_paths,
    )


@pytest.mark.ai_generated
def test_assert_expected_extraction_content_ignores_record_order(
    tmp_path: pathlib.Path, relative_file_paths: set[pathlib.Path]
) -> None:
    """Record files are compared as line sets, so a different ordering should still pass."""
    output_directory = tmp_path / "output"
    expected_output_directory = tmp_path / "expected"
    _build_extraction_tree(output_directory, record_lines=["log_a", "log_b"], timestamps="200101\n")
    _build_extraction_tree(expected_output_directory, record_lines=["log_b", "log_a"], timestamps="200101\n")

    assert_expected_extraction_content(
        extractor_name=_EXTRACTOR_NAME,
        output_directory=output_directory,
        expected_output_directory=expected_output_directory,
        relative_output_files=relative_file_paths,
        relative_expected_files=relative_file_paths,
    )


@pytest.mark.ai_generated
def test_assert_expected_extraction_content_detects_content_mismatch(
    tmp_path: pathlib.Path, relative_file_paths: set[pathlib.Path]
) -> None:
    """Differing extraction content should fail with a binary content mismatch."""
    output_directory = tmp_path / "output"
    expected_output_directory = tmp_path / "expected"
    _build_extraction_tree(output_directory, record_lines=["log_a"], timestamps="200101\n")
    _build_extraction_tree(expected_output_directory, record_lines=["log_a"], timestamps="210101\n")

    with pytest.raises(AssertionError, match="Binary content mismatch"):
        assert_expected_extraction_content(
            extractor_name=_EXTRACTOR_NAME,
            output_directory=output_directory,
            expected_output_directory=expected_output_directory,
            relative_output_files=relative_file_paths,
            relative_expected_files=relative_file_paths,
        )


@pytest.mark.ai_generated
def test_assert_expected_extraction_content_detects_record_mismatch(
    tmp_path: pathlib.Path, relative_file_paths: set[pathlib.Path]
) -> None:
    """A record file listing a different set of logs should fail with a line set mismatch."""
    output_directory = tmp_path / "output"
    expected_output_directory = tmp_path / "expected"
    _build_extraction_tree(output_directory, record_lines=["log_a", "log_b"], timestamps="200101\n")
    _build_extraction_tree(expected_output_directory, record_lines=["log_a"], timestamps="200101\n")

    with pytest.raises(AssertionError, match="Line set mismatch"):
        assert_expected_extraction_content(
            extractor_name=_EXTRACTOR_NAME,
            output_directory=output_directory,
            expected_output_directory=expected_output_directory,
            relative_output_files=relative_file_paths,
            relative_expected_files=relative_file_paths,
        )


@pytest.mark.ai_generated
def test_assert_filetree_matches_identical_trees(tmp_path: pathlib.Path) -> None:
    """Trees with the same files and contents should pass, including nested directories."""
    test_directory = tmp_path / "test"
    expected_directory = tmp_path / "expected"
    for directory in (test_directory, expected_directory):
        _write(directory / "top.txt", "top\n")
        _write(directory / "nested" / "inner.txt", "inner\n")

    assert_filetree_matches(test_dir=test_directory, expected_dir=expected_directory)


@pytest.mark.ai_generated
def test_assert_filetree_matches_ignores_surrounding_whitespace(tmp_path: pathlib.Path) -> None:
    """File contents are stripped before comparison, so trailing newlines should not matter."""
    test_directory = tmp_path / "test"
    expected_directory = tmp_path / "expected"
    _write(test_directory / "top.txt", "top")
    _write(expected_directory / "top.txt", "top\n\n")

    assert_filetree_matches(test_dir=test_directory, expected_dir=expected_directory)


@pytest.mark.ai_generated
def test_assert_filetree_matches_detects_missing_file(tmp_path: pathlib.Path) -> None:
    """A file present only in the expected tree should fail with a file tree mismatch."""
    test_directory = tmp_path / "test"
    expected_directory = tmp_path / "expected"
    _write(test_directory / "top.txt", "top\n")
    _write(expected_directory / "top.txt", "top\n")
    _write(expected_directory / "extra.txt", "extra\n")

    with pytest.raises(AssertionError, match="File trees do not match"):
        assert_filetree_matches(test_dir=test_directory, expected_dir=expected_directory)


@pytest.mark.ai_generated
def test_assert_filetree_matches_detects_content_mismatch(tmp_path: pathlib.Path) -> None:
    """Matching file names with differing contents should fail with a content mismatch."""
    test_directory = tmp_path / "test"
    expected_directory = tmp_path / "expected"
    _write(test_directory / "top.txt", "actual\n")
    _write(expected_directory / "top.txt", "expected\n")

    with pytest.raises(AssertionError, match="Content mismatch in file"):
        assert_filetree_matches(test_dir=test_directory, expected_dir=expected_directory)
