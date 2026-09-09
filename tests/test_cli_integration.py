"""CLI integration tests that mirror the existing API-based integration tests."""

import pathlib
import shutil

import pandas
import py
import pytest
from click.testing import CliRunner

import s3_log_extraction


def _run_cli_extraction_test(tmpdir: py.path.local, workers: int) -> None:
    """
    Helper function to run CLI extraction tests with a specified number of workers.

    Parameters
    ----------
    tmpdir : py.path.local
        Temporary directory for test outputs.
    workers : int
        Number of workers to use for extraction.
    """
    tmpdir = pathlib.Path(tmpdir)

    base_directory = pathlib.Path(__file__).parent
    test_logs_directory = base_directory / "example_logs"
    output_directory = tmpdir / "test_extraction"
    output_directory.mkdir(exist_ok=True)
    expected_output_directory = base_directory / "expected_output"

    runner = CliRunner()

    # Run extraction via CLI
    result = runner.invoke(
        s3_log_extraction.s3logextraction_cli,
        [
            "extract",
            str(test_logs_directory),
            "--workers",
            str(workers),
            "--cache",
            str(output_directory),
            "--encryption",
            "false",
        ],
    )
    assert result.exit_code == 0, f"Extraction failed: {result.output}"

    # Verify output files match expected structure
    relative_output_files = {file.relative_to(output_directory) for file in output_directory.rglob(pattern="*.txt")}
    relative_expected_files = {
        file.relative_to(expected_output_directory)
        for file in expected_output_directory.rglob(pattern="*.txt")
        if "summaries" not in file.parts
    }
    assert relative_output_files == relative_expected_files

    # Verify content matches expected output
    s3_log_extraction.testing.assert_expected_extraction_content(
        extractor_name="S3LogAccessExtractor",
        output_directory=output_directory,
        expected_output_directory=expected_output_directory,
        relative_output_files=relative_output_files,
        relative_expected_files=relative_expected_files,
    )


def test_cli_extraction(tmpdir: py.path.local) -> None:
    """Test extraction using the CLI instead of the API."""
    _run_cli_extraction_test(tmpdir, workers=1)


def test_cli_extraction_parallel(tmpdir: py.path.local) -> None:
    """Test parallel extraction using the CLI instead of the API."""
    _run_cli_extraction_test(tmpdir, workers=2)


@pytest.mark.usefixtures("use_mocked_region_resolver")
def test_cli_generic_summaries(tmpdir: py.path.local) -> None:
    """Test summary generation using the CLI instead of the API."""
    test_dir = pathlib.Path(tmpdir)

    base_tests_dir = pathlib.Path(__file__).parent
    expected_output_dir = base_tests_dir / "expected_output"
    expected_extraction_dir = expected_output_dir / "extraction"
    expected_summaries_dir = expected_output_dir / "summaries"

    test_extraction_dir = test_dir / "extraction"
    test_summary_dir = test_dir / "summaries"
    shutil.copytree(src=expected_extraction_dir, dst=test_extraction_dir)
    # Every requester of the example logs is a documentation-range address, which a real resolution labels
    # `bogon`; the mocked resolver stands in for one so that the summaries have regions to report

    runner = CliRunner()

    # Set cache directory via CLI
    result = runner.invoke(s3_log_extraction.s3logextraction_cli, ["config", "cache", "set", str(test_dir)])
    assert result.exit_code == 0, f"Failed to set cache: {result.output}"

    # Generate summaries via CLI
    result = runner.invoke(s3_log_extraction.s3logextraction_cli, ["update", "summaries", "--encryption", "false"])
    assert result.exit_code == 0, f"Failed to generate summaries: {result.output}"

    # Generate dataset totals via CLI
    result = runner.invoke(s3_log_extraction.s3logextraction_cli, ["update", "totals"])
    assert result.exit_code == 0, f"Failed to generate totals: {result.output}"

    # Generate archive summaries via CLI
    result = runner.invoke(s3_log_extraction.s3logextraction_cli, ["update", "summaries", "--mode", "archive"])
    assert result.exit_code == 0, f"Failed to generate archive summaries: {result.output}"

    # Generate archive totals via CLI
    result = runner.invoke(s3_log_extraction.s3logextraction_cli, ["update", "totals", "--mode", "archive"])
    assert result.exit_code == 0, f"Failed to generate archive totals: {result.output}"

    # Verify the output matches expected files
    test_file_paths = {path.relative_to(test_summary_dir): path for path in test_summary_dir.rglob(pattern="*.tsv")}
    expected_file_paths = {
        path.relative_to(expected_summaries_dir): path for path in expected_summaries_dir.rglob(pattern="*.tsv")
    }
    assert set(test_file_paths.keys()) == set(expected_file_paths.keys())

    for expected_file_path in expected_file_paths.values():
        relative_file_path = expected_file_path.relative_to(expected_summaries_dir)
        test_file_path = test_summary_dir / relative_file_path

        test_mapped_log = pandas.read_table(filepath_or_buffer=test_file_path, index_col=0)
        expected_mapped_log = pandas.read_table(filepath_or_buffer=expected_file_path, index_col=0)

        # Pandas assertion makes no reference to the case being tested when it fails
        try:
            pandas.testing.assert_frame_equal(left=test_mapped_log, right=expected_mapped_log)
        except AssertionError as exception:
            message = (
                f"\n\nTest file path: {test_file_path}\nExpected file path: {expected_file_path}\n\n"
                f"{str(exception)}\n\n"
            )
            raise AssertionError(message)


@pytest.mark.ai_generated
@pytest.mark.usefixtures("use_mocked_region_resolver")
def test_cli_summaries_region_disclosure_threshold(tmpdir: py.path.local) -> None:
    """The disclosure threshold of the by-region summaries is settable from the command line."""
    test_dir = pathlib.Path(tmpdir)

    base_tests_dir = pathlib.Path(__file__).parent
    shutil.copytree(src=base_tests_dir / "expected_output" / "extraction", dst=test_dir / "extraction")

    runner = CliRunner()

    # The example logs span six resolved regions, which does not clear a threshold of six
    result = runner.invoke(
        s3_log_extraction.s3logextraction_cli,
        ["update", "summaries", "--cache", str(test_dir), "--encryption", "false", "--threshold", "6"],
    )
    assert result.exit_code == 0, f"Failed to generate summaries: {result.output}"
    assert not (test_dir / "summaries" / "ds001161" / "by_region.tsv").exists()

    # But it does clear a threshold of five, which is the default
    result = runner.invoke(
        s3_log_extraction.s3logextraction_cli,
        ["update", "summaries", "--cache", str(test_dir), "--encryption", "false", "--threshold", "5"],
    )
    assert result.exit_code == 0, f"Failed to generate summaries: {result.output}"

    by_region = pandas.read_table(filepath_or_buffer=test_dir / "summaries" / "ds001161" / "by_region.tsv")
    assert len(by_region) == 7  # Six resolved regions, plus the requester that could not be geolocated
