"""Tests for the randomized benchmark log generator."""

import pathlib

import pytest

from s3_log_extraction.testing import generate_benchmark

_BENCHMARK_DIRECTORY_NAME = "s3-log-extraction-benchmark"


def _create_single_date_directory(*, directory: pathlib.Path, **kwargs) -> None:
    """Create one year/month/day directory in place of the six-year tree of the real generator."""
    (directory / "2020" / "01" / "01").mkdir(parents=True)


@pytest.fixture(autouse=True)
def small_date_range(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shrink the generated date range, which otherwise spans six years and produces roughly 120 MB of logs."""
    monkeypatch.setattr(
        "s3_log_extraction.testing._benchmarking._create_date_directories", _create_single_date_directory
    )


def _log_files(benchmark_directory: pathlib.Path) -> list[pathlib.Path]:
    """All generated log files, which are the leaves of the year/month/day directory tree."""
    return sorted(file_path for file_path in benchmark_directory.rglob(pattern="*-*-*-*-*-*-*") if file_path.is_file())


@pytest.mark.ai_generated
def test_generate_benchmark_creates_log_files(tmp_path: pathlib.Path) -> None:
    """The generator should lay out a year/month/day tree of non-empty log files."""
    generate_benchmark(directory=tmp_path)

    benchmark_directory = tmp_path / _BENCHMARK_DIRECTORY_NAME
    log_files = _log_files(benchmark_directory)

    assert (benchmark_directory / "2020" / "01" / "01").is_dir() is True
    assert len(log_files) > 0
    assert all(log_file.stat().st_size > 0 for log_file in log_files)


@pytest.mark.ai_generated
def test_generate_benchmark_lines_have_expected_shape(tmp_path: pathlib.Path) -> None:
    """Each generated line should carry the date of its file and a recognized request type and status."""
    generate_benchmark(directory=tmp_path)

    log_files = _log_files(tmp_path / _BENCHMARK_DIRECTORY_NAME)
    for log_file in log_files:
        for line in log_file.read_text().splitlines():
            fields = line.split(" ")

            assert fields[2].startswith("[01/Jan/2020:") is True
            assert fields[7].startswith("REST.") is True
            assert fields[12].isdigit() is True
            assert fields[10] == f"/{fields[8]}"


@pytest.mark.ai_generated
@pytest.mark.parametrize("seed", [0, 1])
def test_generate_benchmark_is_reproducible(tmp_path: pathlib.Path, seed: int) -> None:
    """The same seed should produce byte-identical benchmarks."""
    first_directory = tmp_path / "first"
    first_directory.mkdir()
    second_directory = tmp_path / "second"
    second_directory.mkdir()

    generate_benchmark(directory=first_directory, seed=seed)
    generate_benchmark(directory=second_directory, seed=seed)

    first_contents = {
        log_file.relative_to(first_directory): log_file.read_bytes()
        for log_file in _log_files(first_directory / _BENCHMARK_DIRECTORY_NAME)
    }
    second_contents = {
        log_file.relative_to(second_directory): log_file.read_bytes()
        for log_file in _log_files(second_directory / _BENCHMARK_DIRECTORY_NAME)
    }

    assert first_contents == second_contents


@pytest.mark.ai_generated
def test_generate_benchmark_differs_by_seed(tmp_path: pathlib.Path) -> None:
    """Different seeds should produce different benchmarks."""
    first_directory = tmp_path / "first"
    first_directory.mkdir()
    second_directory = tmp_path / "second"
    second_directory.mkdir()

    generate_benchmark(directory=first_directory, seed=0)
    generate_benchmark(directory=second_directory, seed=1)

    first_names = {log_file.name for log_file in _log_files(first_directory / _BENCHMARK_DIRECTORY_NAME)}
    second_names = {log_file.name for log_file in _log_files(second_directory / _BENCHMARK_DIRECTORY_NAME)}

    assert first_names != second_names


@pytest.mark.ai_generated
def test_generate_benchmark_warns_and_replaces_existing_contents(tmp_path: pathlib.Path) -> None:
    """A non-empty benchmark directory should be reported and then cleared before regenerating."""
    benchmark_directory = tmp_path / _BENCHMARK_DIRECTORY_NAME
    benchmark_directory.mkdir()
    stale_file_path = benchmark_directory / "stale.txt"
    stale_file_path.write_text("stale\n")

    with pytest.warns(UserWarning, match="is not empty"):
        generate_benchmark(directory=tmp_path)

    assert stale_file_path.exists() is False
    assert len(_log_files(benchmark_directory)) > 0
