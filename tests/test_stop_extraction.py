"""Tests for discovering running extraction processes and signalling them to stop."""

import pathlib

import pytest

from s3_log_extraction.extractors import get_running_pids, stop_extraction

_STOP_FILE_NAME = ".stop_extraction"


class _FakeProcess:
    """A stand-in for `psutil.Process` exposing only the attributes that the PID lookup reads."""

    def __init__(self, *, name: str, pid: int) -> None:
        self.info = {"name": name, "pid": pid}


@pytest.mark.ai_generated
def test_get_running_pids_finds_extraction_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only processes named `s3logextraction` should be reported."""
    processes = [
        _FakeProcess(name="s3logextraction", pid=101),
        _FakeProcess(name="s3logextraction", pid=102),
        _FakeProcess(name="python", pid=103),
    ]
    monkeypatch.setattr("s3_log_extraction.extractors._stop.psutil.process_iter", lambda attrs: processes)

    assert get_running_pids() == {"101", "102"}


@pytest.mark.ai_generated
def test_get_running_pids_excludes_the_current_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """The calling process is the one asking to stop, so it should never be included."""
    current_pid = 4242
    processes = [
        _FakeProcess(name="s3logextraction", pid=current_pid),
        _FakeProcess(name="s3logextraction", pid=101),
    ]
    monkeypatch.setattr("s3_log_extraction.extractors._stop.psutil.process_iter", lambda attrs: processes)
    monkeypatch.setattr("s3_log_extraction.extractors._stop.os.getpid", lambda: current_pid)

    assert get_running_pids() == {"101"}


@pytest.mark.ai_generated
def test_stop_extraction_without_running_processes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
) -> None:
    """With nothing running, no stop file should be created."""
    monkeypatch.setattr("s3_log_extraction.extractors._stop.get_running_pids", set)

    stop_extraction(cache_directory=tmp_path)

    assert "No extraction processes are currently running." in capsys.readouterr().out
    assert (tmp_path / "extraction" / _STOP_FILE_NAME).exists() is False


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("running_pids", "expected_message_fragment"),
    [
        ({"101"}, "on PID 101"),
        ({"101", "102"}, "on PIDs ["),
    ],
)
def test_stop_extraction_cleans_up_once_processes_exit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture,
    running_pids: set[str],
    expected_message_fragment: str,
) -> None:
    """The stop file should be written and then removed once the processes are gone."""
    stop_file_path = tmp_path / "extraction" / _STOP_FILE_NAME
    observed_stop_file_states = []

    def _fake_get_running_pids() -> set[str]:
        if not observed_stop_file_states:
            observed_stop_file_states.append(stop_file_path.exists())
            return running_pids
        observed_stop_file_states.append(stop_file_path.exists())
        return set()

    monkeypatch.setattr("s3_log_extraction.extractors._stop.get_running_pids", _fake_get_running_pids)

    stop_extraction(cache_directory=tmp_path)

    printed_output = capsys.readouterr().out
    assert expected_message_fragment in printed_output
    assert "Extraction has been stopped." in printed_output
    assert observed_stop_file_states == [False, True]  # The stop file is present while the processes wind down
    assert stop_file_path.exists() is False


@pytest.mark.ai_generated
def test_stop_extraction_times_out(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
) -> None:
    """Processes that never exit should leave the stop file in place and report a timeout."""
    slept_durations = []
    monkeypatch.setattr("s3_log_extraction.extractors._stop.get_running_pids", lambda: {"101"})
    monkeypatch.setattr("s3_log_extraction.extractors._stop.time.sleep", slept_durations.append)

    stop_extraction(cache_directory=tmp_path, max_timeout_in_seconds=15)

    assert "timed out" in capsys.readouterr().out
    assert slept_durations == [5, 5, 5]
    assert (tmp_path / "extraction" / _STOP_FILE_NAME).exists() is True
