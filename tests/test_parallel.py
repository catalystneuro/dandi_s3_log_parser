"""Tests for the worker count handling used by the parallelized extractors."""

import pytest

from s3_log_extraction.utils import _handle_max_workers

_FIXED_CPU_COUNT = 8


@pytest.fixture
def fixed_cpu_count(monkeypatch: pytest.MonkeyPatch) -> int:
    """Pin the reported CPU count so that the worker arithmetic is deterministic across machines."""
    monkeypatch.setattr("s3_log_extraction.utils.parallel.os.cpu_count", lambda: _FIXED_CPU_COUNT)
    return _FIXED_CPU_COUNT


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("workers", "expected_max_workers"),
    [
        (1, 1),
        (4, 4),
        (_FIXED_CPU_COUNT, _FIXED_CPU_COUNT),
        (_FIXED_CPU_COUNT + 1, _FIXED_CPU_COUNT),  # Capped at the CPU count
        (100, _FIXED_CPU_COUNT),  # Capped at the CPU count
        (-1, _FIXED_CPU_COUNT),  # Use all CPUs
        (-2, _FIXED_CPU_COUNT - 1),  # Use all but one CPU
        (-3, _FIXED_CPU_COUNT - 2),
    ],
)
def test_handle_max_workers(fixed_cpu_count: int, workers: int, expected_max_workers: int) -> None:
    """Positive values cap at the CPU count and negative values reserve `|workers| - 1` CPUs."""
    assert _handle_max_workers(workers=workers) == expected_max_workers


@pytest.mark.ai_generated
def test_handle_max_workers_zero_warns_and_falls_back(fixed_cpu_count: int) -> None:
    """Zero workers is meaningless, so it should warn and fall back to the default of -2."""
    with pytest.warns(UserWarning, match="The number of workers cannot be 0"):
        max_workers = _handle_max_workers(workers=0)

    assert max_workers == _handle_max_workers(workers=-2)


@pytest.mark.ai_generated
def test_handle_max_workers_unknown_cpu_count(monkeypatch: pytest.MonkeyPatch) -> None:
    """A platform that cannot report its CPU count should be treated as having a single CPU."""
    monkeypatch.setattr("s3_log_extraction.utils.parallel.os.cpu_count", lambda: None)

    assert _handle_max_workers(workers=4) == 1
    assert _handle_max_workers(workers=-1) == 1
