"""Tests for the ExtractionHeuristicPreValidator."""

import pathlib
import subprocess

import pytest

from s3_log_extraction.validate._extraction_heuristic_pre_validator import (
    ExtractionHeuristicPreValidator,
)


@pytest.mark.ai_generated
def test_excluded_ip_regex_defaults_to_no_exclusions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", raising=False)

    validator = ExtractionHeuristicPreValidator()
    assert validator._excluded_ip_regex == "^$"


@pytest.mark.ai_generated
def test_excluded_ip_regex_plaintext_override(monkeypatch: pytest.MonkeyPatch) -> None:
    expected_regex = "^192\\.0\\.2\\.1$"
    monkeypatch.setenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", expected_regex)

    validator = ExtractionHeuristicPreValidator()
    assert validator._excluded_ip_regex == expected_regex


@pytest.mark.ai_generated
def test_excluded_ip_regex_env_var_takes_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    expected_regex = "^198\\.51\\.100\\.2$"
    monkeypatch.setenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", expected_regex)
    monkeypatch.setenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", "true")

    validator = ExtractionHeuristicPreValidator()
    assert validator._excluded_ip_regex == expected_regex


@pytest.mark.ai_generated
def test_run_validation_passes_excluded_ip_regex_env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    expected_regex = "^198\\.51\\.100\\.2$"
    monkeypatch.setenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", expected_regex)

    captured_env: dict[str, str] = {}

    def _run_stub(
        *, args: str, shell: bool, capture_output: bool, text: bool, env: dict[str, str]
    ) -> subprocess.CompletedProcess:
        assert args.startswith("awk --file ")
        assert shell is True
        assert capture_output is True
        assert text is True
        captured_env.update(env)
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr("subprocess.run", _run_stub)

    log_path = tmp_path / "test.log"
    log_path.write_text("")

    validator = ExtractionHeuristicPreValidator()
    validator._run_validation(file_path=log_path)
    assert captured_env == {"EXCLUDED_IP_REGEX": expected_regex}


@pytest.mark.ai_generated
def test_run_validation_raises_on_aberrant_line(tmp_path: pathlib.Path) -> None:
    """A `REST.GET.OBJECT` line carrying neither HTTP/1.1 nor HTTP/1.0 should fail the heuristic pre-check."""
    log_path = tmp_path / "aberrant.log"
    log_path.write_text(
        "abc123 bucket [01/Jan/2020:00:00:00 +0000] 192.0.2.1 - REQ123 REST.GET.OBJECT test/file.dat "
        '"GET /test/file.dat HTTP/2" 200 - 10 10 "-" "TestAgent" -\n'
    )

    validator = ExtractionHeuristicPreValidator()

    with pytest.raises(RuntimeError, match="Extraction heuristic pre-check failed"):
        validator._run_validation(file_path=log_path)
