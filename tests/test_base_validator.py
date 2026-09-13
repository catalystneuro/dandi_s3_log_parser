"""Tests for the record keeping and directory traversal shared by all validators."""

import pathlib

import pytest

from s3_log_extraction.validate import BaseValidator


class _CountingValidator(BaseValidator):
    """A validator that records the files it was asked to validate and optionally fails on some of them."""

    tqdm_description = "Counting validation"

    def __init__(self, *, failing_file_names: set[str] | None = None) -> None:
        self.validated_file_paths: list[pathlib.Path] = []
        self.failing_file_names = failing_file_names or set()

        super().__init__()

    def _run_validation(self, file_path: pathlib.Path) -> None:
        self.validated_file_paths.append(file_path)
        if file_path.name in self.failing_file_names:
            message = f"Validation failed for {file_path}."
            raise RuntimeError(message)


@pytest.fixture
def isolated_records_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """Point the validator record cache at a temporary directory rather than the user's real cache."""
    records_directory = tmp_path / "records"
    records_directory.mkdir()
    monkeypatch.setattr(
        "s3_log_extraction.validate._base_validator.get_cache_subdirectory", lambda **kwargs: records_directory
    )
    return records_directory


@pytest.mark.ai_generated
def test_validate_file_writes_record(isolated_records_directory: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """A successful validation should append the absolute file path to the record file."""
    log_file = tmp_path / "example.log"
    log_file.write_text("content\n")

    validator = _CountingValidator()
    validator.validate_file(file_path=log_file)

    assert validator.validated_file_paths == [log_file]
    assert validator.record == {str(log_file.absolute())}
    assert validator.record_file_path.read_text().splitlines() == [str(log_file.absolute())]


@pytest.mark.ai_generated
def test_validate_file_skips_already_recorded(isolated_records_directory: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """A file already present in the record should not be validated a second time."""
    log_file = tmp_path / "example.log"
    log_file.write_text("content\n")

    validator = _CountingValidator()
    validator.validate_file(file_path=log_file)
    validator.validate_file(file_path=log_file)

    assert validator.validated_file_paths == [log_file]
    assert validator.record_file_path.read_text().splitlines() == [str(log_file.absolute())]


@pytest.mark.ai_generated
def test_record_is_reloaded_by_a_new_instance(isolated_records_directory: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """A fresh instance should read back the record written by a previous one and skip those files."""
    log_file = tmp_path / "example.log"
    log_file.write_text("content\n")

    _CountingValidator().validate_file(file_path=log_file)

    reloaded_validator = _CountingValidator()

    assert reloaded_validator.record == {str(log_file.absolute())}

    reloaded_validator.validate_file(file_path=log_file)

    assert reloaded_validator.validated_file_paths == []


@pytest.mark.ai_generated
def test_failed_validation_is_not_recorded(isolated_records_directory: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """A file whose validation raises should not be added to the record."""
    log_file = tmp_path / "bad.log"
    log_file.write_text("content\n")

    validator = _CountingValidator(failing_file_names={"bad.log"})
    with pytest.raises(RuntimeError, match="Validation failed"):
        validator.validate_file(file_path=log_file)

    assert validator.record == set()
    assert validator.record_file_path.exists() is False


@pytest.mark.ai_generated
def test_validate_directory_covers_all_log_files(
    isolated_records_directory: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """All `.log` files, including those in subdirectories, should be validated exactly once."""
    log_directory = tmp_path / "logs"
    nested_directory = log_directory / "nested"
    nested_directory.mkdir(parents=True)
    expected_log_files = {log_directory / "a.log", log_directory / "b.log", nested_directory / "c.log"}
    for log_file in expected_log_files:
        log_file.write_text("content\n")
    (log_directory / "not_a_log.txt").write_text("content\n")

    validator = _CountingValidator()
    validator.validate_directory(directory=log_directory)

    assert set(validator.validated_file_paths) == {pathlib.Path(str(file.absolute())) for file in expected_log_files}
    assert validator.record == {str(file.absolute()) for file in expected_log_files}


@pytest.mark.ai_generated
@pytest.mark.parametrize("limit", [1, 2])
def test_validate_directory_respects_limit(
    isolated_records_directory: pathlib.Path, tmp_path: pathlib.Path, limit: int
) -> None:
    """Only up to `limit` files should be validated per call."""
    log_directory = tmp_path / "logs"
    log_directory.mkdir()
    for index in range(3):
        (log_directory / f"{index}.log").write_text("content\n")

    validator = _CountingValidator()
    validator.validate_directory(directory=log_directory, limit=limit)

    assert len(validator.validated_file_paths) == limit


@pytest.mark.ai_generated
def test_validate_directory_resumes_from_record(
    isolated_records_directory: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Repeated calls with a limit should work through the directory without repeating files."""
    log_directory = tmp_path / "logs"
    log_directory.mkdir()
    for index in range(3):
        (log_directory / f"{index}.log").write_text("content\n")

    validator = _CountingValidator()
    validator.validate_directory(directory=log_directory, limit=2)
    validator.validate_directory(directory=log_directory, limit=2)

    assert len(validator.validated_file_paths) == 3
    assert len(set(validator.validated_file_paths)) == 3


@pytest.mark.ai_generated
def test_record_file_name_is_class_and_hash_specific(isolated_records_directory: pathlib.Path) -> None:
    """The record file name should identify both the validator class and its hash."""
    validator = _CountingValidator()

    assert validator.record_file_path.name == f"_CountingValidator_{hex(hash(validator))[2:]}.txt"
    assert validator.records_directory == isolated_records_directory


@pytest.mark.ai_generated
def test_base_validation_rule_is_not_implemented(isolated_records_directory: pathlib.Path) -> None:
    """The base class itself defines no validation rule, so deferring to it should raise."""

    class _DeferringValidator(BaseValidator):
        def _run_validation(self, file_path: pathlib.Path) -> None:
            super()._run_validation(file_path=file_path)

    with pytest.raises(NotImplementedError, match="Validation rule has not been implemented"):
        _DeferringValidator()._run_validation(file_path=pathlib.Path("example.log"))
