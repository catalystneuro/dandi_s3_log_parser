"""Tests for extraction with IP encryption enabled, which merges through a decrypt/re-encrypt round trip."""

import pathlib
import secrets
import shutil

import pytest

from s3_log_extraction.extractors import S3LogAccessExtractor
from s3_log_extraction.ip_utils import MappingRegionResolver
from s3_log_extraction.utils import get_ip_stats, read_text_from_file

_EXAMPLE_LOGS_DIRECTORY = pathlib.Path(__file__).parent / "example_logs"
_STRONG_PASSWORD = secrets.token_urlsafe(32)


@pytest.fixture(autouse=True)
def strong_password(monkeypatch: pytest.MonkeyPatch) -> None:
    """The encryption key derivation rejects weak passwords, so use a randomly generated secret."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", _STRONG_PASSWORD)


def _read_extracted_ips(extraction_directory: pathlib.Path, *, use_encryption: bool) -> list[str]:
    """Collect every IP address recorded across the `ips.txt` files of an extraction directory."""
    return [
        stripped
        for file_path in sorted(extraction_directory.rglob(pattern="ips.txt"))
        for line in read_text_from_file(file_path=file_path, use_encryption=use_encryption).splitlines()
        if (stripped := line.strip())
    ]


@pytest.mark.ai_generated
def test_encrypted_extraction_matches_plaintext_extraction(tmp_path: pathlib.Path) -> None:
    """Encrypting the extracted IPs should not change which IPs are recorded."""
    plaintext_cache_directory = tmp_path / "plaintext"
    plaintext_cache_directory.mkdir()
    encrypted_cache_directory = tmp_path / "encrypted"
    encrypted_cache_directory.mkdir()

    plaintext_extractor = S3LogAccessExtractor(cache_directory=plaintext_cache_directory, use_encryption=False)
    plaintext_extractor.extract_directory(directory=_EXAMPLE_LOGS_DIRECTORY, limit=1, workers=1)
    encrypted_extractor = S3LogAccessExtractor(cache_directory=encrypted_cache_directory, use_encryption=True)
    encrypted_extractor.extract_directory(directory=_EXAMPLE_LOGS_DIRECTORY, limit=1, workers=1)

    plaintext_ips = _read_extracted_ips(plaintext_extractor.extraction_directory, use_encryption=False)
    decrypted_ips = _read_extracted_ips(encrypted_extractor.extraction_directory, use_encryption=True)

    assert len(plaintext_ips) > 0
    assert sorted(decrypted_ips) == sorted(plaintext_ips)


@pytest.mark.ai_generated
def test_encrypted_extraction_does_not_store_plaintext_ips(tmp_path: pathlib.Path) -> None:
    """The `ips.txt` files on disk should not contain any readable IP address."""
    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=True)
    extractor.extract_directory(directory=_EXAMPLE_LOGS_DIRECTORY, limit=1, workers=1)

    decrypted_ips = set(_read_extracted_ips(extractor.extraction_directory, use_encryption=True))
    raw_contents = b"".join(file_path.read_bytes() for file_path in extractor.extraction_directory.rglob("ips.txt"))

    assert len(decrypted_ips) > 0
    for ip_address in decrypted_ips:
        assert ip_address.encode() not in raw_contents


@pytest.mark.ai_generated
def test_encrypted_extraction_accumulates_across_calls(tmp_path: pathlib.Path) -> None:
    """Extracting one log at a time should merge into the same encrypted files as extracting them all at once."""
    # A pair of logs, so that the second extraction merges into an already encrypted destination
    log_directory = tmp_path / "logs"
    log_directory.mkdir()
    for log_file in sorted(_EXAMPLE_LOGS_DIRECTORY.iterdir())[:2]:
        shutil.copyfile(src=log_file, dst=log_directory / log_file.name)

    incremental_cache_directory = tmp_path / "incremental"
    incremental_cache_directory.mkdir()
    single_pass_cache_directory = tmp_path / "single_pass"
    single_pass_cache_directory.mkdir()

    for _ in range(2):
        incremental_extractor = S3LogAccessExtractor(
            cache_directory=incremental_cache_directory, use_encryption=True
        )
        incremental_extractor.extract_directory(directory=log_directory, limit=1, workers=1)

    single_pass_extractor = S3LogAccessExtractor(cache_directory=single_pass_cache_directory, use_encryption=True)
    single_pass_extractor.extract_directory(directory=log_directory, workers=1)

    incremental_ips = _read_extracted_ips(incremental_extractor.extraction_directory, use_encryption=True)
    single_pass_ips = _read_extracted_ips(single_pass_extractor.extraction_directory, use_encryption=True)

    assert sorted(incremental_ips) == sorted(single_pass_ips)


@pytest.mark.ai_generated
def test_ip_stats_over_encrypted_extraction(
    tmp_path: pathlib.Path, mocked_region_resolver: MappingRegionResolver
) -> None:
    """The IP statistics should be readable from an encrypted extraction cache."""
    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=True)
    extractor.extract_directory(directory=_EXAMPLE_LOGS_DIRECTORY, limit=1, workers=1)

    ip_stats = get_ip_stats(
        cache_directory=tmp_path, use_encryption=True, region_resolver=mocked_region_resolver
    )

    assert ip_stats["extracted_ip_count"] == len(set(_read_extracted_ips(extractor.extraction_directory, use_encryption=True)))
