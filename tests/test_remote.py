"""Remote integration tests for IP geolocation.

These tests require real credentials and live network access.
They are marked ``@pytest.mark.remote`` and are run only in the dedicated
remote-testing CI workflow, which supplies valid ``MAXMIND_ACCOUNT_ID``,
``MAXMIND_LICENSE_KEY``, and ``OPENCAGE_API_KEY`` environment variables.
"""

import os
import pathlib
import re
import warnings

import pytest
import yaml

import s3_log_extraction

_AUTH_ERROR_PATTERNS = ("401", "403", "Unknown token", "Unauthorized", "not authorized")

# ISO 3166-1 alpha-2 country code, optionally followed by an ISO 3166-2 subdivision code
_REGION_LABEL_PATTERN = re.compile(r"^[A-Z]{2}(/[A-Z0-9]{1,3})?$")


def _is_auth_error(exc: Exception) -> bool:
    """Return True if *exc* looks like an API authentication/authorization failure."""
    exc_str = str(exc).lower()
    return any(pattern.lower() in exc_str for pattern in _AUTH_ERROR_PATTERNS)


def _skip_if_quota_exceeded(captured_warnings: list[warnings.WarningMessage]) -> None:
    """Skip the current test if a quota-exceeded warning was emitted during the update call.

    The update functions halt early with a ``RuntimeWarning`` when an API quota is exhausted.
    That is an expected transient state of the shared API accounts, not a code or token failure,
    so the live-lookup assertions cannot be validated and the test is skipped.
    """
    if any("quota exceeded" in str(captured_warning.message).lower() for captured_warning in captured_warnings):
        pytest.skip("API request quota is currently exhausted; cannot validate live lookups.")


def _assert_maxmind_credentials_are_set() -> None:
    for name in ("MAXMIND_ACCOUNT_ID", "MAXMIND_LICENSE_KEY"):
        assert os.environ.get(name, "").strip(), f"{name} environment variable must be set to a non-empty value"


@pytest.mark.remote
@pytest.mark.ai_generated
def test_update_geolite2_database_remote(tmp_path: pathlib.Path) -> None:
    """
    Test that the GeoLite2-City database can be downloaded from MaxMind with the configured credentials.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory for test isolation.
    """
    _assert_maxmind_credentials_are_set()

    try:
        database_path = s3_log_extraction.ip_utils.update_geolite2_database(cache_directory=tmp_path)
    except Exception as exc:
        if _is_auth_error(exc):
            pytest.fail(
                f"MAXMIND_ACCOUNT_ID and MAXMIND_LICENSE_KEY are set but were rejected by MaxMind ({exc}). "
                "Please verify that the GitHub secrets contain a valid account ID and a license key with GeoLite2 "
                "download permission from https://www.maxmind.com/en/accounts/current/license-key"
            )
        raise

    assert database_path == s3_log_extraction.ip_utils.get_geolite2_database_path(cache_directory=tmp_path)
    assert database_path.exists(), "GeoLite2-City.mmdb was not downloaded"
    assert database_path.stat().st_size > 1_000_000, "GeoLite2-City.mmdb is implausibly small"


@pytest.mark.remote
@pytest.mark.ai_generated
def test_update_ip_to_region_codes_remote(tmp_path: pathlib.Path) -> None:
    """
    Test that update_ip_to_region_codes resolves a real public IP via a freshly downloaded GeoLite2 database.

    Uses ``4.4.4.4`` (Level3/Lumen Technologies), a major US-ISP address that is
    outside GitHub, AWS, GCP, and VPN CIDR ranges, to exercise the database lookup path.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory for test isolation.
    """
    test_ip = "4.4.4.4"

    _assert_maxmind_credentials_are_set()

    # Write the ips.txt in an extraction directory
    extraction_dir = tmp_path / "extraction" / "test_dataset" / "test_asset"
    extraction_dir.mkdir(parents=True)
    (extraction_dir / "ips.txt").write_text(test_ip)

    try:
        s3_log_extraction.ip_utils.update_ip_to_region_codes(cache_directory=tmp_path, use_encryption=False)
    except Exception as exc:
        if _is_auth_error(exc):
            pytest.fail(
                f"MAXMIND_ACCOUNT_ID and MAXMIND_LICENSE_KEY are set but were rejected by MaxMind ({exc}). "
                "Please verify that the GitHub secrets contain a valid account ID and a license key with GeoLite2 "
                "download permission from https://www.maxmind.com/en/accounts/current/license-key"
            )
        raise

    ip_cache_dir = tmp_path / "ips"
    ip_to_region_file = ip_cache_dir / "ip_to_region.yaml"
    assert ip_to_region_file.exists(), "ip_to_region.yaml was not created"

    ip_to_region = yaml.safe_load(ip_to_region_file.read_text()) or {}
    assert test_ip in ip_to_region, f"Expected IP {test_ip} to be resolved, got: {ip_to_region}"
    region = ip_to_region[test_ip]
    assert isinstance(region, str) and _REGION_LABEL_PATTERN.match(
        region
    ), f"Expected an ISO 3166 label such as 'US/CA', got: {region!r}"


@pytest.mark.remote
@pytest.mark.ai_generated
def test_update_region_code_coordinates_remote(tmp_path: pathlib.Path) -> None:
    """
    Test that update_region_code_coordinates resolves coordinates via the OpenCage API.

    Pre-populates the IP cache with a ``US/CA`` region code (not present
    in the built-in defaults) to force a live OpenCage geocoding call.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory for test isolation.
    """
    region_code = "US/CA"

    opencage_api_key = os.environ.get("OPENCAGE_API_KEY", "")
    assert opencage_api_key.strip(), "OPENCAGE_API_KEY environment variable must be set to a non-empty value"

    ip_cache_dir = tmp_path / "ips"
    ip_cache_dir.mkdir()

    # Write ip_to_region.yaml with a region that requires OpenCage lookup
    ip_to_region_file = ip_cache_dir / "ip_to_region.yaml"
    ip_to_region_file.write_text(yaml.dump({"4.4.4.4": region_code}))

    with warnings.catch_warnings(record=True) as captured_warnings:
        warnings.simplefilter("always")
        try:
            s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)
        except Exception as exc:
            if _is_auth_error(exc):
                pytest.fail(
                    f"OPENCAGE_API_KEY is set but was rejected by the OpenCage API ({exc}). "
                    "Please verify that the OPENCAGE_API_KEY GitHub secret contains a valid key "
                    "from https://opencagedata.com/dashboard#api-keys"
                )
            raise
    _skip_if_quota_exceeded(captured_warnings)

    coordinates_file = ip_cache_dir / "region_codes_to_coordinates.yaml"
    assert coordinates_file.exists(), "region_codes_to_coordinates.yaml was not created"

    coordinates = yaml.safe_load(coordinates_file.read_text()) or {}
    assert region_code in coordinates, f"Expected '{region_code}' to be geocoded, got keys: {list(coordinates.keys())}"
    entry = coordinates[region_code]
    assert (
        "latitude" in entry and "longitude" in entry
    ), f"Expected latitude/longitude keys in entry for '{region_code}', got: {entry}"
    assert isinstance(entry["latitude"], float), f"Expected float latitude, got: {entry['latitude']!r}"
    assert isinstance(entry["longitude"], float), f"Expected float longitude, got: {entry['longitude']!r}"
    # California, not somewhere else called California
    assert 32.0 < entry["latitude"] < 42.5 and -125.0 < entry["longitude"] < -114.0, entry
