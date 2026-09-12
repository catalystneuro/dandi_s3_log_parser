"""Remote integration tests for IP geolocation.

These tests require real credentials and live network access.
They are marked ``@pytest.mark.remote`` and are run only in the dedicated
remote-testing CI workflow, which supplies valid ``MAXMIND_ACCOUNT_ID`` and
``MAXMIND_LICENSE_KEY`` environment variables.
"""

import os
import pathlib
import re

import pytest
import yaml

import s3_log_extraction

_AUTH_ERROR_PATTERNS = ("401", "403", "Unknown token", "Unauthorized", "not authorized")

# ISO 3166-1 alpha-3 country code, optionally followed by an ISO 3166-2 subdivision code
_REGION_LABEL_PATTERN = re.compile(r"^[A-Z]{3}(/[A-Z0-9]{1,3})?$")


def _is_auth_error(exc: Exception) -> bool:
    """Return True if *exc* looks like an API authentication/authorization failure."""
    exc_str = str(exc).lower()
    return any(pattern.lower() in exc_str for pattern in _AUTH_ERROR_PATTERNS)


def _assert_maxmind_credentials_are_set() -> None:
    for name in ("MAXMIND_ACCOUNT_ID", "MAXMIND_LICENSE_KEY"):
        assert os.environ.get(name, "").strip(), f"{name} environment variable must be set to a non-empty value"


def _fail_if_maxmind_rejected(exc: Exception) -> None:
    if _is_auth_error(exc):
        pytest.fail(
            f"MAXMIND_ACCOUNT_ID and MAXMIND_LICENSE_KEY are set but were rejected by MaxMind ({exc}). "
            "Please verify that the GitHub secrets contain a valid account ID and a license key with GeoLite2 "
            "download permission from https://www.maxmind.com/en/accounts/current/license-key"
        )


def _write_by_region_summary(summary_file_path: pathlib.Path, regions: list[str]) -> None:
    """Write a minimal published by-region summary listing the given region labels."""
    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    rows = "\n".join(f"{region}\t1\t1\t0\t1" for region in regions)
    summary_file_path.write_text(
        f"region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\tnumber_of_views\n{rows}\n"
    )


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
        _fail_if_maxmind_rejected(exc)
        raise

    assert database_path == s3_log_extraction.ip_utils.get_geolite2_database_path(cache_directory=tmp_path)
    assert database_path.exists(), "GeoLite2-City.mmdb was not downloaded"
    assert database_path.stat().st_size > 1_000_000, "GeoLite2-City.mmdb is implausibly small"


@pytest.mark.remote
@pytest.mark.ai_generated
def test_resolver_resolves_public_ip_remote(tmp_path: pathlib.Path) -> None:
    """
    Test that the resolver classifies a real public IP via the live service listings and a freshly downloaded database.

    Uses ``4.4.4.4`` (Level3/Lumen Technologies), a major US-ISP address that is
    outside GitHub, AWS, GCP, and VPN CIDR ranges, to exercise the database lookup path.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory for test isolation.
    """
    test_ip = "4.4.4.4"

    _assert_maxmind_credentials_are_set()

    try:
        with s3_log_extraction.ip_utils.IpRegionResolver(cache_directory=tmp_path) as resolver:
            region = resolver.resolve(test_ip)
            # The live listings must have been fetched for every known service
            assert set(resolver.service_networks.keys()) == {"GH-actions", "GitHub", "AWS", "GCP", "VPN"}
            assert all(len(networks) > 0 for networks in resolver.service_networks.values())
    except Exception as exc:
        _fail_if_maxmind_rejected(exc)
        raise

    assert isinstance(region, str) and _REGION_LABEL_PATTERN.match(
        region
    ), f"Expected an ISO 3166 label such as 'USA/CA', got: {region!r}"
    assert (tmp_path / "geolite2" / "GeoLite2-City.mmdb").exists(), "The database was not downloaded on first use"

    # The resolved label must also have coordinates in the bundled tables, so that the heat maps can place it
    _write_by_region_summary(tmp_path / "summaries" / "ds001" / "by_region.tsv", regions=[region])
    s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)
    coordinates = yaml.safe_load((tmp_path / "ips" / "region_codes_to_coordinates.yaml").read_text()) or {}
    assert region in coordinates, f"Expected '{region}' to have coordinates, got keys: {list(coordinates.keys())}"
    assert isinstance(coordinates[region]["latitude"], float) and isinstance(coordinates[region]["longitude"], float)


@pytest.mark.remote
@pytest.mark.ai_generated
def test_update_region_code_coordinates_locates_aws_region_remote(tmp_path: pathlib.Path) -> None:
    """
    Test that a cloud service region is located with the GeoLite2 database and the live AWS IP range listing.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory for test isolation.
    """
    region_code = "AWS/us-east-1"

    _assert_maxmind_credentials_are_set()

    _write_by_region_summary(tmp_path / "summaries" / "ds001" / "by_region.tsv", regions=[region_code])

    try:
        s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)
    except Exception as exc:
        _fail_if_maxmind_rejected(exc)
        raise

    coordinates = yaml.safe_load((tmp_path / "ips" / "region_codes_to_coordinates.yaml").read_text()) or {}
    assert region_code in coordinates, f"Expected '{region_code}' to be located, got keys: {list(coordinates.keys())}"
    entry = coordinates[region_code]
    assert isinstance(entry["latitude"], float), f"Expected float latitude, got: {entry['latitude']!r}"
    assert isinstance(entry["longitude"], float), f"Expected float longitude, got: {entry['longitude']!r}"
    # us-east-1 is in Northern Virginia
    assert 24.0 < entry["latitude"] < 50.0 and -125.0 < entry["longitude"] < -66.0, entry
