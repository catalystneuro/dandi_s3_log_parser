import datetime
import io
import pathlib
import shutil
import tarfile
import unittest.mock

import geoip2.errors
import opencage.geocoder
import py
import pytest
import yaml

import s3_log_extraction
from s3_log_extraction.ip_utils._geolite2 import (
    GEOLITE2_DATABASE_FILE_NAME,
    GEOLITE2_MAX_DATABASE_AGE_IN_DAYS,
    _is_database_stale,
    open_geolite2_database,
    update_geolite2_database,
)
from s3_log_extraction.ip_utils._update_ip_to_region_codes import _get_region_code_from_ip_address
from s3_log_extraction.ip_utils._update_region_code_coordinates import _get_opencage_query


def _make_city_response(
    *,
    country_code: str | None = None,
    subdivision_codes: tuple[str, ...] = (),
    latitude: float | None = None,
    longitude: float | None = None,
) -> unittest.mock.MagicMock:
    """Build a stand-in for a ``geoip2.models.City`` response with only the attributes the code reads."""
    response = unittest.mock.MagicMock()
    response.country.iso_code = country_code
    response.subdivisions = [unittest.mock.MagicMock(iso_code=code) for code in subdivision_codes]
    response.location.latitude = latitude
    response.location.longitude = longitude
    return response


def _make_reader(city_responses: dict[str, object]) -> unittest.mock.MagicMock:
    """Build a stand-in for a ``geoip2.database.Reader`` that also works as a context manager."""

    def city(ip_address: str) -> object:
        response = city_responses[ip_address]
        if isinstance(response, Exception):
            raise response
        return response

    reader = unittest.mock.MagicMock()
    reader.city.side_effect = city
    reader.__enter__.return_value = reader
    return reader


def _fixed_refresh_date() -> datetime.date:
    """A date whose ordinal is a multiple of the 90-day refresh cycle, so that partition 0 is selected."""
    fixed_ordinal_base = datetime.date(2000, 1, 1).toordinal()
    offset = (-fixed_ordinal_base) % 90
    fixed_date = datetime.date.fromordinal(fixed_ordinal_base + offset)
    assert fixed_date.toordinal() % 90 == 0
    return fixed_date


def test_ip_utils(tmpdir: py.path.local, monkeypatch: pytest.MonkeyPatch) -> None:
    test_cache = pathlib.Path(tmpdir)
    test_ips_dir = test_cache / "ips"

    base_tests_dir = pathlib.Path(__file__).parent
    expected_cache = base_tests_dir / "expected_output"
    expected_ips_dir = expected_cache / "ips"

    # Provide a non-None dummy key so the guard check in the coordinates update passes without real credentials.
    # No actual API calls are made because the ips cache is pre-seeded with expected output below.
    monkeypatch.setenv("OPENCAGE_API_KEY", "test-key-non-remote")
    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("MAXMIND_LICENSE_KEY", raising=False)

    # Pre-seed the ips cache with expected output so the update functions see a complete cache and skip lookups.
    # With all IPs already present in ip_to_region.yaml, ips_to_update will be empty and the database is not opened.
    shutil.copytree(src=expected_ips_dir, dst=test_ips_dir, dirs_exist_ok=True)

    # Test updating IPs to region codes and coordinates
    s3_log_extraction.ip_utils.update_ip_to_region_codes(cache_directory=test_cache, use_encryption=False)
    s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=test_cache, use_encryption=False)
    s3_log_extraction.testing.assert_filetree_matches(test_dir=test_ips_dir, expected_dir=expected_ips_dir)


def test_refresh_ip_to_region_codes(tmpdir: py.path.local) -> None:
    """Test that refresh_ip_to_region_codes selects the correct IP partition and records changes."""
    test_cache = pathlib.Path(tmpdir)
    test_ips_dir = test_cache / "ips"
    test_ips_dir.mkdir(parents=True)

    # Seed the cache with known IPs and regions using RFC 5737 TEST-NET-1 documentation addresses (bogons)
    initial_ip_to_region = {
        "192.0.2.1": "US/CA",
        "192.0.2.2": "US/NY",
        "192.0.2.3": "US/TX",
        "192.0.2.4": "GB/ENG",
        "192.0.2.5": "DE/BY",
    }
    ip_to_region_file = test_ips_dir / "ip_to_region.yaml"
    ip_to_region_file.write_text(yaml.dump(initial_ip_to_region))

    fixed_date = _fixed_refresh_date()

    # With 5 IPs and partition_size = ceil(5/90) = 1, partition_index = 0 picks sorted_ips[0:1]
    sorted_ips = sorted(initial_ip_to_region.keys())
    expected_refreshed_ip = sorted_ips[0]  # "192.0.2.1"
    new_region_for_refreshed_ip = "US/OR"

    def mock_get_region_code(ip_address: str, geolite2_reader: object) -> str:
        if ip_address == expected_refreshed_ip:
            return new_region_for_refreshed_ip
        return initial_ip_to_region[ip_address]  # pragma: no cover

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._refresh_ip_to_region_codes._get_region_code_from_ip_address",
        mock_get_region_code,
    ):
        with unittest.mock.patch(
            "s3_log_extraction.ip_utils._refresh_ip_to_region_codes.open_geolite2_database",
            return_value=_make_reader(city_responses={}),
        ):
            s3_log_extraction.ip_utils.refresh_ip_to_region_codes(
                cache_directory=test_cache,
                use_encryption=False,
                _today=fixed_date,
            )

    # The cache should have the updated region for the refreshed IP
    updated_ip_to_region = yaml.safe_load(ip_to_region_file.read_text()) or {}
    assert updated_ip_to_region[expected_refreshed_ip] == new_region_for_refreshed_ip
    # All other IPs should remain unchanged
    for ip, region in initial_ip_to_region.items():
        if ip != expected_refreshed_ip:
            assert updated_ip_to_region[ip] == region

    # A log file should exist under cache/logs/
    logs_dir = test_cache / "logs"
    log_file = logs_dir / f"ip_refresh_{fixed_date.isoformat()}.yaml"
    assert log_file.exists(), f"Log file not found: {log_file}"

    log_data = yaml.safe_load(log_file.read_text()) or {}
    assert log_data["date"] == fixed_date.isoformat()
    assert log_data["partition_index"] == 0
    assert log_data["ips_checked"] == 1
    assert expected_refreshed_ip in log_data["changes"]
    assert log_data["changes"][expected_refreshed_ip]["old"] == "US/CA"
    assert log_data["changes"][expected_refreshed_ip]["new"] == new_region_for_refreshed_ip


def test_refresh_ip_to_region_codes_no_changes(tmpdir: py.path.local) -> None:
    """Test that no log file is written when no regions have changed."""
    test_cache = pathlib.Path(tmpdir)
    test_ips_dir = test_cache / "ips"
    test_ips_dir.mkdir(parents=True)

    initial_ip_to_region = {"192.0.2.1": "US/CA"}
    ip_to_region_file = test_ips_dir / "ip_to_region.yaml"
    ip_to_region_file.write_text(yaml.dump(initial_ip_to_region))

    def mock_get_region_code_unchanged(ip_address: str, geolite2_reader: object) -> str:
        return initial_ip_to_region[ip_address]

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._refresh_ip_to_region_codes._get_region_code_from_ip_address",
        mock_get_region_code_unchanged,
    ):
        with unittest.mock.patch(
            "s3_log_extraction.ip_utils._refresh_ip_to_region_codes.open_geolite2_database",
            return_value=_make_reader(city_responses={}),
        ):
            s3_log_extraction.ip_utils.refresh_ip_to_region_codes(
                cache_directory=test_cache,
                use_encryption=False,
                _today=_fixed_refresh_date(),
            )

    # Cache should be unchanged
    updated_ip_to_region = yaml.safe_load(ip_to_region_file.read_text()) or {}
    assert updated_ip_to_region == initial_ip_to_region

    # No log file should be written when there are no changes
    logs_dir = test_cache / "logs"
    assert not logs_dir.exists() or not any(logs_dir.iterdir())


def test_refresh_ip_to_region_codes_empty_cache(tmpdir: py.path.local, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that refresh_ip_to_region_codes returns early when the cache is empty."""
    test_cache = pathlib.Path(tmpdir)
    test_ips_dir = test_cache / "ips"
    test_ips_dir.mkdir(parents=True)
    (test_ips_dir / "ip_to_region.yaml").write_text("")

    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("MAXMIND_LICENSE_KEY", raising=False)

    # Should not raise (the database is never opened) and should not write any log files
    s3_log_extraction.ip_utils.refresh_ip_to_region_codes(cache_directory=test_cache, use_encryption=False)

    logs_dir = test_cache / "logs"
    assert not logs_dir.exists() or not any(logs_dir.iterdir())


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("country_code", "subdivision_codes", "expected_region"),
    [
        ("US", ("CA",), "US/CA"),
        ("GB", ("ENG", "WSM"), "GB/ENG"),  # The first-level subdivision is used, not the most specific
        ("US", (), "US"),
        (None, ("CA",), None),
        (None, (), None),
    ],
)
def test_get_region_code_from_geolite2_response(
    country_code: str | None, subdivision_codes: tuple[str, ...], expected_region: str | None
) -> None:
    """Each combination of country and subdivisions in the GeoLite2 response maps to the expected region code."""
    test_ip = "8.8.8.8"
    reader = _make_reader(
        city_responses={test_ip: _make_city_response(country_code=country_code, subdivision_codes=subdivision_codes)}
    )

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._update_ip_to_region_codes._get_cidr_address_ranges_and_subregions",
        return_value=[],
    ):
        region = _get_region_code_from_ip_address(ip_address=test_ip, geolite2_reader=reader)

    assert region == expected_region


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("ip_address", "expected_region"),
    [
        ("192.0.2.1", "bogon"),  # RFC 5737 documentation range
        ("10.0.0.1", "bogon"),  # Private
        ("127.0.0.1", "bogon"),  # Loopback
        ("not-an-ip", None),
    ],
)
def test_get_region_code_skips_database_for_non_global_addresses(ip_address: str, expected_region: str | None) -> None:
    """Non-routable and malformed addresses are classified without consulting the database."""
    reader = _make_reader(city_responses={})

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._update_ip_to_region_codes._get_cidr_address_ranges_and_subregions",
        return_value=[],
    ):
        region = _get_region_code_from_ip_address(ip_address=ip_address, geolite2_reader=reader)

    assert region == expected_region
    reader.city.assert_not_called()


@pytest.mark.ai_generated
def test_get_region_code_address_not_in_database() -> None:
    """A public address the database does not know is left unresolved rather than raising."""
    test_ip = "8.8.8.8"
    reader = _make_reader(city_responses={test_ip: geoip2.errors.AddressNotFoundError("not found")})

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._update_ip_to_region_codes._get_cidr_address_ranges_and_subregions",
        return_value=[],
    ):
        region = _get_region_code_from_ip_address(ip_address=test_ip, geolite2_reader=reader)

    assert region is None


@pytest.mark.ai_generated
def test_get_region_code_known_service_takes_precedence() -> None:
    """An address inside a known service CIDR is labeled by the service and never geolocated."""
    test_ip = "203.0.113.7"
    reader = _make_reader(city_responses={})

    def cidr_ranges(*, service_name: str) -> list[tuple[str, str | None]]:
        return [("203.0.113.0/24", "us-east-1")] if service_name == "AWS" else []

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._update_ip_to_region_codes._get_cidr_address_ranges_and_subregions",
        side_effect=cidr_ranges,
    ):
        region = _get_region_code_from_ip_address(ip_address=test_ip, geolite2_reader=reader)

    assert region == "AWS/us-east-1"
    reader.city.assert_not_called()


@pytest.mark.ai_generated
def test_update_ip_to_region_codes_resolves_new_ips(tmp_path: pathlib.Path) -> None:
    """New IPs from the extraction cache are resolved with the database and written to the cache."""
    extraction_dir = tmp_path / "extraction" / "test_dataset" / "test_asset"
    extraction_dir.mkdir(parents=True)
    test_ips = ["8.8.8.8", "1.1.1.1", "192.0.2.1"]
    (extraction_dir / "ips.txt").write_text("\n".join(test_ips))

    reader = _make_reader(
        city_responses={
            "8.8.8.8": _make_city_response(country_code="US", subdivision_codes=("CA",)),
            "1.1.1.1": _make_city_response(country_code="AU"),
        }
    )

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._update_ip_to_region_codes.open_geolite2_database", return_value=reader
    ) as mock_open:
        with unittest.mock.patch(
            "s3_log_extraction.ip_utils._update_ip_to_region_codes._get_cidr_address_ranges_and_subregions",
            return_value=[],
        ):
            s3_log_extraction.ip_utils.update_ip_to_region_codes(cache_directory=tmp_path, use_encryption=False)

    ip_to_region_file = tmp_path / "ips" / "ip_to_region.yaml"
    ip_to_region = yaml.safe_load(ip_to_region_file.read_text()) or {}
    assert ip_to_region == {"8.8.8.8": "US/CA", "1.1.1.1": "AU", "192.0.2.1": "bogon"}
    mock_open.assert_called_once()

    # A second run with nothing new must not open the database again
    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._update_ip_to_region_codes.open_geolite2_database", return_value=reader
    ) as mock_open:
        s3_log_extraction.ip_utils.update_ip_to_region_codes(cache_directory=tmp_path, use_encryption=False)
    mock_open.assert_not_called()


@pytest.mark.ai_generated
def test_update_ip_to_region_codes_batch_limit(tmp_path: pathlib.Path) -> None:
    """A batch limit caps how many IPs are resolved in one run and leaves the rest for the next."""
    extraction_dir = tmp_path / "extraction" / "test_dataset" / "test_asset"
    extraction_dir.mkdir(parents=True)
    test_ips = [f"8.8.8.{index}" for index in range(1, 6)]
    (extraction_dir / "ips.txt").write_text("\n".join(test_ips))

    reader = _make_reader(city_responses={ip: _make_city_response(country_code="US") for ip in test_ips})

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._update_ip_to_region_codes.open_geolite2_database", return_value=reader
    ):
        with unittest.mock.patch(
            "s3_log_extraction.ip_utils._update_ip_to_region_codes._get_cidr_address_ranges_and_subregions",
            return_value=[],
        ):
            s3_log_extraction.ip_utils.update_ip_to_region_codes(
                batch_size=2, batch_limit=1, cache_directory=tmp_path, use_encryption=False
            )

    ip_to_region_file = tmp_path / "ips" / "ip_to_region.yaml"
    ip_to_region = yaml.safe_load(ip_to_region_file.read_text()) or {}
    assert len(ip_to_region) == 2
    assert set(ip_to_region.values()) == {"US"}


@pytest.mark.ai_generated
def test_update_geolite2_database_requires_credentials(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a cached database and without credentials, the update explains which variables to set."""
    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("MAXMIND_LICENSE_KEY", raising=False)

    with pytest.raises(ValueError, match="MAXMIND_ACCOUNT_ID and MAXMIND_LICENSE_KEY"):
        update_geolite2_database(cache_directory=tmp_path)


@pytest.mark.ai_generated
def test_update_geolite2_database_keeps_fresh_copy(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A recently downloaded database is reused without credentials and without any network access."""
    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("MAXMIND_LICENSE_KEY", raising=False)

    database_path = tmp_path / "geolite2" / GEOLITE2_DATABASE_FILE_NAME
    database_path.parent.mkdir(parents=True)
    database_path.write_bytes(b"fresh")

    with unittest.mock.patch("requests.get") as mock_get:
        assert update_geolite2_database(cache_directory=tmp_path) == database_path
    mock_get.assert_not_called()
    assert database_path.read_bytes() == b"fresh"


@pytest.mark.ai_generated
def test_is_database_stale(tmp_path: pathlib.Path) -> None:
    database_path = tmp_path / GEOLITE2_DATABASE_FILE_NAME
    database_path.write_bytes(b"")
    modified = datetime.datetime.fromtimestamp(database_path.stat().st_mtime, tz=datetime.timezone.utc)

    just_under = modified + datetime.timedelta(days=GEOLITE2_MAX_DATABASE_AGE_IN_DAYS, seconds=-1)
    just_over = modified + datetime.timedelta(days=GEOLITE2_MAX_DATABASE_AGE_IN_DAYS, seconds=1)
    assert _is_database_stale(database_path=database_path, _now=just_under) is False
    assert _is_database_stale(database_path=database_path, _now=just_over) is True


def _make_geolite2_archive(database_content: bytes) -> bytes:
    """Build a tarball laid out like MaxMind's: a dated directory containing the .mmdb and license files."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, content in [
            ("GeoLite2-City_20260901/LICENSE.txt", b"license"),
            (f"GeoLite2-City_20260901/{GEOLITE2_DATABASE_FILE_NAME}", database_content),
        ]:
            member = tarfile.TarInfo(name=name)
            member.size = len(content)
            archive.addfile(tarinfo=member, fileobj=io.BytesIO(content))
    return buffer.getvalue()


@pytest.mark.ai_generated
def test_update_geolite2_database_downloads_and_extracts(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The tarball from MaxMind is fetched with basic auth and only the .mmdb inside it is kept."""
    monkeypatch.setenv("MAXMIND_ACCOUNT_ID", "123456")
    monkeypatch.setenv("MAXMIND_LICENSE_KEY", "test-license-key")

    database_content = b"mmdb-bytes"
    archive_bytes = _make_geolite2_archive(database_content=database_content)

    mock_response = unittest.mock.MagicMock()
    mock_response.__enter__.return_value = mock_response
    mock_response.iter_content.return_value = [archive_bytes[:10], archive_bytes[10:]]

    with unittest.mock.patch("requests.get", return_value=mock_response) as mock_get:
        database_path = update_geolite2_database(cache_directory=tmp_path)

    mock_get.assert_called_once()
    assert mock_get.call_args.kwargs["auth"] == ("123456", "test-license-key")
    assert mock_get.call_args.kwargs["url"].startswith("https://download.maxmind.com/geoip/databases/GeoLite2-City/")
    mock_response.raise_for_status.assert_called_once()

    assert database_path == tmp_path / "geolite2" / GEOLITE2_DATABASE_FILE_NAME
    assert database_path.read_bytes() == database_content
    assert sorted(path.name for path in database_path.parent.iterdir()) == [GEOLITE2_DATABASE_FILE_NAME]


@pytest.mark.ai_generated
def test_update_geolite2_database_force_redownloads_fresh_copy(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MAXMIND_ACCOUNT_ID", "123456")
    monkeypatch.setenv("MAXMIND_LICENSE_KEY", "test-license-key")

    database_path = tmp_path / "geolite2" / GEOLITE2_DATABASE_FILE_NAME
    database_path.parent.mkdir(parents=True)
    database_path.write_bytes(b"old")

    mock_response = unittest.mock.MagicMock()
    mock_response.__enter__.return_value = mock_response
    mock_response.iter_content.return_value = [_make_geolite2_archive(database_content=b"new")]

    with unittest.mock.patch("requests.get", return_value=mock_response):
        update_geolite2_database(cache_directory=tmp_path)
        assert database_path.read_bytes() == b"old"

        update_geolite2_database(cache_directory=tmp_path, force=True)
        assert database_path.read_bytes() == b"new"


@pytest.mark.ai_generated
def test_open_geolite2_database_warns_on_stale_copy_without_credentials(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stale database that cannot be refreshed is still used, with a warning rather than a failure."""
    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("MAXMIND_LICENSE_KEY", raising=False)

    database_path = tmp_path / "geolite2" / GEOLITE2_DATABASE_FILE_NAME
    database_path.parent.mkdir(parents=True)
    database_path.write_bytes(b"stale")

    with (
        unittest.mock.patch("s3_log_extraction.ip_utils._geolite2._is_database_stale", return_value=True),
        unittest.mock.patch("geoip2.database.Reader") as mock_reader,
    ):
        with pytest.warns(RuntimeWarning, match="days old but cannot be refreshed"):
            open_geolite2_database(cache_directory=tmp_path)

    mock_reader.assert_called_once_with(fileish=database_path)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("country_and_region_code", "expected_query", "expected_country_code"),
    [
        ("US/CA", "California, United States", "us"),
        ("GB/ENG", "England, United Kingdom", "gb"),
        ("US", "United States", "us"),
        ("US/ZZ", "ZZ, United States", "us"),  # Unknown subdivision passes through under the country restriction
        ("XX/YY", "XX/YY", None),  # Unknown country: nothing to expand or restrict
    ],
)
def test_get_opencage_query(
    country_and_region_code: str, expected_query: str, expected_country_code: str | None
) -> None:
    assert _get_opencage_query(country_and_region_code) == (expected_query, expected_country_code)


@pytest.mark.ai_generated
def test_update_region_code_coordinates_geocodes_places(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Geographic labels are geocoded by name with a country restriction and cached with their coordinates."""
    test_ips_dir = tmp_path / "ips"
    test_ips_dir.mkdir(parents=True)
    (test_ips_dir / "ip_to_region.yaml").write_text(yaml.dump({"8.8.8.8": "US/CA", "192.0.2.1": "bogon"}))

    monkeypatch.setenv("OPENCAGE_API_KEY", "test-key-non-remote")

    mock_geocoder = unittest.mock.MagicMock()
    mock_geocoder.geocode.return_value = [{"geometry": {"lat": 36.7783, "lng": -119.4179}}]

    with unittest.mock.patch("opencage.geocoder.OpenCageGeocode", return_value=mock_geocoder):
        s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)

    mock_geocoder.geocode.assert_called_once_with("California, United States", countrycode="us")

    coordinates = yaml.safe_load((test_ips_dir / "region_codes_to_coordinates.yaml").read_text())
    assert coordinates["US/CA"] == {"latitude": 36.7783, "longitude": -119.4179}
    assert coordinates["bogon"] == {"latitude": None, "longitude": None}


@pytest.mark.ai_generated
def test_update_region_code_coordinates_locates_services_with_geolite2(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cloud service regions are located by geolocating an address from their CIDR range, not by geocoding."""
    test_ips_dir = tmp_path / "ips"
    test_ips_dir.mkdir(parents=True)
    (test_ips_dir / "ip_to_region.yaml").write_text(yaml.dump({"52.0.0.1": "AWS/us-west-2"}))

    monkeypatch.setenv("OPENCAGE_API_KEY", "test-key-non-remote")

    mock_geocoder = unittest.mock.MagicMock()
    reader = _make_reader(city_responses={"52.0.0.0": _make_city_response(latitude=45.8399, longitude=-119.7006)})

    with unittest.mock.patch("opencage.geocoder.OpenCageGeocode", return_value=mock_geocoder):
        with unittest.mock.patch(
            "s3_log_extraction.ip_utils._update_region_code_coordinates.open_geolite2_database", return_value=reader
        ):
            with unittest.mock.patch(
                "s3_log_extraction.ip_utils._update_region_code_coordinates._get_cidr_address_ranges_and_subregions",
                return_value=[("52.0.0.0/8", "us-west-2")],
            ):
                s3_log_extraction.ip_utils.update_region_code_coordinates(
                    cache_directory=tmp_path, use_encryption=False
                )

    mock_geocoder.geocode.assert_not_called()
    reader.close.assert_called_once()

    expected_coordinates = {"latitude": 45.8399, "longitude": -119.7006}
    coordinates = yaml.safe_load((test_ips_dir / "region_codes_to_coordinates.yaml").read_text())
    assert coordinates["AWS/us-west-2"] == expected_coordinates
    service_coordinates = yaml.safe_load((test_ips_dir / "service_coordinates.yaml").read_text())
    assert service_coordinates == {"AWS/us-west-2": expected_coordinates}


@pytest.mark.ai_generated
def test_update_region_code_coordinates_handles_opencage_quota_exceeded(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """update_region_code_coordinates saves partial progress instead of crashing when the quota is exhausted."""
    test_ips_dir = tmp_path / "ips"
    test_ips_dir.mkdir(parents=True)
    (test_ips_dir / "ip_to_region.yaml").write_text(yaml.dump({"8.8.8.8": "US/CA"}))

    monkeypatch.setenv("OPENCAGE_API_KEY", "test-key-non-remote")

    mock_geocoder = unittest.mock.MagicMock()
    mock_geocoder.geocode.side_effect = opencage.geocoder.RateLimitExceededError()

    with unittest.mock.patch("opencage.geocoder.OpenCageGeocode", return_value=mock_geocoder):
        with pytest.warns(RuntimeWarning, match="quota exceeded"):
            s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)

    # The run must complete and write the (partial) coordinates cache
    coordinates_file = test_ips_dir / "region_codes_to_coordinates.yaml"
    assert coordinates_file.exists()
    assert "US/CA" not in (yaml.safe_load(coordinates_file.read_text()) or {})
