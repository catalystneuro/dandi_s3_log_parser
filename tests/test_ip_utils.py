import datetime
import io
import pathlib
import pickle
import tarfile
import unittest.mock
import warnings

import geoip2.errors
import pytest
import yaml

import s3_log_extraction
from s3_log_extraction.ip_utils import IpRegionResolver, MappingRegionResolver, RegionResolver
from s3_log_extraction.ip_utils._geolite2 import (
    GEOLITE2_DATABASE_FILE_NAME,
    GEOLITE2_MAX_DATABASE_AGE_IN_DAYS,
    _is_database_stale,
    open_geolite2_database,
    update_geolite2_database,
)
from s3_log_extraction.ip_utils._region_codes import country_alpha_2_to_alpha_3, get_region_coordinates

# Loose bounding boxes (south, north, west, east) used to check that a coordinate lands in the right place
_CALIFORNIA_BOX = (32.0, 42.5, -125.0, -114.0)
_ENGLAND_BOX = (49.5, 56.0, -6.5, 2.0)
_BAVARIA_BOX = (47.0, 50.7, 8.9, 13.9)
_CONTIGUOUS_US_BOX = (24.0, 50.0, -125.0, -66.0)

_NO_SERVICE_NETWORKS = {"GitHub": [], "AWS": [], "GCP": [], "VPN": []}


def _assert_within(coordinates: dict[str, float], box: tuple[float, float, float, float]) -> None:
    south, north, west, east = box
    assert south < coordinates["latitude"] < north and west < coordinates["longitude"] < east, coordinates


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


def _write_by_region_summary(summary_file_path: pathlib.Path, regions: list[str]) -> None:
    """Write a minimal published by-region summary listing the given region labels."""
    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    rows = "\n".join(f"{region}\t1\t1\t0\t1" for region in regions)
    summary_file_path.write_text(
        f"region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\tnumber_of_views\n{rows}\n"
    )


# ---------------------------------------------------------------------------
# IpRegionResolver
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("country_code", "subdivision_codes", "expected_region"),
    [
        ("US", ("CA",), "USA/CA"),
        ("GB", ("ENG", "WSM"), "GBR/ENG"),  # The first-level subdivision is used, not the most specific
        ("US", (), "USA"),
        ("XK", ("01",), "XK/01"),  # Kosovo has no ISO 3166-1 alpha-3 code, so the alpha-2 code is kept
        (None, ("CA",), "unknown"),
        (None, (), "unknown"),
    ],
)
def test_resolver_maps_geolite2_response_to_region_label(
    country_code: str | None, subdivision_codes: tuple[str, ...], expected_region: str
) -> None:
    """Each combination of country and subdivisions in the GeoLite2 response maps to the expected region label."""
    test_ip = "8.8.8.8"
    reader = _make_reader(
        city_responses={test_ip: _make_city_response(country_code=country_code, subdivision_codes=subdivision_codes)}
    )
    resolver = IpRegionResolver(service_networks=_NO_SERVICE_NETWORKS, geolite2_reader=reader)

    assert resolver.resolve(test_ip) == expected_region


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("ip_address", "expected_region"),
    [
        ("192.0.2.1", "bogon"),  # RFC 5737 documentation range
        ("10.0.0.1", "bogon"),  # Private
        ("127.0.0.1", "bogon"),  # Loopback
        ("not-an-ip", "unknown"),
    ],
)
def test_resolver_skips_database_for_non_global_addresses(ip_address: str, expected_region: str) -> None:
    """Non-routable and malformed addresses are classified without consulting the database."""
    reader = _make_reader(city_responses={})
    resolver = IpRegionResolver(service_networks=_NO_SERVICE_NETWORKS, geolite2_reader=reader)

    assert resolver.resolve(ip_address) == expected_region
    reader.city.assert_not_called()


@pytest.mark.ai_generated
def test_resolver_labels_address_not_in_database_as_unknown() -> None:
    """A public address the database does not know is labeled unknown, never ``None``, rather than raising."""
    test_ip = "8.8.8.8"
    reader = _make_reader(city_responses={test_ip: geoip2.errors.AddressNotFoundError("not found")})
    resolver = IpRegionResolver(service_networks=_NO_SERVICE_NETWORKS, geolite2_reader=reader)

    assert resolver.resolve(test_ip) == "unknown"


@pytest.mark.ai_generated
def test_resolver_known_service_takes_precedence() -> None:
    """An address inside a known service range is labeled by the service and never geolocated, even a bogon."""
    reader = _make_reader(city_responses={})
    service_networks = {
        "GitHub": [("192.30.252.0/22", None)],
        "AWS": [("203.0.113.0/24", "us-east-1")],
        "GCP": [],
        "VPN": [("198.51.100.0/24", None)],
    }
    resolver = IpRegionResolver(service_networks=service_networks, geolite2_reader=reader)

    assert resolver.resolve("203.0.113.7") == "AWS/us-east-1"
    assert resolver.resolve("192.30.253.1") == "GitHub"
    assert resolver.resolve("198.51.100.9") == "VPN"
    reader.city.assert_not_called()


@pytest.mark.ai_generated
def test_resolver_service_precedence_and_specificity() -> None:
    """Services are checked in their fixed order, and within a service the most specific range wins."""
    service_networks = {
        "GitHub": [],
        "AWS": [("52.0.0.0/8", "us-east-1"), ("52.94.0.0/16", "eu-west-1")],
        "GCP": [],
        "VPN": [("52.94.1.0/24", None)],  # Also lists an AWS address, but AWS is checked first
    }
    resolver = IpRegionResolver(service_networks=service_networks, geolite2_reader=_make_reader(city_responses={}))

    assert resolver.resolve("52.1.2.3") == "AWS/us-east-1"
    assert resolver.resolve("52.94.1.1") == "AWS/eu-west-1"


@pytest.mark.ai_generated
def test_resolver_skips_invalid_service_ranges_with_a_warning() -> None:
    """A malformed entry in a service listing is skipped with a warning rather than failing every lookup."""
    service_networks = {
        "GitHub": [],
        "AWS": [("not-a-cidr", "us-east-1"), ("203.0.113.0/24", "us-east-1")],
        "GCP": [],
        "VPN": [],
    }
    resolver = IpRegionResolver(service_networks=service_networks, geolite2_reader=_make_reader(city_responses={}))

    with pytest.warns(UserWarning, match="Skipping invalid CIDR entry 'not-a-cidr'"):
        assert resolver.resolve("203.0.113.7") == "AWS/us-east-1"


@pytest.mark.ai_generated
def test_resolver_memoizes_and_opens_database_lazily(tmp_path: pathlib.Path) -> None:
    """The database is opened once, on the first address that needs it, and each address is looked up once."""
    reader = _make_reader(city_responses={"8.8.8.8": _make_city_response(country_code="US", subdivision_codes=("CA",))})

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._resolver.open_geolite2_database", return_value=reader
    ) as mock_open:
        with IpRegionResolver(cache_directory=tmp_path, service_networks=_NO_SERVICE_NETWORKS) as resolver:
            assert resolver.resolve("192.0.2.1") == "bogon"
            mock_open.assert_not_called()

            assert resolver.resolve("8.8.8.8") == "USA/CA"
            assert resolver.resolve("8.8.8.8") == "USA/CA"

    mock_open.assert_called_once_with(cache_directory=tmp_path)
    reader.city.assert_called_once_with("8.8.8.8")
    reader.close.assert_called_once()


@pytest.mark.ai_generated
def test_resolver_does_not_close_a_reader_it_was_given() -> None:
    """A reader handed in from outside stays open for its owner."""
    reader = _make_reader(city_responses={})
    with IpRegionResolver(service_networks=_NO_SERVICE_NETWORKS, geolite2_reader=reader):
        pass
    reader.close.assert_not_called()


@pytest.mark.ai_generated
def test_resolver_pickles_with_service_networks_but_without_reader(tmp_path: pathlib.Path) -> None:
    """A resolver sent to a worker process carries the service ranges and reopens the database there."""
    service_networks = {"GitHub": [], "AWS": [("203.0.113.0/24", "us-east-1")], "GCP": [], "VPN": []}
    reader = _make_reader(city_responses={})
    resolver = IpRegionResolver(cache_directory=tmp_path, service_networks=service_networks, geolite2_reader=reader)

    unpickled = pickle.loads(pickle.dumps(resolver))

    assert unpickled.service_networks == service_networks
    assert unpickled.resolve("203.0.113.7") == "AWS/us-east-1"
    assert unpickled._geolite2_reader is None
    assert unpickled._cache_directory == tmp_path


@pytest.mark.ai_generated
def test_resolver_fetches_service_networks_on_first_use() -> None:
    """Without given ranges, the published listings are fetched once, on the first address that needs them."""
    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._resolver._get_cidr_address_ranges_and_subregions",
        side_effect=lambda *, service_name: [("203.0.113.0/24", "us-east-1")] if service_name == "AWS" else [],
    ) as mock_ranges:
        resolver = IpRegionResolver(geolite2_reader=_make_reader(city_responses={}))
        mock_ranges.assert_not_called()

        assert resolver.resolve("203.0.113.7") == "AWS/us-east-1"
        assert resolver.resolve("203.0.113.8") == "AWS/us-east-1"

    assert sorted(call.kwargs["service_name"] for call in mock_ranges.call_args_list) == [
        "AWS",
        "GCP",
        "GH-actions",
        "GitHub",
        "VPN",
    ]


@pytest.mark.ai_generated
def test_github_ranges_are_recognized_by_shape_not_by_key() -> None:
    """
    Only the IPv4 ranges of GitHub's meta document are kept, whatever keys GitHub files them under.

    The document also carries SSH keys, PGP key blocks, domains, and other metadata, under keys added over time, and
    none of it may leak into the ranges or trigger the resolver's invalid-CIDR warning.
    """
    pgp_key_block = "-----BEGIN PGP PUBLIC KEY BLOCK-----\n\nABC\n-----END PGP PUBLIC KEY BLOCK-----"
    github_meta = {
        "verifiable_password_authentication": False,
        "ssh_key_fingerprints": {"SHA256_ED25519": "+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU"},
        "ssh_keys": ["ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"],
        "some_future_key_listing": [pgp_key_block],
        "some_future_numeric_listing": [42, None],
        "hooks": ["192.0.2.0/24", "2001:db8::/32"],
        "web": ["198.51.100.0/25"],
        "actions": ["203.0.113.0/24"],
        "domains": {"website": ["*.github.com"]},
        "artifact_attestations": {"trust_domain": "", "services": ["*.example.com"]},
    }
    empty_listings = {"AWS": {"prefixes": []}, "GCP": {"prefixes": []}, "VPN": []}

    ip_utils_module = s3_log_extraction.ip_utils._ip_utils
    ip_utils_module._get_cidr_address_ranges_and_subregions.cache_clear()
    try:
        with (
            unittest.mock.patch.object(
                ip_utils_module,
                "_request_cidr_range",
                side_effect=lambda service_name: (
                    github_meta if service_name in ("GitHub", "GH-actions") else empty_listings[service_name]
                ),
            ),
            warnings.catch_warnings(),
        ):
            warnings.simplefilter("error")
            service_networks = s3_log_extraction.ip_utils.fetch_service_networks()
            resolver = IpRegionResolver(
                service_networks=service_networks, geolite2_reader=_make_reader(city_responses={})
            )

            # The actions* ranges are split into the "GH-actions" service; the rest are "GitHub".
            assert service_networks["GitHub"] == [
                ("192.0.2.0/24", None),
                ("198.51.100.0/25", None),
            ]
            assert service_networks["GH-actions"] == [("203.0.113.0/24", None)]
            assert resolver.resolve("198.51.100.7") == "GitHub"
            assert resolver.resolve("203.0.113.7") == "GH-actions"
    finally:
        ip_utils_module._get_cidr_address_ranges_and_subregions.cache_clear()


@pytest.mark.ai_generated
def test_mapping_region_resolver() -> None:
    """A mapping resolver returns the mapped label and ``missing`` for anything else, and satisfies the protocol."""
    resolver = MappingRegionResolver({"192.0.2.1": "USA/CA", "192.0.2.2": None})

    assert isinstance(resolver, RegionResolver)
    assert isinstance(IpRegionResolver(service_networks=_NO_SERVICE_NETWORKS), RegionResolver)
    assert resolver.resolve("192.0.2.1") == "USA/CA"
    assert resolver.resolve("192.0.2.2") == "missing"
    assert resolver.resolve("192.0.2.3") == "missing"


# ---------------------------------------------------------------------------
# GeoLite2 database acquisition
# ---------------------------------------------------------------------------


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
def test_update_geolite2_database_reports_maxmind_rejection(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A rejected download surfaces MaxMind's own explanation alongside the HTTP status."""
    import requests

    monkeypatch.setenv("MAXMIND_ACCOUNT_ID", "123456")
    monkeypatch.setenv("MAXMIND_LICENSE_KEY", "test-license-key")

    mock_response = unittest.mock.MagicMock()
    mock_response.__enter__.return_value = mock_response
    mock_response.text = '{"code":"AUTHORIZATION_INVALID","error":"A valid license key is required."}'
    mock_response.raise_for_status.side_effect = requests.HTTPError("401 Client Error: Unauthorized for url: x")

    with unittest.mock.patch("requests.get", return_value=mock_response):
        with pytest.raises(requests.HTTPError, match=r"401 Client Error.*\nMaxMind said: .*AUTHORIZATION_INVALID"):
            update_geolite2_database(cache_directory=tmp_path)

    assert not (tmp_path / "geolite2" / GEOLITE2_DATABASE_FILE_NAME).exists()


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


# ---------------------------------------------------------------------------
# ISO 3166 tables and coordinates
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("alpha_2", "expected_alpha_3"),
    [
        ("US", "USA"),
        ("GB", "GBR"),
        ("de", "DEU"),  # Case-insensitive input
        ("XK", "XK"),  # Kosovo is outside ISO 3166-1, so there is nothing to convert to
        ("ZZ", "ZZ"),
    ],
)
def test_country_alpha_2_to_alpha_3(alpha_2: str, expected_alpha_3: str) -> None:
    assert country_alpha_2_to_alpha_3(alpha_2) == expected_alpha_3


@pytest.mark.ai_generated
def test_get_region_coordinates_known_subdivisions() -> None:
    """Subdivision points land in the right place, including first-level regions Natural Earth only maps finer."""
    _assert_within(get_region_coordinates("USA/CA"), _CALIFORNIA_BOX)
    _assert_within(get_region_coordinates("DEU/BY"), _BAVARIA_BOX)
    # England has no Natural Earth unit of its own; its point is aggregated from its counties via ISO 3166-2
    _assert_within(get_region_coordinates("GBR/ENG"), _ENGLAND_BOX)


@pytest.mark.ai_generated
def test_get_region_coordinates_country_fallbacks() -> None:
    """A bare country label and an unknown subdivision both resolve to the country's point; unknown countries do not."""
    country_coordinates = get_region_coordinates("USA")
    _assert_within(country_coordinates, _CONTIGUOUS_US_BOX)
    assert get_region_coordinates("USA/ZZ") == country_coordinates
    assert get_region_coordinates("XX/YY") is None
    assert get_region_coordinates("XX") is None


@pytest.mark.ai_generated
def test_update_region_code_coordinates_locates_published_regions(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
) -> None:
    """Every label of the published by-region summaries is located from the bundled tables; unknown ones reported."""
    _write_by_region_summary(tmp_path / "summaries" / "ds001" / "by_region.tsv", regions=["USA/CA", "bogon", "XX/YY"])
    _write_by_region_summary(tmp_path / "summaries" / "archive" / "by_region.tsv", regions=["USA/CA", "AUS", "missing"])

    with unittest.mock.patch("requests.get") as mock_get:
        s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)
    mock_get.assert_not_called()

    coordinates = yaml.safe_load((tmp_path / "ips" / "region_codes_to_coordinates.yaml").read_text())
    _assert_within(coordinates["USA/CA"], _CALIFORNIA_BOX)
    assert coordinates["AUS"] == get_region_coordinates("AUS")
    assert coordinates["bogon"] == {"latitude": None, "longitude": None}
    assert coordinates["missing"] == {"latitude": None, "longitude": None}
    assert "XX/YY" not in coordinates
    assert "XX/YY" in capsys.readouterr().out


@pytest.mark.ai_generated
def test_update_region_code_coordinates_without_summaries(tmp_path: pathlib.Path) -> None:
    """With no by-region summary published yet, only the default entries are written."""
    s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)

    coordinates = yaml.safe_load((tmp_path / "ips" / "region_codes_to_coordinates.yaml").read_text())
    assert set(coordinates.keys()) == set(s3_log_extraction.ip_utils._globals._DEFAULT_REGION_CODES_TO_COORDINATES)


@pytest.mark.ai_generated
def test_update_region_code_coordinates_locates_services_with_geolite2(tmp_path: pathlib.Path) -> None:
    """Cloud service regions are located by geolocating an address from their range, once, not from the tables."""
    _write_by_region_summary(tmp_path / "summaries" / "ds001" / "by_region.tsv", regions=["AWS/us-west-2", "GitHub"])

    reader = _make_reader(city_responses={"52.0.0.0": _make_city_response(latitude=45.8399, longitude=-119.7006)})

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._update_region_code_coordinates.open_geolite2_database", return_value=reader
    ) as mock_open:
        with unittest.mock.patch(
            "s3_log_extraction.ip_utils._update_region_code_coordinates._get_cidr_address_ranges_and_subregions",
            return_value=[("52.0.0.0/8", "us-west-2")],
        ):
            s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)

            # A second run finds the region already located and does not open the database again
            s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)

    mock_open.assert_called_once()
    reader.close.assert_called_once()

    coordinates = yaml.safe_load((tmp_path / "ips" / "region_codes_to_coordinates.yaml").read_text())
    assert coordinates["AWS/us-west-2"] == {"latitude": 45.8399, "longitude": -119.7006}
    assert coordinates["GitHub"] == {"latitude": None, "longitude": None}
