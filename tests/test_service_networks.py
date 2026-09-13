"""Tests for fetching the published IP ranges of the known cloud services and VPN listings."""

import pytest
import requests

import s3_log_extraction
from s3_log_extraction.ip_utils import fetch_service_networks

_GITHUB_META = {"hooks": ["192.0.2.0/24"], "domains": {"website": ["*.github.com"]}}
_AWS_RANGES = {
    "prefixes": [
        {"ip_prefix": "198.51.100.0/24", "region": "us-east-1"},
        {"ip_prefix": "198.51.100.0/25"},  # No region reported
    ]
}
_GCP_RANGES = {
    "prefixes": [
        {"ipv4Prefix": "203.0.113.0/24", "scope": "us-central1"},
        {"ipv6Prefix": "2001:db8::/32"},  # IPv6 is not handled
    ]
}
_VPN_RANGES = "192.0.2.128/25\n198.51.100.128/25\n"

_URL_TO_PAYLOAD = {
    "https://api.github.com/meta": _GITHUB_META,
    "https://ip-ranges.amazonaws.com/ip-ranges.json": _AWS_RANGES,
    "https://www.gstatic.com/ipranges/cloud.json": _GCP_RANGES,
    "https://raw.githubusercontent.com/josephrocca/is-vpn/main/vpn-or-datacenter-ipv4-ranges.txt": _VPN_RANGES,
}


class _FakeResponse:
    """A stand-in for `requests.Response` exposing only what the range fetching reads."""

    def __init__(self, *, payload: dict | str) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload

    @property
    def content(self) -> bytes:
        return self._payload.encode(encoding="utf-8")


@pytest.fixture
def mocked_service_listings(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Serve the published listings from fixtures rather than over the network, and report the URLs requested."""
    requested_urls = []
    s3_log_extraction.ip_utils._ip_utils._request_cidr_range.cache_clear()
    s3_log_extraction.ip_utils._ip_utils._get_cidr_address_ranges_and_subregions.cache_clear()

    def _fake_get(url: str) -> _FakeResponse:
        requested_urls.append(url)
        return _FakeResponse(payload=_URL_TO_PAYLOAD[url])

    monkeypatch.setattr(requests, "get", _fake_get)
    yield requested_urls

    s3_log_extraction.ip_utils._ip_utils._request_cidr_range.cache_clear()
    s3_log_extraction.ip_utils._ip_utils._get_cidr_address_ranges_and_subregions.cache_clear()


@pytest.mark.ai_generated
def test_fetch_service_networks_covers_every_known_service(mocked_service_listings: list[str]) -> None:
    """Every known service should be fetched from its own published endpoint."""
    service_networks = fetch_service_networks()

    assert sorted(service_networks.keys()) == ["AWS", "GCP", "GitHub", "VPN"]
    assert sorted(mocked_service_listings) == sorted(_URL_TO_PAYLOAD.keys())


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("service_name", "expected_networks"),
    [
        ("GitHub", [("192.0.2.0/24", None)]),
        ("AWS", [("198.51.100.0/24", "us-east-1"), ("198.51.100.0/25", None)]),
        ("GCP", [("203.0.113.0/24", "us-central1")]),
        ("VPN", [("192.0.2.128/25", None), ("198.51.100.128/25", None)]),
    ],
)
def test_fetch_service_networks_parses_each_listing(
    mocked_service_listings: list[str], service_name: str, expected_networks: list[tuple[str, str | None]]
) -> None:
    """Each listing has its own shape, and the subregion is carried along where the service reports one."""
    service_networks = fetch_service_networks()

    assert service_networks[service_name] == expected_networks


@pytest.mark.ai_generated
def test_fetch_service_networks_is_cached(mocked_service_listings: list[str]) -> None:
    """The published listings should be requested only once per session."""
    fetch_service_networks()
    fetch_service_networks()

    assert len(mocked_service_listings) == len(_URL_TO_PAYLOAD)
