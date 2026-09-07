"""
Resolution of IP addresses to region labels, done on the fly while the summaries are generated.

A label is one of:

- a known service, such as ``"GitHub"``, ``"VPN"``, or a cloud region such as ``"AWS/us-east-1"``, when the address
  falls in the published ranges of that service;
- ``"bogon"`` when the address is private, reserved, or otherwise never routed publicly;
- a place, written as the ISO 3166-1 alpha-3 country code and the ISO 3166-2 subdivision code separated by a slash
  (``"USA/CA"`` for California, ``"GBR/ENG"`` for England), or the country code alone when no subdivision is known;
- ``"unknown"`` when the address is malformed, absent from the GeoLite2 database, or without a country there.

A label is never ``None``: every label must survive string handling in the summaries.
"""

import functools
import ipaddress
import pathlib
import typing
import warnings

from ._geolite2 import open_geolite2_database
from ._globals import _KNOWN_SERVICES
from ._ip_utils import _get_cidr_address_ranges_and_subregions
from ._region_codes import country_alpha_2_to_alpha_3

# The published ranges of one service: ``(cidr_address, subregion)`` pairs, where the subregion is the cloud region
# (``"us-east-1"``) when the service reports one and ``None`` otherwise
ServiceNetworks = dict[str, list[tuple[str, str | None]]]

# For one service, its IPv4 ranges grouped by prefix length, most specific first, each group keyed by the network
# address shifted down to that prefix length, so that an address is matched with one dictionary lookup per group
_PrefixTables = tuple[tuple[int, dict[int, str | None]], ...]

_IPV4_BIT_LENGTH = 32
_DEFAULT_MEMO_SIZE = 2**20


@typing.runtime_checkable
class RegionResolver(typing.Protocol):
    """Anything that maps an IP address to a region label, as the summaries need."""

    def resolve(self, ip_address: str, /) -> str: ...


def fetch_service_networks() -> ServiceNetworks:
    """Fetch the published IP ranges of every known cloud service and VPN listing (GitHub, AWS, GCP, VPN)."""
    return {
        service_name: list(_get_cidr_address_ranges_and_subregions(service_name=service_name))
        for service_name in _KNOWN_SERVICES
    }


def _freeze_service_networks(
    service_networks: ServiceNetworks,
) -> tuple[tuple[str, tuple[tuple[str, str | None], ...]], ...]:
    """Turn the service ranges into a hashable form, so that the prefix tables built from them can be cached."""
    return tuple(
        (service_name, tuple((cidr_address, subregion) for cidr_address, subregion in networks))
        for service_name, networks in service_networks.items()
    )


@functools.lru_cache(maxsize=4)
def _build_prefix_tables(
    frozen_service_networks: tuple[tuple[str, tuple[tuple[str, str | None], ...]], ...], /
) -> tuple[tuple[str, _PrefixTables], ...]:
    """
    Index the IPv4 ranges of every service by prefix length, in the order the services take precedence.

    Entries that are not valid CIDR strings are skipped with a warning, as are IPv6 ranges, which the sources do not
    report for the addresses found in S3 logs. A range with host bits set is accepted and normalized.
    """
    tables: list[tuple[str, _PrefixTables]] = []
    for service_name, networks in frozen_service_networks:
        networks_by_prefix_length: dict[int, dict[int, str | None]] = {}
        for cidr_address, subregion in networks:
            try:
                network = ipaddress.ip_network(address=cidr_address, strict=False)
            except ValueError as exception:
                warnings.warn(
                    message=f"Skipping invalid CIDR entry {cidr_address!r} of service {service_name!r}: {exception}",
                    stacklevel=2,
                )
                continue
            if network.version != 4:
                continue

            shift = _IPV4_BIT_LENGTH - network.prefixlen
            networks_by_prefix_length.setdefault(network.prefixlen, {})[
                int(network.network_address) >> shift
            ] = subregion
        prefix_tables = tuple(
            (prefix_length, networks_by_prefix_length[prefix_length])
            for prefix_length in sorted(networks_by_prefix_length.keys(), reverse=True)
        )
        tables.append((service_name, prefix_tables))
    return tuple(tables)


class IpRegionResolver:
    """
    Resolve IP addresses to region labels with the published service ranges and the local GeoLite2-City database.

    The service ranges are checked first, then whether the address is publicly routable, and only then the
    database, so a cloud provider's address is labeled by the provider and never by the datacenter's location.
    Results are memoized per instance, since one requester appears in many requests.

    The service ranges are fetched, and the database opened (downloaded first when missing or stale, see
    ``open_geolite2_database``), the first time an address needs them. Both are shared across every summary of a
    run. An instance can be pickled into worker processes: the ranges travel with it, while each process opens
    its own database reader.

    Parameters
    ----------
    cache_directory : str | pathlib.Path | None
        The cache directory holding the GeoLite2 database.
        If ``None``, the default cache directory will be used.
    service_networks : dict of str to list of tuple, optional
        The published ranges of each known service, keyed by service name, as ``(cidr_address, subregion)`` pairs.
        Fetched from the services on first use when not given.
    geolite2_reader : geoip2.database.Reader, optional
        An already opened GeoLite2-City reader to use in place of the one in the cache directory.
    """

    def __init__(
        self,
        *,
        cache_directory: str | pathlib.Path | None = None,
        service_networks: ServiceNetworks | None = None,
        geolite2_reader: "geoip2.database.Reader | None" = None,
    ) -> None:
        self._cache_directory = pathlib.Path(cache_directory) if cache_directory is not None else None
        self._service_networks = service_networks
        self._service_tables: tuple[tuple[str, _PrefixTables], ...] | None = None
        self._geolite2_reader = geolite2_reader
        self._owns_reader = geolite2_reader is None
        self._memoized_resolve = functools.lru_cache(maxsize=_DEFAULT_MEMO_SIZE)(self._resolve_without_memo)

    def __enter__(self) -> "IpRegionResolver":
        return self

    def __exit__(self, *exception_info: object) -> None:
        self.close()

    def __getstate__(self) -> dict[str, object]:
        # The ranges are fetched here so that worker processes reuse them instead of each fetching their own; the
        # database reader is a memory map and is reopened by whichever process unpickles the resolver
        return {"cache_directory": self._cache_directory, "service_networks": self.service_networks}

    def __setstate__(self, state: dict[str, object]) -> None:
        self.__init__(cache_directory=state["cache_directory"], service_networks=state["service_networks"])

    @property
    def service_networks(self) -> ServiceNetworks:
        """The published ranges of every known service, fetched on first access."""
        if self._service_networks is None:
            self._service_networks = fetch_service_networks()
        return self._service_networks

    def _get_service_tables(self) -> tuple[tuple[str, _PrefixTables], ...]:
        """The service ranges indexed for matching, built once per instance and shared between equal listings."""
        if self._service_tables is None:
            self._service_tables = _build_prefix_tables(_freeze_service_networks(self.service_networks))
        return self._service_tables

    def _get_geolite2_reader(self) -> "geoip2.database.Reader":
        if self._geolite2_reader is None:
            self._geolite2_reader = open_geolite2_database(cache_directory=self._cache_directory)
            self._owns_reader = True
        return self._geolite2_reader

    def close(self) -> None:
        """Close the GeoLite2 reader if this resolver opened it."""
        if self._geolite2_reader is not None and self._owns_reader:
            self._geolite2_reader.close()
        self._geolite2_reader = None

    def resolve(self, ip_address: str, /) -> str:
        """Return the region label of an IP address; see the module docstring for the possible labels."""
        return self._memoized_resolve(ip_address)

    def _resolve_without_memo(self, ip_address: str) -> str:
        try:
            address = ipaddress.ip_address(address=ip_address)
        except ValueError:
            return "unknown"

        service_label = self._match_service(address=address)
        if service_label is not None:
            return service_label

        # Private, reserved, loopback, link-local, and documentation ranges are never routed publicly
        if not address.is_global:
            return "bogon"

        return self._locate(ip_address=ip_address)

    def _match_service(self, *, address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str | None:
        """Return the label of the first known service whose ranges contain the address, most specific range first."""
        if address.version != 4:
            return None

        address_as_integer = int(address)
        for service_name, prefix_tables in self._get_service_tables():
            for prefix_length, networks in prefix_tables:
                key = address_as_integer >> (_IPV4_BIT_LENGTH - prefix_length)
                if key in networks:
                    subregion = networks[key]
                    return service_name if subregion is None else f"{service_name}/{subregion}"
        return None

    def _locate(self, *, ip_address: str) -> str:
        """Look a publicly routable address up in the GeoLite2 database."""
        import geoip2.errors

        try:
            response = self._get_geolite2_reader().city(ip_address)
        except geoip2.errors.AddressNotFoundError:
            return "unknown"

        country_alpha_2 = response.country.iso_code
        if country_alpha_2 is None:
            return "unknown"

        country_alpha_3 = country_alpha_2_to_alpha_3(country_alpha_2)
        subdivision_code = response.subdivisions[0].iso_code if len(response.subdivisions) > 0 else None
        return country_alpha_3 if subdivision_code is None else f"{country_alpha_3}/{subdivision_code}"


class MappingRegionResolver:
    """
    Resolve IP addresses from a fixed mapping of address to label.

    Addresses absent from the mapping are labeled ``"missing"``. This stands in for a live resolution in tests, and
    serves pipelines that resolve their requesters by other means.

    Parameters
    ----------
    ip_to_region : dict of str to str
        The label of every address to resolve.
    """

    def __init__(self, ip_to_region: dict[str, str], /) -> None:
        self._ip_to_region = dict(ip_to_region)

    def __enter__(self) -> "MappingRegionResolver":
        return self

    def __exit__(self, *exception_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Nothing to release; present so that a mapping resolver can stand wherever a live one is owned."""

    def resolve(self, ip_address: str, /) -> str:
        """Return the mapped label of an IP address, or ``"missing"`` when the address is not in the mapping."""
        return self._ip_to_region.get(ip_address) or "missing"
