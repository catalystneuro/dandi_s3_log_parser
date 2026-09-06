import ipaddress
import itertools
import math
import pathlib
import random
import typing

import tqdm

from ._geolite2 import open_geolite2_database
from ._globals import _KNOWN_SERVICES
from ._ip_cache import load_ip_cache, write_ip_cache
from ._ip_utils import _get_cidr_address_ranges_and_subregions, _ip_in_cidr, _read_ips_from_file
from ._region_codes import country_alpha_2_to_alpha_3
from ..config import get_cache_directory


def update_ip_to_region_codes(
    batch_size: int = 1_000,
    batch_limit: int | None = None,
    cache_directory: str | pathlib.Path | None = None,
    use_encryption: bool = True,
) -> None:
    """
    Update the ``ip_to_region.yaml`` file in the cache directory.

    Every IP address found in the extraction cache that is not yet in ``ip_to_region.yaml`` is first checked
    against the CIDR ranges of known cloud services and VPNs, and otherwise geolocated with the local GeoLite2-City
    database (downloaded on demand; see ``update_geolite2_database``).

    Parameters
    ----------
    batch_size : int
        Number of IP addresses to process between writes of the cache file.
        Default is 1,000.
    batch_limit : int | None
        Maximum number of batches to process.
        If `None`, all batches will be processed.
        Default is `None`.
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
        If `None`, the default cache directory will be used.
    use_encryption : bool
        If ``True`` (default), IP data files are decrypted when reading and encrypted when writing.
        If ``False``, IP data files are read and written as plaintext.
    """
    cache_dir = pathlib.Path(cache_directory) if cache_directory is not None else get_cache_directory()
    extraction_directory = cache_dir / "extraction"
    extraction_directory.mkdir(exist_ok=True)
    all_ips: set[str] = set()
    for full_ips_file in tqdm.tqdm(
        iterable=extraction_directory.rglob(pattern="ips.txt"),
        desc="Reading IP files",
        unit=" files",
        smoothing=0,
    ):
        all_ips.update(_read_ips_from_file(file_path=full_ips_file, use_encryption=use_encryption))

    ip_to_region = load_ip_cache(
        cache_type="ip_to_region", cache_directory=cache_directory, use_encryption=use_encryption
    )
    # Skip IPs already in the cache; use the refresh command to re-check those
    ips_to_update = list(all_ips - set(ip_to_region.keys()))
    if not ips_to_update:
        return

    # If a batch limit is set, shuffle the IPs to ensure repeated runs update different IPs
    if batch_limit is not None:
        random.shuffle(ips_to_update)

    number_of_batches = math.ceil(len(ips_to_update) / batch_size)
    if batch_limit is not None:
        number_of_batches = min(number_of_batches, batch_limit)
        ips_to_update = ips_to_update[: batch_limit * batch_size]

    with open_geolite2_database(cache_directory=cache_directory) as geolite2_reader:
        for ip_batch in tqdm.tqdm(
            iterable=itertools.batched(iterable=ips_to_update, n=batch_size),
            total=number_of_batches,
            desc="Resolving IP regions in batches",
            unit="batches",
            smoothing=0,
            position=0,
            leave=False,
        ):
            for ip_address in tqdm.tqdm(
                iterable=ip_batch,
                total=batch_size,
                desc="Resolving IP regions",
                unit=" IP addresses",
                smoothing=0,
                position=1,
                leave=False,
            ):
                ip_to_region[ip_address] = _get_region_code_from_ip_address(
                    ip_address=ip_address, geolite2_reader=geolite2_reader
                )

            write_ip_cache(
                data=ip_to_region,
                cache_type="ip_to_region",
                cache_directory=cache_directory,
                use_encryption=use_encryption,
            )


def _get_region_code_from_ip_address(
    ip_address: str,
    geolite2_reader: "geoip2.database.Reader",
) -> str | typing.Literal["bogon"] | None:
    """
    Classify an IP address as a known service (e.g. ``"AWS/us-east-1"``), a bogon, or a place.

    A place is written as the ISO 3166-1 alpha-3 country code and the ISO 3166-2 subdivision code, separated by a
    slash: ``"USA/CA"`` for California, ``"GBR/ENG"`` for England. The first-level subdivision is used when the
    database knows several. Only the country code is returned when no subdivision is known, and ``None`` when
    the address is not in the database at all.
    """
    import geoip2.errors

    # Determine if the IP address belongs to GitHub, AWS, Google, or known VPNs
    # Azure not yet easily doable; keep an eye on
    # https://learn.microsoft.com/en-us/answers/questions/1410071/up-to-date-azure-public-api-to-get-azure-ip-ranges
    # maybe it will change in the future
    for service_name in _KNOWN_SERVICES:
        cidr_addresses_and_subregions = _get_cidr_address_ranges_and_subregions(service_name=service_name)

        matched_cidr_address_and_subregion = next(
            (
                (cidr_address, subregion)
                for cidr_address, subregion in cidr_addresses_and_subregions
                if _ip_in_cidr(ip_address=ip_address, cidr_address=cidr_address)
            ),
            None,
        )
        if matched_cidr_address_and_subregion is not None:
            region_service_string = service_name

            subregion = matched_cidr_address_and_subregion[1]
            if subregion is not None:
                region_service_string += f"/{subregion}"
            return region_service_string

    # Private, reserved, loopback, link-local, and documentation ranges are never routed publicly
    try:
        if not ipaddress.ip_address(address=ip_address).is_global:
            return "bogon"
    except ValueError:
        return None

    try:
        response = geolite2_reader.city(ip_address)
    except geoip2.errors.AddressNotFoundError:
        return None

    country_alpha_2 = response.country.iso_code
    subdivision_code = response.subdivisions[0].iso_code if len(response.subdivisions) > 0 else None

    match (country_alpha_2 is None, subdivision_code is None):
        case (True, _):
            region_string = None
        case (False, True):
            region_string = country_alpha_2_to_alpha_3(country_alpha_2)
        case (False, False):
            region_string = f"{country_alpha_2_to_alpha_3(country_alpha_2)}/{subdivision_code}"

    return region_string
