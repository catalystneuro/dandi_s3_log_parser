import pathlib

import natsort
import pandas
import tqdm

from ._geolite2 import open_geolite2_database
from ._globals import _DEFAULT_REGION_CODES_TO_COORDINATES, _KNOWN_SERVICES
from ._ip_cache import load_ip_cache, write_ip_cache
from ._ip_utils import _get_cidr_address_ranges_and_subregions
from ._region_codes import get_region_coordinates
from ..config import get_cache_subdirectory


def _collect_published_region_codes(*, cache_directory: str | pathlib.Path | None = None) -> set[str]:
    """Collect every region label that appears in a published ``by_region.tsv`` summary, per dataset or archive-wide."""
    summary_directory = get_cache_subdirectory(cache_directory=cache_directory, name="summaries")

    region_codes: set[str] = set()
    for summary_file_path in summary_directory.rglob(pattern="by_region.tsv"):
        summary_table = pandas.read_table(filepath_or_buffer=summary_file_path, usecols=["region"])
        region_codes.update(str(region) for region in summary_table["region"])
    return region_codes


def update_region_code_coordinates(
    cache_directory: str | pathlib.Path | None = None,
    use_encryption: bool = True,
) -> None:
    """
    Update the `region_codes_to_coordinates.yaml` file in the cache directory.

    Every region label of the published ``by_region.tsv`` summaries that has no coordinates yet is located. Geographic
    labels (``"USA/CA"``) are looked up in the ISO 3166 coordinate tables shipped with the package, and cloud service
    regions (``"AWS/us-east-1"``) are located with the GeoLite2 database. No web API is involved, and the database is
    only opened when a cloud service region is new.

    Parameters
    ----------
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
        If `None`, the default cache directory will be used.
    use_encryption : bool
        If ``True`` (default), the coordinates file is decrypted when read and encrypted when written.
        If ``False``, it is read and written as plaintext.
    """
    region_codes_to_coordinates: dict[str, dict[str, float]] = dict(_DEFAULT_REGION_CODES_TO_COORDINATES)
    previous_region_codes_to_coordinates = load_ip_cache(
        cache_type="region_codes_to_coordinates", cache_directory=cache_directory, use_encryption=use_encryption
    )
    region_codes_to_coordinates.update(previous_region_codes_to_coordinates)

    published_region_codes = _collect_published_region_codes(cache_directory=cache_directory)
    region_codes_to_update = published_region_codes - set(region_codes_to_coordinates.keys())
    unresolved_region_codes = []
    geolite2_reader = None
    try:
        for country_and_region_code in tqdm.tqdm(
            iterable=region_codes_to_update,
            total=len(region_codes_to_update),
            desc="Updating region coordinates",
            smoothing=0,
            unit="regions",
        ):
            service_name = country_and_region_code.split("/")[0]
            if service_name in _KNOWN_SERVICES:
                if geolite2_reader is None:
                    geolite2_reader = open_geolite2_database(cache_directory=cache_directory)
                coordinates = _get_service_coordinates_from_geolite2(
                    country_and_region_code=country_and_region_code, geolite2_reader=geolite2_reader
                )
            else:
                coordinates = get_region_coordinates(country_and_region_code)

            if coordinates is not None:
                region_codes_to_coordinates[country_and_region_code] = coordinates
            else:
                unresolved_region_codes.append(country_and_region_code)
    finally:
        if geolite2_reader is not None:
            geolite2_reader.close()

    region_codes_to_coordinates_ordered = {
        key: region_codes_to_coordinates[key] for key in natsort.natsorted(seq=region_codes_to_coordinates.keys())
    }

    write_ip_cache(
        data=region_codes_to_coordinates_ordered,
        cache_type="region_codes_to_coordinates",
        cache_directory=cache_directory,
        use_encryption=use_encryption,
    )

    if any(unresolved_region_codes):
        message = (
            f"\nThe following region codes have no known coordinates:\n"
            f"{', '.join(natsort.natsorted(unresolved_region_codes))}\n\n"
        )
        print(message)


def _get_service_coordinates_from_geolite2(
    *,
    country_and_region_code: str,
    geolite2_reader: "geoip2.database.Reader",
) -> dict[str, float] | None:
    """
    Locate a cloud service region (e.g. ``"AWS/us-east-1"``) by geolocating the first IP of one of its CIDR ranges.

    A service label without a region (``"GitHub"``, ``"VPN"``) names no place and is handled by the defaults.
    """
    import geoip2.errors

    service_name, _, subregion = country_and_region_code.partition("/")
    if not subregion:
        return None

    cidr_addresses_and_subregions = _get_cidr_address_ranges_and_subregions(service_name=service_name)
    subregion_to_cidr_address = {subregion: cidr_address for cidr_address, subregion in cidr_addresses_and_subregions}
    if subregion not in subregion_to_cidr_address:
        return None

    ip_address = subregion_to_cidr_address[subregion].split("/")[0]
    try:
        location = geolite2_reader.city(ip_address).location
    except geoip2.errors.AddressNotFoundError:
        return None
    if location.latitude is None or location.longitude is None:
        return None

    return {"latitude": location.latitude, "longitude": location.longitude}
