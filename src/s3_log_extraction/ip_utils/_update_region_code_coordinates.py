import pathlib

import natsort
import tqdm
import yaml

from ._geolite2 import open_geolite2_database
from ._globals import _DEFAULT_REGION_CODES_TO_COORDINATES, _KNOWN_SERVICES
from ._ip_cache import load_ip_cache, write_ip_cache
from ._ip_utils import _get_cidr_address_ranges_and_subregions
from ._region_codes import get_region_coordinates
from ..config import get_cache_subdirectory


def update_region_code_coordinates(
    cache_directory: str | pathlib.Path | None = None,
    use_encryption: bool = True,
) -> None:
    """
    Update the `region_codes_to_coordinates.yaml` file in the cache directory.

    Geographic labels (``"USA/CA"``) are looked up in the ISO 3166 coordinate tables shipped with the package, and
    cloud service regions (``"AWS/us-east-1"``) are located with the GeoLite2 database. No web API is involved.

    Parameters
    ----------
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
        If `None`, the default cache directory will be used.
    use_encryption : bool
        If ``True`` (default), IP cache files are decrypted when reading and encrypted when writing.
        If ``False``, IP cache files are read and written as plaintext.
    """
    ip_cache_directory = get_cache_subdirectory(cache_directory=cache_directory, name="ips")

    ip_to_region_codes_file_path = ip_cache_directory / "ip_to_region.yaml"
    if not ip_to_region_codes_file_path.exists():
        message = (
            f"\nCannot update region codes to coordinates because the IP to region file does not exist: "
            f"{ip_to_region_codes_file_path}\n\n"
            f"Please run `s3logextraction update ip regions` first to create the IP to region file.\n"
        )
        raise FileNotFoundError(message)

    service_coordinates_file_path = ip_cache_directory / "service_coordinates.yaml"
    if not service_coordinates_file_path.exists():
        service_coordinates_file_path.touch()
    with service_coordinates_file_path.open(mode="r") as file_stream:
        service_coordinates = yaml.safe_load(stream=file_stream) or {}

    region_codes_to_coordinates: dict[str, dict[str, float]] = dict(_DEFAULT_REGION_CODES_TO_COORDINATES)
    previous_region_codes_to_coordinates = load_ip_cache(
        cache_type="region_codes_to_coordinates", cache_directory=cache_directory, use_encryption=use_encryption
    )
    region_codes_to_coordinates.update(previous_region_codes_to_coordinates)

    ip_to_region = load_ip_cache(
        cache_type="ip_to_region", cache_directory=cache_directory, use_encryption=use_encryption
    )
    region_codes_to_update = set(ip_to_region.values()) - set(region_codes_to_coordinates.keys())
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
            # Unresolvable labels (and IPs without any region) do not have coordinates, so skip
            if country_and_region_code is None or country_and_region_code == "bogon":
                continue

            service_name = country_and_region_code.split("/")[0]
            if service_name in _KNOWN_SERVICES:
                if geolite2_reader is None:
                    geolite2_reader = open_geolite2_database(cache_directory=cache_directory)
                coordinates = _get_service_coordinates_from_geolite2(
                    country_and_region_code=country_and_region_code,
                    geolite2_reader=geolite2_reader,
                    service_coordinates=service_coordinates,
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
    with service_coordinates_file_path.open(mode="w") as file_stream:
        yaml.dump(data=service_coordinates, stream=file_stream)

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
    service_coordinates: dict[str, dict[str, float]],
) -> dict[str, float] | None:
    """
    Locate a cloud service region (e.g. ``"AWS/us-east-1"``) by geolocating the first IP of one of its CIDR ranges.

    Note that services with a single code (e.g., "GitHub") should be handled via the global default dictionary.
    """
    import geoip2.errors

    coordinates = service_coordinates.get(country_and_region_code, None)
    if coordinates is not None:
        return coordinates

    service_name, subregion = country_and_region_code.split("/")
    cidr_addresses_and_subregions = _get_cidr_address_ranges_and_subregions(service_name=service_name)
    subregion_to_cidr_address = {subregion: cidr_address for cidr_address, subregion in cidr_addresses_and_subregions}

    ip_address = subregion_to_cidr_address[subregion].split("/")[0]
    try:
        location = geolite2_reader.city(ip_address).location
    except geoip2.errors.AddressNotFoundError:
        return None
    if location.latitude is None or location.longitude is None:
        return None

    coordinates = {"latitude": location.latitude, "longitude": location.longitude}
    service_coordinates[country_and_region_code] = coordinates

    return coordinates
