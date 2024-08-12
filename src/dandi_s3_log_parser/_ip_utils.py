"""Various private utility functions for handling IP address related tasks."""

import datetime
import hashlib
import ipaddress
import traceback
from importlib.metadata import version as importlib_version

import ipinfo
import requests
import yaml

from ._config import (
    _IP_HASH_TO_REGION_FILE_PATH,
    DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH,
    IPINFO_CREDENTIALS,
    IPINFO_HASH_SALT,
)


def _cidr_address_to_ip_range(*, cidr_address: str) -> list[str]:
    """Convert a CIDR address to a list of IP addresses."""
    cidr_address_class = type(ipaddress.ip_address(cidr_address.split("/")[0]))
    ip_address_range = []
    if cidr_address_class is ipaddress.IPv4Address:
        ip_address_range = ipaddress.IPv4Network(address=cidr_address)
    elif cidr_address_class is ipaddress.IPv6Address:  # pragma: no cover
        ip_address_range = ipaddress.IPv6Network(address=cidr_address)

    return [str(ip_address) for ip_address in ip_address_range]


def _get_latest_github_ip_ranges() -> list[str]:
    """Retrieve the latest GitHub CIDR ranges from their API and expand them into a list of IP addresses."""
    github_ip_request = requests.get("https://api.github.com/meta").json()

    skip_keys = ["domains", "ssh_key_fingerprints", "verifiable_password_authentication", "ssh_keys"]
    keys = set(github_ip_request.keys()) - set(skip_keys)
    github_cidr_addresses = [
        cidr_address for key in keys for cidr_address in github_ip_request[key] if "::" not in cidr_address  # Skip IPv6
    ]

    all_github_ips = [
        str(ip_address)
        for cidr_address in github_cidr_addresses
        for ip_address in _cidr_address_to_ip_range(cidr_address=cidr_address)
    ]

    return all_github_ips


def _load_ip_hash_to_region_cache() -> dict[str, str]:
    """Load the IP hash to region cache from disk."""
    if not _IP_HASH_TO_REGION_FILE_PATH.exists():
        return {}  # pragma: no cover

    with open(file=_IP_HASH_TO_REGION_FILE_PATH) as stream:
        return yaml.load(stream=stream, Loader=yaml.SafeLoader)


def _save_ip_hash_to_region_cache(*, ip_hash_to_region: dict[str, str]) -> None:
    """Save the IP hash to region cache to disk."""
    with open(file=_IP_HASH_TO_REGION_FILE_PATH, mode="w") as stream:
        yaml.dump(data=ip_hash_to_region, stream=stream)


def _get_region_from_ip_address(*, ip_address: str, ip_hash_to_region: dict[str, str]) -> str | None:
    """
    If the parsed S3 logs are meant to be shared openly, the remote IP could be used to directly identify individuals.

    Instead, identify the generic region of the world the request came from and report that instead.
    """
    if ip_address == "unknown":
        return "unknown"

    ip_hash = hashlib.sha1(string=bytes(ip_address, "utf-8") + IPINFO_HASH_SALT).hexdigest()

    # Early return for speed
    lookup_result = ip_hash_to_region.get(ip_hash)
    if lookup_result is not None:
        return lookup_result

    # Log errors in IP fetching
    # Lines cannot be covered without testing on a real IP
    try:  # pragma: no cover
        handler = ipinfo.getHandler(access_token=IPINFO_CREDENTIALS)
        details = handler.getDetails(ip_address=ip_address)

        country = details.details.get("country", None)
        region = details.details.get("region", None)

        region_string = ""  # Not technically necessary, but quiets the linter
        match (country is None, region is None):
            case (True, True):
                region_string = "unknown"
            case (True, False):
                region_string = region
            case (False, True):
                region_string = country
            case (False, False):
                region_string = f"{country}/{region}"
        ip_hash_to_region[ip_hash] = region_string

        return region_string
    except ipinfo.exceptions.RequestQuotaExceededError:  # pragma: no cover
        # Return the generic 'unknown' but do not cache
        return "unknown"
    except Exception as exception:  # pragma: no cover
        errors_folder_path = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
        errors_folder_path.mkdir(exist_ok=True)

        dandi_s3_log_parser_version = importlib_version(distribution_name="dandi_s3_log_parser")
        date = datetime.datetime.now().strftime("%y%m%d")
        lines_errors_file_path = errors_folder_path / f"v{dandi_s3_log_parser_version}_{date}_ipinfo_errors.txt"

        with open(file=lines_errors_file_path, mode="a") as io:
            io.write(
                f"Error fetching IP information for {ip_address}!\n\n"
                f"{type(exception)}: {exception!s}\n\n"
                f"{traceback.format_exc()}",
            )

        return "unknown"
