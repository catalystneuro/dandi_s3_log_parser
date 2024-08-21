"""Various private utility functions for handling IP address related tasks."""

import functools
import hashlib
import ipaddress
import os
import traceback
from typing import Literal

import ipinfo
import requests
import yaml

from ._config import (
    _IP_HASH_NOT_IN_SERVICES_FILE_PATH,
    _IP_HASH_TO_REGION_FILE_PATH,
)
from ._error_collection import _collect_error
from ._globals import _KNOWN_SERVICES


def get_region_from_ip_address(
    ip_address: str, ip_hash_to_region: dict[str, str], ip_hash_not_in_services: dict[str, bool]
) -> str | None:
    """
    If the parsed S3 logs are meant to be shared openly, the remote IP could be used to directly identify individuals.

    Instead, identify the generic region of the world the request came from and report that instead.
    """
    if ip_address == "unknown":
        return "unknown"

    if "IPINFO_CREDENTIALS" not in os.environ:
        message = "The environment variable 'IPINFO_CREDENTIALS' must be set to import `dandi_s3_log_parser`!"
        raise ValueError(message)  # pragma: no cover
    ipinfo_credentials = os.environ["IPINFO_CREDENTIALS"]

    if "IP_HASH_SALT" not in os.environ:
        message = (
            "The environment variable 'IP_HASH_SALT' must be set to import `dandi_s3_log_parser`! "
            "To retrieve the value, set a temporary value to this environment variable "
            "and then use the `get_hash_salt` helper function and set it to the correct value."
        )
        raise ValueError(message)  # pragma: no cover
    ip_hash_salt = bytes.fromhex(os.environ["IP_HASH_SALT"])

    # Probably a legitimate user, so fetch the geographic region
    ip_hash = hashlib.sha1(string=bytes(ip_address, "utf-8") + ip_hash_salt).hexdigest()

    # Early return for speed
    lookup_result = ip_hash_to_region.get(ip_hash, None)
    if lookup_result is not None:
        return lookup_result

    # Determine if IP address belongs to GitHub, AWS, Google, or known VPNs
    # Azure not yet easily doable; keep an eye on
    # https://learn.microsoft.com/en-us/answers/questions/1410071/up-to-date-azure-public-api-to-get-azure-ip-ranges
    # and others, maybe it will change in the future
    if ip_hash_not_in_services.get(ip_hash, None) is None:
        for service_name in _KNOWN_SERVICES:
            cidr_addresses = _get_cidr_address_ranges(service_name=service_name)

            if any(
                ipaddress.ip_address(address=ip_address) in ipaddress.ip_network(address=cidr_address)
                for cidr_address in cidr_addresses
            ):
                ip_hash_to_region[ip_hash] = service_name
                return service_name
    ip_hash_not_in_services[ip_hash] = True

    # Log errors in IP fetching
    # Lines cannot be covered without testing on a real IP
    try:  # pragma: no cover
        handler = ipinfo.getHandler(access_token=ipinfo_credentials)
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
        message = (
            f"Error fetching IP information for {ip_address}!\n\n"
            f"{type(exception)}: {exception}\n\n"
            f"{traceback.format_exc()}",
        )
        _collect_error(message=message, error_type="ipinfo")

        return "unknown"


@functools.lru_cache
def _get_cidr_address_ranges(*, service_name: str) -> list[str]:
    match service_name:
        case "GitHub":
            github_cidr_request = requests.get(url="https://api.github.com/meta").json()
            skip_keys = ["domains", "ssh_key_fingerprints", "verifiable_password_authentication", "ssh_keys"]
            keys = set(github_cidr_request.keys()) - set(skip_keys)
            github_cidr_addresses = [
                cidr_address
                for key in keys
                for cidr_address in github_cidr_request[key]
                if "::" not in cidr_address
                # Skip IPv6
            ]

            return github_cidr_addresses
        # Note: these endpoints also return the 'locations' of the specific subnet, such as 'us-east-2'
        case "AWS":
            aws_cidr_request = requests.get(url="https://ip-ranges.amazonaws.com/ip-ranges.json").json()
            aws_cidr_addresses = [prefix["ip_prefix"] for prefix in aws_cidr_request["prefixes"]]

            return aws_cidr_addresses
        case "GCP":
            gcp_cidr_request = requests.get(url="https://www.gstatic.com/ipranges/cloud.json").json()
            gcp_cidr_addresses = [
                prefix["ipv4Prefix"]
                for prefix in gcp_cidr_request["prefixes"]
                if "ipv4Prefix" in prefix  # Not handling IPv6 yet
            ]

            return gcp_cidr_addresses
        case "Azure":
            raise NotImplementedError("Azure CIDR address fetching is not yet implemented!")  # pragma: no cover
        case "VPN":
            # Very nice public and maintained listing! Hope this stays stable.
            vpn_cidr_addresses = (
                requests.get(
                    url="https://raw.githubusercontent.com/josephrocca/is-vpn/main/vpn-or-datacenter-ipv4-ranges.txt"
                )
                .content.decode("utf-8")
                .splitlines()
            )

            return vpn_cidr_addresses
        case _:
            raise ValueError(f"Service name '{service_name}' is not supported!")  # pragma: no cover


def _load_ip_hash_cache(*, name: Literal["region", "services"]) -> dict[str, str] | dict[str, bool]:
    """Load the IP hash to region cache from disk."""
    match name:
        case "region":
            if not _IP_HASH_TO_REGION_FILE_PATH.exists():
                return {}  # pragma: no cover

            with open(file=_IP_HASH_TO_REGION_FILE_PATH) as stream:
                return yaml.load(stream=stream, Loader=yaml.SafeLoader)
        case "services":
            if not _IP_HASH_NOT_IN_SERVICES_FILE_PATH.exists():
                return {}  # pragma: no cover

            with open(file=_IP_HASH_NOT_IN_SERVICES_FILE_PATH) as stream:
                return yaml.load(stream=stream, Loader=yaml.SafeLoader)
        case _:
            raise ValueError(f"Name '{name}' is not recognized!")  # pragma: no cover


def _save_ip_hash_cache(*, name: Literal["region", "services"], ip_cache: dict[str, str] | dict[str, bool]) -> None:
    """Save the IP hash to region cache to disk."""
    match name:
        case "region":
            with open(file=_IP_HASH_TO_REGION_FILE_PATH, mode="w") as stream:
                yaml.dump(data=ip_cache, stream=stream)
        case "services":
            with open(file=_IP_HASH_NOT_IN_SERVICES_FILE_PATH, mode="w") as stream:
                yaml.dump(data=ip_cache, stream=stream)
        case _:
            raise ValueError(f"Name '{name}' is not recognized!")  # pragma: no cover
