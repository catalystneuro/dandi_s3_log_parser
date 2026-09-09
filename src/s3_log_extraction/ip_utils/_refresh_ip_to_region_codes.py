import datetime
import math
import pathlib

import yaml

from ._geolite2 import open_geolite2_database
from ._ip_cache import load_ip_cache, write_ip_cache
from ._update_ip_to_region_codes import _get_region_code_from_ip_address
from ..config import get_cache_subdirectory

_REFRESH_CYCLE_DAYS = 90


def refresh_ip_to_region_codes(
    *,
    cache_directory: str | pathlib.Path | None = None,
    use_encryption: bool = True,
    _today: datetime.date | None = None,
) -> None:
    """
    Refresh a subset of the existing ``ip_to_region`` cache entries by re-resolving them against GeoLite2.

    The subset is selected deterministically based on the current date using a
    90-day cycle that partitions the alphabetically ordered IPs. Running this
    command once per day ensures the entire cache is refreshed every 90 days.

    Any changes detected are recorded in a YAML log file written to
    ``[cache_directory]/logs/ip_refresh_<date>.yaml``; no file is written when
    the checked partition has no changes.

    Parameters
    ----------
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
        If ``None``, the default cache directory will be used.
    use_encryption : bool
        If ``True`` (default), IP data files are decrypted when reading and encrypted when writing.
        If ``False``, IP data files are read and written as plaintext.
    _today : datetime.date | None
        Override today's date. Intended for testing only.
        If ``None`` (default), ``datetime.date.today()`` is used.
    """
    ip_to_region = load_ip_cache(
        cache_type="ip_to_region", cache_directory=cache_directory, use_encryption=use_encryption
    )

    if not ip_to_region:
        return

    sorted_ips = sorted(ip_to_region.keys())
    cache_size = len(sorted_ips)

    # Number of IPs to check per day is the cache size divided by the 90-day cycle
    partition_size = max(1, math.ceil(cache_size / _REFRESH_CYCLE_DAYS))

    today = _today if _today is not None else datetime.date.today()
    partition_index = today.toordinal() % _REFRESH_CYCLE_DAYS

    start_idx = partition_index * partition_size
    end_idx = min(start_idx + partition_size, cache_size)
    ips_to_refresh = sorted_ips[start_idx:end_idx]

    if not ips_to_refresh:
        return

    changes: dict[str, dict[str, str]] = {}
    with open_geolite2_database(cache_directory=cache_directory) as geolite2_reader:
        for ip_address in ips_to_refresh:
            old_region = ip_to_region[ip_address]
            new_region = _get_region_code_from_ip_address(ip_address=ip_address, geolite2_reader=geolite2_reader)
            if new_region != old_region:
                changes[ip_address] = {"old": old_region, "new": new_region}
                ip_to_region[ip_address] = new_region

    if changes:
        logs_directory = get_cache_subdirectory(cache_directory=cache_directory, name="logs")
        log_file_path = logs_directory / f"ip_refresh_{today.isoformat()}.yaml"
        log_data: dict = {
            "date": today.isoformat(),
            "partition_index": partition_index,
            "ips_checked": len(ips_to_refresh),
            "changes": changes,
        }
        with log_file_path.open(mode="w") as file_stream:
            yaml.dump(data=log_data, stream=file_stream)

        write_ip_cache(
            data=ip_to_region,
            cache_type="ip_to_region",
            cache_directory=cache_directory,
            use_encryption=use_encryption,
        )
