import collections
import datetime
import pathlib

import pandas
import tqdm

from .globals import (
    REGION_DISCLOSURE_THRESHOLD,
    REGION_VALUE_COLUMN_NAMES,
    SESSION_TIMEOUT_IN_SECONDS,
    TIMESTAMP_FORMAT,
)
from ..config import get_cache_directory, get_cache_subdirectory
from ..ip_utils import country_alpha_2_to_alpha_3, is_cloud_service_or_vpn_label, is_resolved_region, load_ip_cache
from ..ip_utils._ip_utils import _read_ips_from_file


def _read_summary_value(value: str | int | float, /) -> int:
    """
    Read a single value of a by-region summary that was written previously.

    Summaries written by earlier versions censored values below a disclosure threshold with sentinel
    strings such as ``"<50"``. Those carry no number and are read as zero, so that they compare as
    changed against any true value and the summary is republished once enough regions move.
    """
    numeric_value = pandas.to_numeric(value, errors="coerce")
    return 0 if pandas.isna(numeric_value) else int(numeric_value)


def _collect_resolved_region_values(summary_table: pandas.DataFrame, /) -> dict[str, tuple[int, ...]]:
    """
    Reduce a by-region summary to the values of its resolved regions, keyed by region.

    Only resolved regions are collected. Labels such as ``"missing"`` or ``"undetermined"`` aggregate
    requesters whose location is unknown, so they name no place and cannot be counted as one.
    """
    values_by_region: dict[str, list[int]] = {}
    for _, row in summary_table.iterrows():
        region = str(row["region"])
        if not is_resolved_region(region):
            continue

        values = values_by_region.setdefault(region, [0] * len(REGION_VALUE_COLUMN_NAMES))
        for index, column_name in enumerate(REGION_VALUE_COLUMN_NAMES):
            values[index] += _read_summary_value(row[column_name] if column_name in summary_table.columns else 0)
    return {region: tuple(values) for region, values in values_by_region.items()}


def _count_updated_regions(*, summary_table: pandas.DataFrame, previous_summary_table: pandas.DataFrame | None) -> int:
    """
    Count the resolved regions whose values the new summary would change.

    Every resolved region of a summary that has no previous version counts as updated, since writing that
    summary for the first time discloses all of them at once.
    """
    new_values = _collect_resolved_region_values(summary_table)
    if previous_summary_table is None:
        return len(new_values)

    previous_values = _collect_resolved_region_values(previous_summary_table)
    unchanged = (0,) * len(REGION_VALUE_COLUMN_NAMES)
    return sum(
        1
        for region in set(new_values) | set(previous_values)
        if new_values.get(region, unchanged) != previous_values.get(region, unchanged)
    )


def _write_summary_by_region(
    *,
    summary_table: pandas.DataFrame,
    summary_file_path: pathlib.Path,
    region_disclosure_threshold: int = REGION_DISCLOSURE_THRESHOLD,
) -> bool:
    """
    Write a by-region summary only if the update it carries spans more than the threshold of regions.

    Parameters
    ----------
    summary_table : pandas.DataFrame
        The newly computed by-region summary, with its true values.
    summary_file_path : pathlib.Path
        Destination of the summary. Any version already there is read to determine what the new summary
        would change, and is left untouched when the update is withheld.
    region_disclosure_threshold : int
        Number of resolved regions an update must move at once to be published.
        Defaults to ``REGION_DISCLOSURE_THRESHOLD``.

    Returns
    -------
    bool
        Whether the summary was written.
    """
    previous_summary_table = (
        pandas.read_table(filepath_or_buffer=summary_file_path) if summary_file_path.exists() else None
    )
    number_of_updated_regions = _count_updated_regions(
        summary_table=summary_table, previous_summary_table=previous_summary_table
    )
    if number_of_updated_regions <= region_disclosure_threshold:
        return False

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)
    return True


def _count_regions_and_countries(summary_file_path: pathlib.Path, /) -> tuple[int, int]:
    """
    Count the regions and the distinct countries of a by-region summary.

    A by-region summary is published only once its update spans enough resolved regions, so a dataset with
    little activity may not have one yet. Both counts are zero in that case.
    """
    if not summary_file_path.exists():
        return 0, 0

    summary_table = pandas.read_table(filepath_or_buffer=summary_file_path)
    regions = [str(region) for region in summary_table["region"]]

    unique_countries: set[str] = set()
    for region in regions:
        if not is_resolved_region(region):
            continue

        country_code, region_name = region.split("/", 1)
        if "AWS" in country_code:
            # AWS region names start with an alpha-2 country code ("us-east-1"); align it with the alpha-3
            # codes of geographic labels so that the same country is not counted twice
            country_code = country_alpha_2_to_alpha_3(region_name.split("-")[0])
        unique_countries.add(country_code)

    return len(regions), len(unique_countries)


def _collect_asset_views(
    *,
    asset_directory: pathlib.Path,
    use_encryption: bool = True,
    session_timeout_in_seconds: int = SESSION_TIMEOUT_IN_SECONDS,
) -> list[tuple[str, str]]:
    """
    Collect the views of a single asset.

    A view is a streaming session, not a request. One person exploring one file over a remote connection
    emits hundreds to thousands of partial range requests, so raw request counts measure a mix of interest
    and the mechanical cost of reading the file. Collapsing each burst of requests into a single countable
    unit is the fair measure of interest.

    A session is a maximal run of streaming requests from one IP address to this asset in which no two
    consecutive requests are more than ``session_timeout_in_seconds`` apart. Only streaming requests count,
    which the extraction cache marks with a ``0`` in ``download.txt``. Full downloads are reported
    separately by ``number_of_downloads``.

    Each view is returned with the date it began and the IP that made it, so that a view can be attributed
    to a single day and a single region. A session spans requests and can straddle midnight, so it is
    counted on the day of its first request.

    Parameters
    ----------
    asset_directory : pathlib.Path
        Path to a per-asset extraction directory containing the line-aligned ``timestamps.txt``,
        ``download.txt``, and ``ips.txt`` files.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` is decrypted before reading.
        If ``False``, the file is read as plaintext.
    session_timeout_in_seconds : int
        Maximum gap between two consecutive streaming requests of the same session.
        Defaults to ``SESSION_TIMEOUT_IN_SECONDS`` (8 hours).

    Returns
    -------
    list of tuple of str
        One ``(date, ip)`` pair per view, where ``date`` is the ``YYYY-MM-DD`` day the session began.

    Raises
    ------
    RuntimeError
        If any per-request file is missing, or if they are not line-aligned. Either means the extraction
        cache is incompatible (extracted before ``download.txt`` was introduced) or corrupted.
    """
    timestamps_file_path = asset_directory / "timestamps.txt"
    download_file_path = asset_directory / "download.txt"
    ips_file_path = asset_directory / "ips.txt"
    missing_file_names = [
        file_path.name
        for file_path in (timestamps_file_path, download_file_path, ips_file_path)
        if not file_path.exists()
    ]
    if missing_file_names:
        message = (
            f"\n\nThe extracted files for '{asset_directory}' are incomplete: "
            f"{', '.join(missing_file_names)} not found.\n"
            "Extraction writes every per-request file of an asset together, so none of them should be absent.\n\n"
            "A missing 'download.txt' means the asset was extracted before that file was introduced, which makes "
            "the extraction cache incompatible. Re-extract the asset to resolve it.\n"
            "Any other missing file means the extraction cache is corrupted.\n\n"
        )
        raise RuntimeError(message)

    timestamps = [stripped for line in timestamps_file_path.read_text().splitlines() if (stripped := line.strip())]
    downloads = [stripped for line in download_file_path.read_text().splitlines() if (stripped := line.strip())]
    ips = _read_ips_from_file(file_path=ips_file_path, use_encryption=use_encryption)

    if not len(timestamps) == len(downloads) == len(ips):
        message = (
            f"\n\nThe extracted files for '{asset_directory}' are not line-aligned "
            f"(timestamps: {len(timestamps)}, downloads: {len(downloads)}, IPs: {len(ips)}).\n"
            "Line N of each file must describe the same request for views to be counted.\n\n"
            "A short 'download.txt' means the asset was extracted before that file was introduced, which makes "
            "the extraction cache incompatible. Re-extract the asset to resolve it.\n"
            "Any other mismatch means the extraction cache is corrupted, most often by an extraction that was "
            "interrupted partway through writing these files.\n\n"
        )
        raise RuntimeError(message)

    parsed_timestamps_per_ip = collections.defaultdict(list)
    for timestamp, download, ip in zip(timestamps, downloads, ips):
        if download != "0":  # Full downloads are not views
            continue

        parsed_timestamps_per_ip[ip].append(
            datetime.datetime.strptime(timestamp, TIMESTAMP_FORMAT).replace(tzinfo=datetime.timezone.utc)
        )

    views = []
    for ip, parsed_timestamps in parsed_timestamps_per_ip.items():
        parsed_timestamps.sort()
        session_starts = [parsed_timestamps[0]] + [
            current
            for previous, current in zip(parsed_timestamps, parsed_timestamps[1:])
            if (current - previous).total_seconds() > session_timeout_in_seconds
        ]
        views.extend((session_start.strftime(format="%Y-%m-%d"), ip) for session_start in session_starts)
    return views


def _collect_unique_ips(
    asset_directories: list[pathlib.Path],
    use_encryption: bool = True,
    ip_to_region: dict[str, str] | None = None,
) -> set[str]:
    """
    Collect all unique IP addresses across the given asset directories.

    Parameters
    ----------
    asset_directories : list of pathlib.Path
        Paths to per-asset extraction directories containing ``ips.txt`` files.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` files are decrypted before reading.
        If ``False``, files are read as plaintext.
    ip_to_region : dict of str to str, optional
        Mapping of IP address to region/service label, used to exclude known cloud
        service IPs (e.g. GitHub, AWS, GCP, VPN) from the collected set. If not
        provided, no exclusion is applied.

    Returns
    -------
    set of str
        The set of unique IP addresses found across all ``ips.txt`` files, excluding
        any IPs classified as a known cloud service or VPN.
    """
    ip_to_region = ip_to_region or {}
    unique_ips: set[str] = set()
    for asset_directory in asset_directories:
        full_ips_file_path = asset_directory / "ips.txt"
        if not full_ips_file_path.exists():
            continue
        ips = _read_ips_from_file(file_path=full_ips_file_path, use_encryption=use_encryption)
        unique_ips.update(ip for ip in ips if not is_cloud_service_or_vpn_label(ip_to_region.get(ip) or ""))
    return unique_ips


def _summarize_dataset_requester_count(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    ip_to_region: dict[str, str],
    use_encryption: bool = True,
) -> None:
    """
    Compute and save the unique requester count for a dataset.

    Reads all ``ips.txt`` files from the given asset directories, counts the
    number of unique IP addresses across the entire dataset (excluding known cloud
    service and VPN IPs), and writes the value to ``summary_file_path``.

    The count is not paired with any location, so it cannot single out a requester and is written with
    its true value on every update.

    Parameters
    ----------
    asset_directories : list of pathlib.Path
        Paths to the per-asset extraction directories containing ``ips.txt`` files.
    summary_file_path : pathlib.Path
        Destination file where the count (as a string) will be written.
    ip_to_region : dict of str to str
        Mapping of IP address to region/service label, used to exclude known cloud
        service IPs (e.g. GitHub, AWS, GCP, VPN) from the requester count.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` files are decrypted before reading.
        If ``False``, files are read as plaintext.
    """
    unique_ips = _collect_unique_ips(
        asset_directories=asset_directories, use_encryption=use_encryption, ip_to_region=ip_to_region
    )

    if not unique_ips:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_file_path.write_text(str(len(unique_ips)))


def generate_summaries(
    level: int = 0,
    cache_directory: str | pathlib.Path | None = None,
    use_encryption: bool = True,
    region_disclosure_threshold: int = REGION_DISCLOSURE_THRESHOLD,
) -> None:
    """
    Generate summaries for each dataset in the extraction directory.

    There are several TSV summary files generated per outer level of the S3 bucket structure:
        - `by_day.tsv`: Summarizes the total bytes sent per day across all assets in the dataset.
        - `by_asset.tsv`: Summarizes the total bytes sent per asset in the dataset.
        - `by_region.tsv`: Summarizes the total bytes sent per region based on geolocations of the indexed IPs.

    Every summary is written with its true values, except for `by_region.tsv`. That one pairs activity with
    requester location, so it is written only when the update it carries moves more than
    ``region_disclosure_threshold`` resolved regions at once. Its totals therefore drift out of step with
    the other summaries between publications.

    Parameters
    ----------
    level : int
        The level of summaries to generate.
        Currently only level 0 is supported, which generates summaries for each dataset.
        Please raise an issue to request this feature: https://github.com/dandi/s3-log-extraction/issues/new
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` and IP cache files are decrypted when read.
        If ``False``, files are read as plaintext.
    region_disclosure_threshold : int
        Number of resolved regions an update to a `by_region.tsv` must move at once to be published.
        Default is ``REGION_DISCLOSURE_THRESHOLD`` (5).
    """
    if level != 0:
        message = (
            "\n\nCurrently only level 0 summaries are supported."
            "Please raise an issue to request this feature: https://github.com/dandi/s3-log-extraction/issues/new\n\n"
        )
        raise NotImplementedError(message)

    cache_dir = pathlib.Path(cache_directory) if cache_directory is not None else get_cache_directory()
    extraction_directory = cache_dir / "extraction"
    extraction_directory.mkdir(exist_ok=True)
    summary_directory = get_cache_subdirectory(cache_directory=cache_directory, name="summaries")
    ip_to_region = load_ip_cache(
        cache_type="ip_to_region", cache_directory=cache_directory, use_encryption=use_encryption
    )

    datasets = [item for item in extraction_directory.iterdir() if item.is_dir()]
    all_archive_unique_ips: set[str] = set()
    for dataset in tqdm.tqdm(
        iterable=datasets,
        total=len(datasets),
        desc="Summarizing Datasets",
        position=0,
        leave=True,
        mininterval=5.0,
        smoothing=0,
        unit="dataset",
    ):
        dataset_id = dataset.name

        asset_directories = sorted([file_path.parent for file_path in dataset.rglob(pattern="*bytes_sent.txt")])
        _summarize_dataset(
            dataset_id=dataset_id,
            asset_directories=asset_directories,
            summary_directory=summary_directory,
            ip_to_region=ip_to_region,
            use_encryption=use_encryption,
            region_disclosure_threshold=region_disclosure_threshold,
        )

        all_archive_unique_ips.update(
            _collect_unique_ips(
                asset_directories=asset_directories, use_encryption=use_encryption, ip_to_region=ip_to_region
            )
        )
    if all_archive_unique_ips:
        archive_directory = summary_directory / "archive"
        archive_directory.mkdir(exist_ok=True)
        (archive_directory / "requester_count.tsv").write_text(str(len(all_archive_unique_ips)))


def _summarize_dataset(
    *,
    dataset_id: str,
    asset_directories: list[pathlib.Path],
    summary_directory: pathlib.Path,
    ip_to_region: dict[str, str],
    use_encryption: bool = True,
    region_disclosure_threshold: int = REGION_DISCLOSURE_THRESHOLD,
) -> None:
    # Sessionizing decrypts ips.txt, so it is done once here and shared by all three summaries
    views_by_asset_directory = {
        asset_directory: _collect_asset_views(asset_directory=asset_directory, use_encryption=use_encryption)
        for asset_directory in asset_directories
    }

    _summarize_dataset_by_day(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_day.tsv",
        views_by_asset_directory=views_by_asset_directory,
    )
    _summarize_dataset_by_asset(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_asset.tsv",
        views_by_asset_directory=views_by_asset_directory,
    )
    _summarize_dataset_by_region(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_region.tsv",
        ip_to_region=ip_to_region,
        views_by_asset_directory=views_by_asset_directory,
        use_encryption=use_encryption,
        region_disclosure_threshold=region_disclosure_threshold,
    )
    _summarize_dataset_requester_count(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "requester_count.tsv",
        ip_to_region=ip_to_region,
        use_encryption=use_encryption,
    )


def _summarize_dataset_by_day(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    views_by_asset_directory: dict[pathlib.Path, list[tuple[str, str]]],
) -> None:
    all_dates = []
    all_bytes_sent = []
    all_downloads = []
    number_of_views_by_day = collections.defaultdict(int)
    for asset_directory in asset_directories:
        for view_date, _ in views_by_asset_directory.get(asset_directory, []):
            number_of_views_by_day[view_date] += 1

        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        timestamps_file_path = asset_directory / "timestamps.txt"

        if not timestamps_file_path.exists():
            continue

        dates = [
            datetime.datetime.strptime(str(timestamp.strip()), "%y%m%d%H%M%S").strftime(format="%Y-%m-%d")
            for timestamp in timestamps_file_path.read_text().splitlines()
        ]
        all_dates.extend(dates)

        bytes_sent_file_path = asset_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]
        all_bytes_sent.extend(bytes_sent)

        download_file_path = asset_directory / "download.txt"
        downloads = [int(value.strip()) for value in download_file_path.read_text().splitlines()]
        all_downloads.extend(downloads)

    summarized_activity_by_day = collections.defaultdict(int)
    number_of_requests_by_day = collections.defaultdict(int)
    number_of_downloads_by_day = collections.defaultdict(int)
    for date, bytes_sent, download in zip(all_dates, all_bytes_sent, all_downloads):
        summarized_activity_by_day[date] += bytes_sent
        number_of_requests_by_day[date] += 1
        number_of_downloads_by_day[date] += download

    if len(summarized_activity_by_day) == 0:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    all_dates_ordered = list(summarized_activity_by_day.keys())
    summary_table = pandas.DataFrame(
        data={
            "date": all_dates_ordered,
            "bytes_sent": list(summarized_activity_by_day.values()),
            "number_of_requests": [number_of_requests_by_day[date] for date in all_dates_ordered],
            "number_of_downloads": [number_of_downloads_by_day[date] for date in all_dates_ordered],
            "number_of_views": [number_of_views_by_day[date] for date in all_dates_ordered],
        }
    )
    summary_table.sort_values(by="date", inplace=True)
    summary_table.index = range(len(summary_table))
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _summarize_dataset_by_asset(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    views_by_asset_directory: dict[pathlib.Path, list[tuple[str, str]]],
) -> None:
    dataset_id = summary_file_path.parent.name
    extraction_base_path = summary_file_path.parent.parent.parent / "extraction" / dataset_id  # Assumes same cache dir

    summarized_activity_by_asset = collections.defaultdict(int)
    number_of_requests_by_asset = collections.defaultdict(int)
    number_of_downloads_by_asset = collections.defaultdict(int)
    number_of_views_by_asset = collections.defaultdict(int)
    for asset_directory in asset_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing
        bytes_sent_file_path = asset_directory / "bytes_sent.txt"

        if not bytes_sent_file_path.exists():
            continue

        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]

        asset_path = str(asset_directory.relative_to(extraction_base_path))
        summarized_activity_by_asset[asset_path] += sum(bytes_sent)
        number_of_requests_by_asset[asset_path] += len(bytes_sent)

        download_file_path = asset_directory / "download.txt"
        downloads = [int(value.strip()) for value in download_file_path.read_text().splitlines()]
        number_of_downloads_by_asset[asset_path] += sum(downloads)

        number_of_views_by_asset[asset_path] += len(views_by_asset_directory.get(asset_directory, []))

    if len(summarized_activity_by_asset) == 0:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    all_asset_paths = list(summarized_activity_by_asset.keys())
    summary_table = pandas.DataFrame(
        data={
            "asset_path": all_asset_paths,
            "bytes_sent": list(summarized_activity_by_asset.values()),
            "number_of_requests": [number_of_requests_by_asset[path] for path in all_asset_paths],
            "number_of_downloads": [number_of_downloads_by_asset[path] for path in all_asset_paths],
            "number_of_views": [number_of_views_by_asset[path] for path in all_asset_paths],
        }
    )
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _summarize_dataset_by_region(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    ip_to_region: dict[str, str],
    views_by_asset_directory: dict[pathlib.Path, list[tuple[str, str]]],
    use_encryption: bool = True,
    region_disclosure_threshold: int = REGION_DISCLOSURE_THRESHOLD,
) -> None:
    all_regions = []
    all_bytes_sent = []
    all_downloads = []
    number_of_views_by_region = collections.defaultdict(int)
    for asset_directory in asset_directories:
        for _, view_ip in views_by_asset_directory.get(asset_directory, []):
            # A ``None`` entry, as written by earlier versions for an address that could not be geolocated,
            # names no place any more than an absent one does
            number_of_views_by_region[ip_to_region.get(view_ip) or "missing"] += 1

        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing
        full_ips_file_path = asset_directory / "ips.txt"

        if not full_ips_file_path.exists():
            continue

        full_ips = _read_ips_from_file(file_path=full_ips_file_path, use_encryption=use_encryption)
        regions = [ip_to_region.get(ip) or "missing" for ip in full_ips]
        all_regions.extend(regions)

        bytes_sent_file_path = asset_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]
        all_bytes_sent.extend(bytes_sent)

        download_file_path = asset_directory / "download.txt"
        downloads = [int(value.strip()) for value in download_file_path.read_text().splitlines()]
        all_downloads.extend(downloads)

    summarized_activity_by_region = collections.defaultdict(int)
    number_of_requests_by_region = collections.defaultdict(int)
    number_of_downloads_by_region = collections.defaultdict(int)
    for region, bytes_sent, download in zip(all_regions, all_bytes_sent, all_downloads):
        summarized_activity_by_region[region] += bytes_sent
        number_of_requests_by_region[region] += 1
        number_of_downloads_by_region[region] += download

    if len(summarized_activity_by_region) == 0:
        return

    all_regions_ordered = list(summarized_activity_by_region.keys())
    summary_table = pandas.DataFrame(
        data={
            "region": all_regions_ordered,
            "bytes_sent": list(summarized_activity_by_region.values()),
            "number_of_requests": [number_of_requests_by_region[region] for region in all_regions_ordered],
            "number_of_downloads": [number_of_downloads_by_region[region] for region in all_regions_ordered],
            "number_of_views": [number_of_views_by_region[region] for region in all_regions_ordered],
        }
    )
    _write_summary_by_region(
        summary_table=summary_table,
        summary_file_path=summary_file_path,
        region_disclosure_threshold=region_disclosure_threshold,
    )
