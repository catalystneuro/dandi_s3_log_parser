import collections
import csv
import gzip
import json
import pathlib
import typing

from ..ip_utils._ip_utils import _read_ips_from_file
from ..ip_utils._resolver import IpRegionResolver, RegionResolver


class LogBucketStats(typing.TypedDict):
    """Statistics for all objects in a local S3 Inventory.

    Attributes
    ----------
    file_count : int
        Total number of object keys recorded in the inventory.
    total_size_bytes : int or None
        Sum of object sizes in bytes, or ``None`` if the inventory does not
        include a ``Size`` column.
    """

    file_count: int
    total_size_bytes: int | None


class ExtractionCompletionStats(typing.TypedDict):
    """Completion statistics comparing processed records against inventory size.

    Attributes
    ----------
    processed_file_count : int
        Number of unique log filenames found in the remote extraction end record.
    inventory_file_count : int
        Total number of object keys recorded in the latest inventory snapshot.
    percent_complete : float
        ``processed_file_count / inventory_file_count * 100`` (or ``0.0`` when
        ``inventory_file_count`` is zero).
    """

    processed_file_count: int
    inventory_file_count: int
    percent_complete: float


class IpCategoryCount(typing.TypedDict):
    """Count and percentage for a single IP classification category.

    Attributes
    ----------
    count : int
        Number of IP addresses in this category.
    percent : float
        Fraction of total cached IPs, expressed as a percentage.
    """

    count: int
    percent: float


class IpStats(typing.TypedDict):
    """Classification statistics of the IP addresses in the extraction cache.

    Attributes
    ----------
    extracted_ip_count : int
        Number of unique IP addresses found across all ``ips.txt`` files in the
        extraction cache.
    determined : IpCategoryCount
        IPs resolved to a geographic region (``"USA/CA"``) or a country (``"USA"``).
    unknown : IpCategoryCount
        IPs that are malformed, absent from the GeoLite2 database, or without a country there (``"unknown"``).
    bogon : IpCategoryCount
        IPs flagged as bogon (private / reserved address space).
    vpn : IpCategoryCount
        IPs classified as VPN or datacenter addresses.
    cloud_service : IpCategoryCount
        IPs belonging to a known cloud provider CIDR (AWS or GCP).
    github : IpCategoryCount
        IPs belonging to GitHub CIDR ranges.
    """

    extracted_ip_count: int
    determined: IpCategoryCount
    unknown: IpCategoryCount
    bogon: IpCategoryCount
    vpn: IpCategoryCount
    cloud_service: IpCategoryCount
    github: IpCategoryCount


def get_ip_stats(
    cache_directory: str | pathlib.Path | None = None,
    use_encryption: bool = True,
    region_resolver: RegionResolver | None = None,
) -> IpStats:
    """Return classification stats of the IP addresses in the extraction cache.

    Collects the unique IPs across all ``ips.txt`` files in the extraction cache, resolves each of them the
    same way the summaries do, and bins every one into one of these mutually-exclusive categories:

    * **determined** – a geographic region (``"USA/CA"``) or country (``"USA"``).
    * **unknown** – the IP is malformed, absent from the GeoLite2 database, or has no country there.
    * **bogon** – the IP is in private / reserved address space (``"bogon"``).
    * **vpn** – the IP matches a known VPN / datacenter CIDR (starts with ``"VPN"``).
    * **cloud_service** – the IP belongs to an AWS or GCP CIDR range.
    * **github** – the IP belongs to a GitHub CIDR range.

    Parameters
    ----------
    cache_directory : path-like or None, optional
        Root of the cache tree.  When ``None`` the configured default is used.
    use_encryption : bool, optional
        If ``True`` (default), ``ips.txt`` files are decrypted before reading.
        Pass ``False`` for plaintext files.
    region_resolver : RegionResolver, optional
        Resolves each IP address to its region/service label. Defaults to an ``IpRegionResolver`` over the
        GeoLite2 database in the cache directory.

    Returns
    -------
    IpStats
        A typed dict with the extracted count and per-category breakdowns.
    """
    from ..config import get_cache_directory

    cache_path = pathlib.Path(cache_directory) if cache_directory is not None else get_cache_directory()

    # Count unique IPs across all ips.txt files in the extraction subdirectory, matching the scope of the summaries
    extraction_dir = cache_path / "extraction"
    extracted_ips: set[str] = set()
    if extraction_dir.exists():
        for ips_file in extraction_dir.rglob("ips.txt"):
            extracted_ips.update(_read_ips_from_file(file_path=ips_file, use_encryption=use_encryption))
    extracted_ip_count = len(extracted_ips)

    def _categorize(region: str) -> str:
        match region:
            case "unknown":
                return "unknown"
            case "bogon":
                return "bogon"
            case _ if region.startswith("VPN"):
                return "vpn"
            case _ if region.startswith(("AWS", "GCP")):
                return "cloud_service"
            case _ if region.startswith("GitHub"):
                return "github"
            case _:
                return "determined"

    counts: collections.Counter[str] = collections.Counter()
    if extracted_ips:
        owns_resolver = region_resolver is None
        if owns_resolver:
            region_resolver = IpRegionResolver(cache_directory=cache_directory)
        try:
            counts.update(_categorize(region_resolver.resolve(ip)) for ip in extracted_ips)
        finally:
            if owns_resolver:
                region_resolver.close()

    def _pct(n: int) -> float:
        return (n / extracted_ip_count * 100) if extracted_ip_count > 0 else 0.0

    return IpStats(
        extracted_ip_count=extracted_ip_count,
        determined=IpCategoryCount(count=counts["determined"], percent=_pct(counts["determined"])),
        unknown=IpCategoryCount(count=counts["unknown"], percent=_pct(counts["unknown"])),
        bogon=IpCategoryCount(count=counts["bogon"], percent=_pct(counts["bogon"])),
        vpn=IpCategoryCount(count=counts["vpn"], percent=_pct(counts["vpn"])),
        cloud_service=IpCategoryCount(count=counts["cloud_service"], percent=_pct(counts["cloud_service"])),
        github=IpCategoryCount(count=counts["github"], percent=_pct(counts["github"])),
    )


def _extract_date_from_log_filename(filename: str) -> str | None:
    """
    Extract ``YYYY-MM-DD`` from a standard S3 server access log filename.

    S3 access log files are named ``YYYY-MM-DD-HH-MM-SS-UniqueString``.
    This function validates that the first three dash-separated components
    look like a valid calendar date.

    Parameters
    ----------
    filename : str
        The file name component of an S3 object key (no directory separators).

    Returns
    -------
    str or None
        The date string ``"YYYY-MM-DD"`` when the filename matches the S3
        access log naming convention, otherwise ``None``.
    """
    parts = filename.split("-")
    if len(parts) < 3:
        return None
    year_str, month_str, day_str = parts[0], parts[1], parts[2]
    if len(year_str) != 4 or len(month_str) != 2 or len(day_str) != 2:
        return None
    if not (year_str.isdigit() and month_str.isdigit() and day_str.isdigit()):
        return None
    return f"{year_str}-{month_str}-{day_str}"


def _load_inventory_manifest(
    inventory_directory: pathlib.Path,
) -> tuple[str, list[str], pathlib.Path]:
    """
    Load the most recent inventory manifest and return parsing metadata.

    Parameters
    ----------
    inventory_directory : pathlib.Path
        Root of the pre-downloaded S3 inventory tree.

    Returns
    -------
    source_bucket : str
        The bucket name recorded in ``manifest.json``.
    file_schema : list[str]
        Ordered list of column names from the inventory CSV.
    symlink_path : pathlib.Path
        Path to the ``symlink.txt`` file in the most recent hive partition.

    Raises
    ------
    FileNotFoundError
        If no ``dt=*`` hive partitions are found.
    """
    hive_directory = inventory_directory / "hive"
    hive_partitions = sorted(hive_directory.glob("dt=*"))
    if not hive_partitions:
        message = f"No hive partitions found in {hive_directory}."
        raise FileNotFoundError(message)
    latest_partition = hive_partitions[-1]

    dt_value = latest_partition.name[len("dt=") :]
    date_part = dt_value[:10]
    time_part = dt_value[11:]
    timestamp_dir_name = f"{date_part}T{time_part}Z"
    manifest_path = inventory_directory / timestamp_dir_name / "manifest.json"

    with manifest_path.open(mode="r") as file_stream:
        manifest = json.load(fp=file_stream)

    source_bucket: str = manifest["sourceBucket"]
    file_schema = [col.strip() for col in manifest["fileSchema"].split(",")]
    symlink_path = latest_partition / "symlink.txt"

    return source_bucket, file_schema, symlink_path


def _read_s3_urls_from_local_inventory(
    inventory_directory: pathlib.Path,
    s3_root: str,
) -> dict[str, list[str]]:
    """
    Parse a local AWS S3 Inventory directory and return S3 URLs grouped by date.

    Reads the most recent hive partition, resolves the corresponding
    ``manifest.json``, follows the ``symlink.txt`` references, and parses
    every referenced ``data/*.csv.gz`` file.  Only keys whose full
    ``s3://`` URL starts with ``s3_root`` (trailing slash normalised) are
    included.

    Dates are extracted from each matching key using two strategies, applied
    in order:

    1. **Path-based** — if the path relative to ``s3_root`` starts with
       components that look like ``YYYY/MM/DD/…`` (each part is the expected
       number of digits), the date is taken from those three components.
    2. **Filename-based** — if path-based extraction fails (e.g. flat log
       files stored directly in the bucket root, or logs nested under an
       ``account-id/region/bucket/`` prefix before the date directories),
       the date is extracted from the log filename itself.  S3 server access
       log files use the naming convention
       ``YYYY-MM-DD-HH-MM-SS-UniqueString``, so the date is always present
       in the filename regardless of path depth.

    This dual strategy means the function correctly handles buckets that have
    a mix of flat-storage (legacy) and nested-storage (current) log files,
    even when ``s3_root`` is set to the outer bucket root.

    The AWS S3 Inventory directory must follow the standard layout::

        <inventory_directory>/
        ├── <timestamp>/          # e.g. 2026-05-03T01-00Z/
        │   └── manifest.json
        ├── data/
        │   └── <uuid>.csv.gz
        └── hive/
            └── dt=<YYYY-MM-DD-HH-MM>/
                └── symlink.txt

    Parameters
    ----------
    inventory_directory : pathlib.Path
        Root of the pre-downloaded S3 inventory tree.
    s3_root : str
        S3 prefix used to filter object keys
        (e.g. ``"s3://my-logs-bucket/logs"`` or ``"s3://my-logs-bucket"``
        for a bucket-root prefix that covers both flat and nested files).

    Returns
    -------
    dict[str, list[str]]
        Mapping of ``"YYYY-MM-DD"`` date strings to lists of matching S3
        URLs found in the inventory snapshot.

    Raises
    ------
    FileNotFoundError
        If no ``dt=*`` hive partitions are found.
    ValueError
        If the ``Key`` column is absent from the inventory schema.
    """
    inventory_directory = pathlib.Path(inventory_directory)
    source_bucket, file_schema, symlink_path = _load_inventory_manifest(inventory_directory)

    if "Key" not in file_schema:
        message = f"'Key' column not found in inventory schema: {file_schema}"
        raise ValueError(message)
    key_index = file_schema.index("Key")

    # Read symlink.txt — each line is an S3 path to a data/*.csv.gz file.
    symlink_lines = [line.strip() for line in symlink_path.read_text().splitlines() if line.strip()]

    # Parse each local CSV.gz file referenced by the symlink.
    s3_root_prefix = s3_root.rstrip("/") + "/"
    inventory: dict[str, list[str]] = collections.defaultdict(list)
    for s3_data_path in symlink_lines:
        uuid_filename = s3_data_path.split("/")[-1]
        local_csv_gz_path = inventory_directory / "data" / uuid_filename
        with gzip.open(local_csv_gz_path, "rt", newline="") as gz_file:
            reader = csv.reader(gz_file)
            for row in reader:
                if len(row) <= key_index:
                    continue
                key = row[key_index]
                s3_url = f"s3://{source_bucket}/{key}"
                if not s3_url.startswith(s3_root_prefix):
                    continue
                relative_path = s3_url[len(s3_root_prefix) :]
                parts = relative_path.split("/")

                # Strategy 1: path-based date extraction for year/month/day/... structure.
                # Validate that the first three components look like a calendar date so that
                # deeply-nested paths (e.g. account-id/region/bucket/year/month/day/logfile)
                # are not misidentified.
                date = None
                if len(parts) >= 4:
                    year, month, day = parts[0], parts[1], parts[2]
                    if (
                        len(year) == 4
                        and year.isdigit()
                        and len(month) == 2
                        and month.isdigit()
                        and len(day) == 2
                        and day.isdigit()
                    ):
                        date = f"{year}-{month}-{day}"

                # Strategy 2: filename-based date extraction as a fallback.
                # Handles flat files stored directly in the bucket root as well as
                # files nested under a non-date prefix (e.g. account-id/region/bucket/).
                # S3 server access log filenames always start with YYYY-MM-DD-HH-MM-SS-*.
                if date is None:
                    date = _extract_date_from_log_filename(parts[-1])

                if date is None:
                    continue

                inventory[date].append(s3_url)

    return dict(inventory)


def get_log_bucket_stats(
    inventory_directory: pathlib.Path,
) -> LogBucketStats:
    """
    Return the file count and total size for all objects in the inventory.

    Reads the most recent hive partition of a local AWS S3 Inventory
    directory, follows the ``symlink.txt`` references, and accumulates
    statistics for every object key recorded in the inventory CSV files.

    The AWS S3 Inventory directory must follow the standard layout::

        <inventory_directory>/
        ├── <timestamp>/          # e.g. 2026-05-03T01-00Z/
        │   └── manifest.json
        ├── data/
        │   └── <uuid>.csv.gz
        └── hive/
            └── dt=<YYYY-MM-DD-HH-MM>/
                └── symlink.txt

    Parameters
    ----------
    inventory_directory : pathlib.Path
        Root of the pre-downloaded S3 inventory tree.

    Returns
    -------
    LogBucketStats
        A typed dict with:

        ``file_count`` : int
            Total number of object keys in the inventory.
        ``total_size_bytes`` : int or None
            Sum of object sizes in bytes, or ``None`` when the inventory
            does not include a ``Size`` column.

    Raises
    ------
    FileNotFoundError
        If no ``dt=*`` hive partitions are found.
    ValueError
        If the ``Key`` column is absent from the inventory schema.
    """
    inventory_directory = pathlib.Path(inventory_directory)
    _, file_schema, symlink_path = _load_inventory_manifest(inventory_directory)

    if "Key" not in file_schema:
        message = f"'Key' column not found in inventory schema: {file_schema}"
        raise ValueError(message)
    key_index = file_schema.index("Key")
    size_index = file_schema.index("Size") if "Size" in file_schema else None

    symlink_lines = [line.strip() for line in symlink_path.read_text().splitlines() if line.strip()]

    file_count = 0
    total_size_bytes: int | None = 0 if size_index is not None else None

    for s3_data_path in symlink_lines:
        uuid_filename = s3_data_path.split("/")[-1]
        local_csv_gz_path = inventory_directory / "data" / uuid_filename
        with gzip.open(local_csv_gz_path, "rt", newline="") as gz_file:
            reader = csv.reader(gz_file)
            for row in reader:
                if len(row) <= key_index:
                    continue
                file_count += 1
                if size_index is not None and len(row) > size_index:
                    total_size_bytes += int(row[size_index])  # type: ignore[operator]

    return LogBucketStats(file_count=file_count, total_size_bytes=total_size_bytes)


def get_extraction_completion(
    inventory_directory: pathlib.Path,
    *,
    cache_directory: pathlib.Path | None = None,
) -> ExtractionCompletionStats:
    """
    Compare remote extraction progress against the latest local inventory count.

    This helper reads:

    - latest inventory file count via :func:`get_log_bucket_stats`
    - current remote extraction end records (all files in ``records/`` whose
      names end with ``processing-end.txt``)

    and returns a simple percentage complete summary.

    Parameters
    ----------
    inventory_directory : pathlib.Path
        Root of the pre-downloaded S3 inventory tree.
    cache_directory : pathlib.Path or None, optional
        Cache directory containing the ``records/`` subdirectory.  If omitted,
        the configured default cache directory is used.

    Returns
    -------
    ExtractionCompletionStats
        A typed dict with processed count, inventory count, and completion
        percentage.
    """
    from ..config import get_cache_subdirectory

    inventory_stats = get_log_bucket_stats(inventory_directory=inventory_directory)
    records_directory = get_cache_subdirectory(cache_directory=cache_directory, name="records")
    record_file_paths = [
        record_file_path
        for record_file_path in records_directory.iterdir()
        if record_file_path.is_file() and record_file_path.name.endswith("processing-end.txt")
    ]

    processed_file_record_keys: set[str] = set()
    for record_file_path in record_file_paths:
        processed_file_record_keys.update(
            {line.strip() for line in record_file_path.read_text().splitlines() if line.strip()}
        )
    processed_file_count = len(processed_file_record_keys)

    inventory_file_count = inventory_stats["file_count"]
    percent_complete = 0.0 if inventory_file_count == 0 else processed_file_count / inventory_file_count * 100.0

    return ExtractionCompletionStats(
        processed_file_count=processed_file_count,
        inventory_file_count=inventory_file_count,
        percent_complete=percent_complete,
    )
