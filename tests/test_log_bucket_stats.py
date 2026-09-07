"""Mocked tests for the get_log_bucket_stats API helper and the 'stats' CLI command."""

import csv
import gzip
import io
import json
import pathlib
from unittest.mock import patch

import pytest
from click.testing import CliRunner

import s3_log_extraction._command_line_interface._cli as cli_module
from s3_log_extraction._command_line_interface._cli import s3logextraction_cli
from s3_log_extraction.ip_utils import MappingRegionResolver
from s3_log_extraction.utils.inventory import get_extraction_completion, get_ip_stats, get_log_bucket_stats


def _build_inventory_directory(
    tmp_path: pathlib.Path,
    *,
    source_bucket: str,
    rows: list[tuple],
    file_schema: str,
    timestamp: str = "2024-01-05T01-00Z",
    dt_partition: str = "dt=2024-01-05-01-00",
) -> pathlib.Path:
    """
    Create a minimal local AWS S3 Inventory directory for testing.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Root directory under which the inventory tree is created.
    source_bucket : str
        Value placed in the ``sourceBucket`` field of ``manifest.json``.
    rows : list[tuple]
        Rows to write into the CSV data file; each tuple must match *file_schema*.
    file_schema : str
        Comma-separated column names written into ``manifest.json``.
    timestamp : str
        Name of the timestamped manifest directory, e.g. ``"2024-01-05T01-00Z"``.
    dt_partition : str
        Name of the hive partition directory, e.g. ``"dt=2024-01-05-01-00"``.

    Returns
    -------
    pathlib.Path
        The root of the created inventory directory.
    """
    inventory_dir = tmp_path / "inventory"
    inventory_dir.mkdir(exist_ok=True)
    data_dir = inventory_dir / "data"
    data_dir.mkdir(exist_ok=True)
    hive_dir = inventory_dir / "hive"
    hive_dir.mkdir(exist_ok=True)

    # Write CSV.gz data file
    csv_buffer = io.BytesIO()
    with gzip.open(csv_buffer, "wt", newline="") as gz:
        writer = csv.writer(gz)
        for row in rows:
            writer.writerow(row)
    uuid_filename = "test-uuid.csv.gz"
    (data_dir / uuid_filename).write_bytes(csv_buffer.getvalue())

    # Write manifest.json in the timestamp directory
    timestamp_dir = inventory_dir / timestamp
    timestamp_dir.mkdir(exist_ok=True)
    manifest = {
        "sourceBucket": source_bucket,
        "fileFormat": "CSV",
        "fileSchema": file_schema,
        "files": [
            {
                "key": f"inventory/{source_bucket}/data/{uuid_filename}",
                "size": len(csv_buffer.getvalue()),
            }
        ],
    }
    with (timestamp_dir / "manifest.json").open("w") as f:
        json.dump(manifest, f)

    # Write hive partition and symlink.txt
    partition_dir = hive_dir / dt_partition
    partition_dir.mkdir(exist_ok=True)
    s3_data_ref = f"s3://inventory-bucket/inventory/{source_bucket}/data/{uuid_filename}"
    (partition_dir / "symlink.txt").write_text(s3_data_ref + "\n")

    return inventory_dir


# ---------------------------------------------------------------------------
# API helper tests
# ---------------------------------------------------------------------------

SOURCE_BUCKET = "my-bucket"

_API_PARAMS = [
    pytest.param(
        "Bucket, Key, Size",
        [
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 100),
            (SOURCE_BUCKET, "logs/2024-01-01-00-05-00-BBBB", 200),
            (SOURCE_BUCKET, "other/2024-01-02-00-00-00-CCCC", 300),
            (SOURCE_BUCKET, "other/2024-01-02-00-05-00-DDDD", 400),
        ],
        4,
        1000,
        id="with_size_column",
    ),
    pytest.param(
        "Bucket, Key",
        [
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA"),
            (SOURCE_BUCKET, "other/2024-01-02-00-00-00-BBBB"),
        ],
        2,
        None,
        id="without_size_column",
    ),
]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("file_schema", "rows", "expected_count", "expected_size"),
    _API_PARAMS,
)
def test_get_log_bucket_stats(
    tmp_path: pathlib.Path,
    file_schema: str,
    rows: list[tuple],
    expected_count: int,
    expected_size: int | None,
) -> None:
    """
    All inventory keys are counted regardless of their path prefix.

    Covers the ``Size`` column present and absent cases.  Also validates that
    the return value is a ``LogBucketStats`` typed dict with the expected keys.
    """
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=rows,
        file_schema=file_schema,
    )

    stats = get_log_bucket_stats(inventory_directory=inventory_dir)

    assert stats["file_count"] == expected_count
    assert stats["total_size_bytes"] == expected_size
    # Structural check: must be a dict with both expected keys.
    assert isinstance(stats, dict)
    assert "file_count" in stats
    assert "total_size_bytes" in stats


# ---------------------------------------------------------------------------
# CLI command tests
# ---------------------------------------------------------------------------

_CLI_PARAMS = [
    pytest.param(
        "Bucket, Key, Size",
        [
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 100),
            (SOURCE_BUCKET, "other/2024-01-02-00-00-00-BBBB", 400),
            (SOURCE_BUCKET, "other/2024-01-02-00-05-00-CCCC", 500),
        ],
        ["File count", "3"],
        id="with_size_column",
    ),
    pytest.param(
        "Bucket, Key",
        [
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA"),
            (SOURCE_BUCKET, "logs/2024-01-02-00-00-00-BBBB"),
            (SOURCE_BUCKET, "other/2024-01-03-00-00-00-CCCC"),
        ],
        ["File count", "3"],
        id="without_size_column",
    ),
]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("file_schema", "rows", "expected_in_output"),
    _CLI_PARAMS,
)
def test_stats_cli(
    tmp_path: pathlib.Path,
    file_schema: str,
    rows: list[tuple],
    expected_in_output: list[str],
) -> None:
    """
    The 'stats' CLI command counts all inventory keys and produces the expected output.

    Covers the ``Size`` column present and absent cases, and validates that the
    ``File count`` label always appears.
    """
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=rows,
        file_schema=file_schema,
    )
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        s3logextraction_cli,
        [
            "stats",
            "--inventory",
            str(inventory_dir),
            "--cache",
            str(cache_dir),
            "--encryption",
            "false",
        ],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    for expected in expected_in_output:
        assert expected in result.output


@pytest.mark.ai_generated
def test_get_extraction_completion(
    tmp_path: pathlib.Path,
) -> None:
    """
    Completion helper compares end-record count against inventory file count.

    Also verifies that duplicate record entries are de-duplicated.
    """
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=[
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 100),
            (SOURCE_BUCKET, "logs/2024-01-01-00-05-00-BBBB", 200),
            (SOURCE_BUCKET, "logs/2024-01-02-00-00-00-CCCC", 300),
            (SOURCE_BUCKET, "logs/2024-01-02-00-05-00-DDDD", 400),
        ],
        file_schema="Bucket, Key, Size",
    )

    cache_dir = tmp_path / "cache"
    records_dir = cache_dir / "records"
    records_dir.mkdir(parents=True)
    (records_dir / "CustomExtractor-processing-end.txt").write_text(
        "2024-01-01-00-00-00-AAAA\n2024-01-01-00-00-00-AAAA\n2024-01-01-00-05-00-BBBB\n"
    )
    (records_dir / "CustomExtractor-processing-start.txt").write_text("2024-01-02-00-00-00-CCCC\n")

    completion = get_extraction_completion(inventory_directory=inventory_dir, cache_directory=cache_dir)

    assert completion["processed_file_count"] == 2
    assert completion["inventory_file_count"] == 4
    assert completion["percent_complete"] == 50.0


@pytest.mark.ai_generated
def test_completion_cli(
    tmp_path: pathlib.Path,
) -> None:
    """
    The completion CLI reports processed count, inventory count, and % complete.
    """
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=[
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 100),
            (SOURCE_BUCKET, "logs/2024-01-01-00-05-00-BBBB", 200),
            (SOURCE_BUCKET, "logs/2024-01-02-00-00-00-CCCC", 300),
            (SOURCE_BUCKET, "logs/2024-01-02-00-05-00-DDDD", 400),
        ],
        file_schema="Bucket, Key, Size",
    )

    cache_dir = tmp_path / "cache"
    records_dir = cache_dir / "records"
    records_dir.mkdir(parents=True)
    (records_dir / "AnotherName-processing-end.txt").write_text("2024-01-01-00-00-00-AAAA\n2024-01-01-00-05-00-BBBB\n")

    runner = CliRunner()
    result = runner.invoke(
        s3logextraction_cli,
        ["completion", "--inventory", str(inventory_dir), "--cache", str(cache_dir)],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert "Processed files" in result.output
    assert "Inventory files" in result.output
    assert "Percent complete" in result.output
    assert "50.00%" in result.output
    assert "Total size (B)" not in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("command", "target_function_name"),
    [
        pytest.param(["stop"], "stop_extraction", id="stop"),
        pytest.param(["reset", "extraction"], "reset_extraction", id="reset_extraction"),
        pytest.param(["update", "ip", "coordinates"], "update_region_code_coordinates", id="update_ip_coordinates"),
        pytest.param(["update", "summaries"], "generate_summaries", id="update_summaries"),
        pytest.param(["update", "totals"], "generate_all_dataset_totals", id="update_totals"),
    ],
)
def test_cli_commands_forward_cache_directory(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, command: list[str], target_function_name: str
) -> None:
    """
    Commands backed by cache-aware APIs accept ``--cache`` and forward it correctly.
    """
    captured: dict[str, object] = {}

    def _stub(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_module, target_function_name, _stub)

    cache_dir = tmp_path / "custom-cache"
    runner = CliRunner()
    result = runner.invoke(s3logextraction_cli, [*command, "--cache", str(cache_dir)])

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert captured["cache_directory"] == cache_dir


@pytest.mark.ai_generated
def test_update_summaries_archive_forwards_cache_directory(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    ``update summaries --mode archive`` passes ``cache_directory`` directly to ``generate_archive_summaries``.
    """
    captured: dict[str, pathlib.Path | list[str] | None] = {}

    def _stub_generate_archive_summaries(
        cache_directory: pathlib.Path | str | None = None,
        asset_types_in_order: tuple[str, ...] | list[str] | None = None,
        region_disclosure_threshold: int = 5,
    ) -> None:
        captured["cache_directory"] = pathlib.Path(cache_directory) if cache_directory is not None else None
        captured["asset_types_in_order"] = list(asset_types_in_order) if asset_types_in_order is not None else None
        captured["region_disclosure_threshold"] = region_disclosure_threshold

    monkeypatch.setattr(cli_module, "generate_archive_summaries", _stub_generate_archive_summaries)

    cache_dir = tmp_path / "custom-cache"
    runner = CliRunner()
    result = runner.invoke(
        s3logextraction_cli,
        [
            "update",
            "summaries",
            "--mode",
            "archive",
            "--asset-types-in-order",
            "Video,Neurophysiology,Miscellaneous",
            "--threshold",
            "7",
            "--cache",
            str(cache_dir),
        ],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert captured["cache_directory"] == cache_dir
    assert captured["asset_types_in_order"] == ["Video", "Neurophysiology", "Miscellaneous"]
    assert captured["region_disclosure_threshold"] == 7


@pytest.mark.ai_generated
def test_update_totals_archive_forwards_cache_directory(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    ``update totals --mode archive`` passes ``cache_directory`` directly to ``generate_archive_totals``.
    """
    captured: dict[str, pathlib.Path] = {}

    def _stub_generate_archive_totals(cache_directory: pathlib.Path | str | None = None) -> None:
        captured["cache_directory"] = pathlib.Path(cache_directory) if cache_directory is not None else None

    monkeypatch.setattr(cli_module, "generate_archive_totals", _stub_generate_archive_totals)

    cache_dir = tmp_path / "custom-cache"
    runner = CliRunner()
    result = runner.invoke(
        s3logextraction_cli,
        ["update", "totals", "--mode", "archive", "--cache", str(cache_dir)],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert captured["cache_directory"] == cache_dir


# ---------------------------------------------------------------------------
# get_ip_stats tests
# ---------------------------------------------------------------------------


def _write_plaintext_ips_txt(cache_dir: pathlib.Path, subpath: str, ips: list[str]) -> None:
    """Write a plaintext (unencrypted) ips.txt file at cache_dir/subpath/ips.txt."""
    target = cache_dir / subpath
    target.mkdir(parents=True, exist_ok=True)
    (target / "ips.txt").write_text("\n".join(ips) + "\n")


@pytest.mark.ai_generated
def test_get_ip_stats_all_categories(tmp_path: pathlib.Path) -> None:
    """
    get_ip_stats resolves every extracted IP and bins it into its classification category.

    Writes plaintext ips.txt files whose addresses resolve to every category and asserts the counts and the
    per-category percentages of the extracted total.
    """
    all_ips = [
        "1.1.1.1",
        "2.2.2.2",
        "3.3.3.3",
        "4.4.4.4",
        "5.5.5.5",
        "6.6.6.6",
        "7.7.7.7",
        "8.8.8.8",
        "9.9.9.9",
        "10.10.10.10",
        "11.11.11.11",  # repeated below, so it is one extracted IP
    ]
    _write_plaintext_ips_txt(tmp_path, "extraction/dataset/asset1", all_ips[:6])
    _write_plaintext_ips_txt(tmp_path, "extraction/dataset/asset2", all_ips[6:] + ["11.11.11.11"])

    region_resolver = MappingRegionResolver(
        {
            "1.1.1.1": "USA/CA",  # determined
            "2.2.2.2": "AUS/NSW",  # determined
            "3.3.3.3": "AUS",  # determined (country only)
            "4.4.4.4": "unknown",  # unknown
            "5.5.5.5": "bogon",  # bogon
            "6.6.6.6": "bogon",  # bogon
            "7.7.7.7": "VPN",  # vpn
            "8.8.8.8": "VPN/datacenter",  # vpn (sub-label)
            "9.9.9.9": "AWS/us-east-1",  # cloud_service
            "10.10.10.10": "GCP/us-central1",  # cloud_service
            "11.11.11.11": "GitHub",  # github
        }
    )

    stats = get_ip_stats(cache_directory=tmp_path, use_encryption=False, region_resolver=region_resolver)

    assert stats["extracted_ip_count"] == 11

    assert stats["determined"]["count"] == 3
    assert stats["unknown"]["count"] == 1
    assert stats["bogon"]["count"] == 2
    assert stats["vpn"]["count"] == 2
    assert stats["cloud_service"]["count"] == 2
    assert stats["github"]["count"] == 1

    assert abs(stats["determined"]["percent"] - 3 / 11 * 100) < 0.01
    assert abs(stats["cloud_service"]["percent"] - 2 / 11 * 100) < 0.01


@pytest.mark.ai_generated
def test_get_ip_stats_empty_extraction(tmp_path: pathlib.Path) -> None:
    """get_ip_stats returns zeros and 0.0% for an empty extraction cache, without resolving anything."""
    (tmp_path / "extraction").mkdir(parents=True)

    with patch("s3_log_extraction.utils.inventory.IpRegionResolver") as mock_resolver:
        stats = get_ip_stats(cache_directory=tmp_path, use_encryption=False)
    mock_resolver.assert_not_called()

    assert stats["extracted_ip_count"] == 0
    for key in ("determined", "unknown", "bogon", "vpn", "cloud_service", "github"):
        assert stats[key]["count"] == 0  # type: ignore[literal-required]
        assert stats[key]["percent"] == 0.0  # type: ignore[literal-required]


@pytest.mark.ai_generated
def test_get_ip_stats_missing_extraction_directory(tmp_path: pathlib.Path) -> None:
    """get_ip_stats returns zeros when nothing has been extracted yet."""
    stats = get_ip_stats(cache_directory=tmp_path, use_encryption=False)

    assert stats["extracted_ip_count"] == 0
