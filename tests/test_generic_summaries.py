import json
import pathlib
import secrets
import shutil
import unittest.mock

import geoip2.errors
import pandas
import py
import pytest

import s3_log_extraction
from s3_log_extraction.ip_utils import MappingRegionResolver

# A view is a maximal run of streaming requests from one IP to one asset separated by no more than 8 hours.
# The timestamps below are all on 2025-01-01 unless noted, written in the extraction cache's YYMMDDHHmmss format.
_STREAMING = 0
_DOWNLOAD = 1


def _write_asset(
    *,
    asset_directory: pathlib.Path,
    requests: list[tuple[str, int, str]],
    use_encryption: bool = False,
) -> None:
    """
    Write the line-aligned per-request files of a single synthetic asset.

    Each entry of ``requests`` is a ``(timestamp, download, ip)`` triplet, where ``timestamp`` uses the
    ``YYMMDDHHmmss`` format of the extraction cache and ``download`` is ``1`` for a full download or
    ``0`` for a streaming request.
    """
    asset_directory.mkdir(parents=True, exist_ok=True)

    timestamps = [timestamp for timestamp, _, _ in requests]
    downloads = [str(download) for _, download, _ in requests]
    ips = [ip for _, _, ip in requests]

    (asset_directory / "timestamps.txt").write_text("\n".join(timestamps) + "\n")
    (asset_directory / "download.txt").write_text("\n".join(downloads) + "\n")
    (asset_directory / "bytes_sent.txt").write_text("\n".join(["1"] * len(requests)) + "\n")
    s3_log_extraction.utils.write_text_to_file(
        file_path=asset_directory / "ips.txt", text="\n".join(ips) + "\n", use_encryption=use_encryption
    )


def _write_asset_spanning_regions(*, asset_directory: pathlib.Path, regions: list[str]) -> MappingRegionResolver:
    """
    Write an asset streamed once from each of the given regions, by a distinct requester per region.

    The requesters are drawn from the TEST-NET-1 documentation range, and the returned resolver stands in for a
    geolocation of them to the given regions.
    """
    requests = [(f"2501010000{index:02d}", _STREAMING, f"192.0.2.{index}") for index in range(len(regions))]
    _write_asset(asset_directory=asset_directory, requests=requests)
    return MappingRegionResolver({f"192.0.2.{index}": region for index, region in enumerate(regions)})


def test_generic_summaries(tmpdir: py.path.local, mocked_region_resolver: MappingRegionResolver):
    test_dir = pathlib.Path(tmpdir)

    base_tests_dir = pathlib.Path(__file__).parent
    expected_output_dir = base_tests_dir / "expected_output"
    expected_extraction_dir = expected_output_dir / "extraction"
    expected_summaries_dir = expected_output_dir / "summaries"

    test_extraction_dir = test_dir / "extraction"
    test_summary_dir = test_dir / "summaries"
    shutil.copytree(src=expected_extraction_dir, dst=test_extraction_dir)

    # Every requester of the example logs is a documentation-range address, which a real resolution labels
    # `bogon`; the mocked resolver stands in for one so that the summaries have regions to report
    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=False, region_resolver=mocked_region_resolver
    )
    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)

    test_file_paths = {path.relative_to(test_summary_dir): path for path in test_summary_dir.rglob(pattern="*.tsv")}
    expected_file_paths = {
        path.relative_to(expected_summaries_dir): path for path in expected_summaries_dir.rglob(pattern="*.tsv")
    }
    # No by_region.tsv is among them: every requester of this example data is unresolved, so no update to a
    # by-region summary ever spans enough regions to be published
    assert set(test_file_paths.keys()) == set(expected_file_paths.keys())

    for expected_file_path in expected_file_paths.values():
        relative_file_path = expected_file_path.relative_to(expected_summaries_dir)
        test_file_path = test_summary_dir / relative_file_path

        test_mapped_log = pandas.read_table(filepath_or_buffer=test_file_path, index_col=0)
        expected_mapped_log = pandas.read_table(filepath_or_buffer=expected_file_path, index_col=0)

        # Pandas assertion makes no reference to the case being tested when it fails
        try:
            pandas.testing.assert_frame_equal(left=test_mapped_log, right=expected_mapped_log)
        except AssertionError as exception:
            message = (
                f"\n\nTest file path: {test_file_path}\nExpected file path: {expected_file_path}\n\n"
                f"{str(exception)}\n\n"
            )
            raise AssertionError(message)

    # Verify requester_count.tsv files
    test_tsv_paths = {
        path.relative_to(test_summary_dir): path for path in test_summary_dir.rglob(pattern="requester_count.tsv")
    }
    expected_tsv_paths = {
        path.relative_to(expected_summaries_dir): path
        for path in expected_summaries_dir.rglob(pattern="requester_count.tsv")
    }
    assert set(test_tsv_paths.keys()) == set(expected_tsv_paths.keys())

    for relative_path, expected_txt_path in expected_tsv_paths.items():
        test_txt_path = test_summary_dir / relative_path
        assert test_txt_path.read_text().strip() == expected_txt_path.read_text().strip(), (
            f"\n\nMismatch in {relative_path}:\n"
            f"  test:     {test_txt_path.read_text().strip()!r}\n"
            f"  expected: {expected_txt_path.read_text().strip()!r}\n"
        )

    # Verify totals.json
    test_totals = json.loads((test_summary_dir / "totals.json").read_text())
    expected_totals = json.loads((expected_summaries_dir / "totals.json").read_text())
    assert (
        test_totals == expected_totals
    ), f"\n\ntotals.json mismatch:\n  test:     {test_totals}\n  expected: {expected_totals}\n"

    # Verify archive_totals.json
    test_archive_totals = json.loads((test_summary_dir / "archive_totals.json").read_text())
    expected_archive_totals = json.loads((expected_summaries_dir / "archive_totals.json").read_text())
    assert (
        test_archive_totals == expected_archive_totals
    ), f"\n\narchive_totals.json mismatch:\n  test:     {test_archive_totals}\n  expected: {expected_archive_totals}\n"


def test_generate_all_dataset_totals_skips_archive(tmpdir: py.path.local):
    """Verify that the 'archive' subdirectory is excluded from dataset totals."""
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    # Set up a real dataset summary
    dataset_dir = summary_dir / "ds001161"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t1194564\t4\t3\n"
    )

    # Set up an archive summary that should be excluded
    archive_dir = summary_dir / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t7481053\t7\t5\n"
    )

    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)

    totals = json.loads((summary_dir / "totals.json").read_text())
    assert "ds001161" in totals, "'ds001161' should be present in totals.json"
    assert "archive" not in totals, "'archive' should be excluded from totals.json"


@pytest.mark.ai_generated
def test_generate_archive_totals_raises_without_archive_requester_count(tmpdir: py.path.local) -> None:
    """Archive totals should fail if archive requester count has not been generated."""
    test_dir = pathlib.Path(tmpdir)
    archive_dir = test_dir / "summaries" / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t7481053\t7\t5\n"
    )

    with pytest.raises(FileNotFoundError, match="Archive requester count file not found"):
        s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)


@pytest.mark.ai_generated
def test_generate_archive_totals_raises_without_archive_by_day_summary(tmpdir: py.path.local) -> None:
    """Archive totals are read from the archive by-day summary, so they cannot be taken without it."""
    test_dir = pathlib.Path(tmpdir)
    archive_dir = test_dir / "summaries" / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "requester_count.tsv").write_text("100\n")

    with pytest.raises(FileNotFoundError, match="Archive by-day summary file not found"):
        s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)


@pytest.mark.ai_generated
def test_generate_archive_totals_reports_true_counts(tmpdir: py.path.local) -> None:
    """Archive totals are the true sums of the archive by-day summary, however small they are."""
    test_dir = pathlib.Path(tmpdir)
    archive_dir = test_dir / "summaries" / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\tnumber_of_views\n2026-01-01\t10\t4\t3\t1\n"
    )
    (archive_dir / "requester_count.tsv").write_text("2\n")

    s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)

    archive_totals = json.loads((test_dir / "summaries" / "archive_totals.json").read_text())
    assert archive_totals["total_bytes_sent"] == 10
    assert archive_totals["total_number_of_requests"] == 4
    assert archive_totals["total_number_of_downloads"] == 3
    assert archive_totals["total_number_of_views"] == 1
    assert archive_totals["number_of_requesters"] == 2


@pytest.mark.ai_generated
def test_generate_all_dataset_totals_reports_true_counts(tmpdir: py.path.local) -> None:
    """Dataset totals are the true sums of the dataset by-day summary, however small they are."""
    test_dir = pathlib.Path(tmpdir)
    dataset_dir = test_dir / "summaries" / "ds001"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\tnumber_of_views\n2026-01-01\t10\t4\t3\t1\n"
    )
    (dataset_dir / "requester_count.tsv").write_text("2\n")

    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)

    totals = json.loads((test_dir / "summaries" / "totals.json").read_text())
    assert totals["ds001"]["total_bytes_sent"] == 10
    assert totals["ds001"]["total_number_of_requests"] == 4
    assert totals["ds001"]["total_number_of_downloads"] == 3
    assert totals["ds001"]["total_number_of_views"] == 1
    assert totals["ds001"]["number_of_requesters"] == 2


@pytest.mark.ai_generated
def test_totals_count_regions_only_from_a_published_by_region_summary(tmpdir: py.path.local) -> None:
    """A dataset whose by-region summary is still withheld reports no regions and no countries."""
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    withheld_dir = summary_dir / "ds001"
    withheld_dir.mkdir(parents=True)
    (withheld_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t4\t3\n"
    )

    published_dir = summary_dir / "ds002"
    published_dir.mkdir(parents=True)
    (published_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t4\t3\n"
    )
    (published_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n"
        "US/California\t4\t2\t1\n"
        "DE/Berlin\t4\t1\t1\n"
        "missing\t2\t1\t1\n"
    )

    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)

    totals = json.loads((summary_dir / "totals.json").read_text())
    assert totals["ds001"]["number_of_unique_regions"] == 0
    assert totals["ds001"]["number_of_unique_countries"] == 0
    assert totals["ds002"]["number_of_unique_regions"] == 3
    assert totals["ds002"]["number_of_unique_countries"] == 2  # "missing" names no country


@pytest.mark.ai_generated
def test_totals_count_an_aws_region_and_its_country_once(tmpdir: py.path.local) -> None:
    """An AWS region is attributed to the alpha-3 code of its country, so it does not add a second country."""
    test_dir = pathlib.Path(tmpdir)
    dataset_dir = test_dir / "summaries" / "ds001"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t4\t3\n"
    )
    (dataset_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n"
        "USA/CA\t4\t2\t1\n"
        "AWS/us-east-1\t4\t1\t1\n"
        "DEU/BE\t2\t1\t1\n"
    )

    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)

    totals = json.loads((test_dir / "summaries" / "totals.json").read_text())
    assert totals["ds001"]["number_of_unique_regions"] == 3
    assert totals["ds001"]["number_of_unique_countries"] == 2  # USA (twice) and DEU


@pytest.mark.ai_generated
def test_generate_archive_summaries_reports_true_counts(tmpdir: py.path.local) -> None:
    """The archive by-day summary aggregates the true values of the dataset summaries."""
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("60\n")

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("40\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)

    archive_by_day = pandas.read_table(filepath_or_buffer=summary_dir / "archive" / "by_day.tsv")
    assert archive_by_day.loc[0, "bytes_sent"] == 50
    assert archive_by_day.loc[0, "number_of_requests"] == 3
    assert archive_by_day.loc[0, "number_of_downloads"] == 2

    # Every dataset by-region summary is still withheld, so the archive has nothing to aggregate for one
    assert not (summary_dir / "archive" / "by_region.tsv").exists()


@pytest.mark.ai_generated
def test_generate_archive_summaries_leaves_requester_count_untouched(tmpdir: py.path.local) -> None:
    """
    The archive requester count is deduplicated across datasets, so the archive step must not rewrite it.

    Summing the per-dataset counts would count a requester once per dataset it accessed.
    """
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("60\n")

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("40\n")

    # The deduplicated count written by `generate_summaries`; the two datasets share 25 requesters
    archive_dir = summary_dir / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "requester_count.tsv").write_text("75\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)

    archive_requester_count_file_path = archive_dir / "requester_count.tsv"
    assert archive_requester_count_file_path.read_text().strip() == "75"


@pytest.mark.ai_generated
def test_generate_archive_summaries_writes_no_requester_count(tmpdir: py.path.local) -> None:
    """Without a deduplicated count to preserve, the archive step writes none rather than an inflated sum."""
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("60\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)

    assert not (summary_dir / "archive" / "requester_count.tsv").exists()


@pytest.mark.ai_generated
def test_generate_archive_summaries_aggregates_optional_by_asset_type_per_week(tmpdir: py.path.local) -> None:
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("20\n")
    (ds001_dir / "by_asset_type_per_week.tsv").write_text(
        "week_start\tNeurophysiology\tMiscellaneous\n2025-12-29\t1\t2\n2026-01-05\t3\t4\n"
    )

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("20\n")
    (ds002_dir / "by_asset_type_per_week.tsv").write_text("week_start\tVideo\n2025-12-29\t5\n2026-01-05\t7\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)

    archive_file_path = summary_dir / "archive" / "by_asset_type_per_week.tsv"
    assert archive_file_path.exists()
    archive_summary = pandas.read_table(filepath_or_buffer=archive_file_path)
    expected_summary = pandas.DataFrame(
        data={
            "week_start": ["2025-12-29", "2026-01-05"],
            "Miscellaneous": [2, 4],
            "Neurophysiology": [1, 3],
            "Video": [5, 7],
        }
    )
    pandas.testing.assert_frame_equal(left=archive_summary, right=expected_summary)


@pytest.mark.ai_generated
def test_generate_archive_summaries_accepts_custom_asset_type_order(tmpdir: py.path.local) -> None:
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("20\n")
    (ds001_dir / "by_asset_type_per_week.tsv").write_text(
        "week_start\tNeurophysiology\tMiscellaneous\n2025-12-29\t1\t2\n"
    )

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("20\n")
    (ds002_dir / "by_asset_type_per_week.tsv").write_text("week_start\tVideo\n2025-12-29\t5\n")

    s3_log_extraction.summarize.generate_archive_summaries(
        cache_directory=test_dir, asset_types_in_order=["Video", "Neurophysiology", "Miscellaneous"]
    )

    archive_file_path = summary_dir / "archive" / "by_asset_type_per_week.tsv"
    archive_summary = pandas.read_table(filepath_or_buffer=archive_file_path)
    assert archive_summary.columns.tolist() == ["week_start", "Video", "Neurophysiology", "Miscellaneous"]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("region_label", "expected"),
    [
        # A resolved label pairs a top-level code with a subdivision of it
        ("US/California", True),
        ("DE/Berlin", True),
        ("AWS/us-east-1", True),
        ("GCP/us-central1", True),
        # Unresolved outcomes of geolocation name no place
        ("unknown", False),
        ("undetermined", False),
        ("missing", False),
        ("bogon", False),
        # Neither do services whose region was never reported
        ("GitHub", False),
        ("VPN", False),
        # Nor a ``None`` entry left in the cache by earlier versions
        (None, False),
    ],
)
def test_is_resolved_region(region_label: str | None, expected: bool) -> None:
    """Only labels that name a place, which is to say the ones carrying a slash, are resolved regions."""
    from s3_log_extraction.ip_utils import is_resolved_region

    assert is_resolved_region(region_label) == expected


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("region_label", "expected"),
    [
        # Genuine known cloud service / VPN labels are excluded
        ("GitHub", True),
        ("VPN", True),
        ("AWS/us-east-1", True),
        ("GCP/us-central1", True),
        # Unresolved-location labels are NOT cloud/VPN labels; a real requester's IP
        # simply failed to geolocate (rate limit, quota exceeded, obscure IP, etc.)
        ("unknown", False),
        ("undetermined", False),
        ("missing", False),
        ("bogon", False),
        # Genuine geographic labels are not excluded
        ("US/California", False),
        # A ``None`` entry left in the cache by earlier versions is an unresolved location, not a service
        (None, False),
    ],
)
def test_is_cloud_service_or_vpn_label(region_label: str | None, expected: bool) -> None:
    """Only genuine cloud/VPN service labels are excluded; unresolved-location labels are not."""
    from s3_log_extraction.ip_utils import is_cloud_service_or_vpn_label

    assert is_cloud_service_or_vpn_label(region_label) == expected


@pytest.mark.ai_generated
def test_summaries_geolocate_requesters_while_summarizing(tmpdir: py.path.local) -> None:
    """
    Requesters are resolved during the summary run, against the service ranges and the GeoLite2 database.

    Nothing about any requester's location is written to disk other than the aggregated by-region summary.
    """
    test_dir = pathlib.Path(tmpdir)
    _write_asset(
        asset_directory=test_dir / "extraction" / "ds001" / "asset",
        requests=[
            ("250101000000", _DOWNLOAD, "8.8.8.8"),  # Geolocated
            ("250101000001", _STREAMING, "1.1.1.1"),  # In the database without a subdivision
            ("250101000002", _STREAMING, "9.9.9.9"),  # Not in the database
            ("250101000003", _STREAMING, "203.0.113.7"),  # In a cloud service range, so never geolocated
            ("250101000004", _STREAMING, "192.0.2.1"),  # Not publicly routable
        ],
    )

    city_responses = {
        "8.8.8.8": unittest.mock.MagicMock(
            country=unittest.mock.MagicMock(iso_code="US"), subdivisions=[unittest.mock.MagicMock(iso_code="CA")]
        ),
        "1.1.1.1": unittest.mock.MagicMock(country=unittest.mock.MagicMock(iso_code="AU"), subdivisions=[]),
        "9.9.9.9": geoip2.errors.AddressNotFoundError("not found"),
    }

    def city(ip_address: str) -> object:
        response = city_responses[ip_address]
        if isinstance(response, Exception):
            raise response
        return response

    reader = unittest.mock.MagicMock()
    reader.city.side_effect = city
    service_networks = {"GitHub": [], "AWS": [("203.0.113.0/24", "us-east-1")], "GCP": [], "VPN": []}

    with unittest.mock.patch("s3_log_extraction.ip_utils._resolver.open_geolite2_database", return_value=reader):
        with unittest.mock.patch(
            "s3_log_extraction.ip_utils._resolver.fetch_service_networks", return_value=service_networks
        ):
            # A threshold of zero publishes the by-region summary as soon as one resolved region is updated
            s3_log_extraction.summarize.generate_summaries(
                cache_directory=test_dir, use_encryption=False, region_disclosure_threshold=0
            )

    by_region = pandas.read_table(filepath_or_buffer=test_dir / "summaries" / "ds001" / "by_region.tsv", index_col=0)
    assert sorted(by_region.index) == ["AUS", "AWS/us-east-1", "USA/CA", "bogon", "unknown"]
    assert by_region.loc["USA/CA", "number_of_downloads"] == 1
    assert by_region.loc["AUS", "number_of_views"] == 1
    # The cloud service requester is not a requester, and the database was never asked about it
    assert (test_dir / "summaries" / "ds001" / "requester_count.tsv").read_text().strip() == "4"
    assert sorted(call.args[0] for call in reader.city.call_args_list) == ["1.1.1.1", "8.8.8.8", "9.9.9.9"]
    reader.close.assert_called_once()
    assert not (test_dir / "ips").exists()


@pytest.mark.ai_generated
def test_collect_unique_ips_excludes_known_cloud_service_ips(tmpdir: py.path.local) -> None:
    """Known cloud service/VPN IPs are excluded from the requester count, but unresolved-location IPs are not."""
    from s3_log_extraction.summarize._generate_summaries import _collect_unique_ips

    asset_dir = pathlib.Path(tmpdir) / "asset"
    asset_dir.mkdir(parents=True, exist_ok=True)
    (asset_dir / "ips.txt").write_text("1.2.3.4\n5.6.7.8\n9.10.11.12\n13.14.15.16\n17.18.19.20\n21.22.23.24\n")

    region_resolver = MappingRegionResolver(
        {
            "1.2.3.4": "US/California",
            "5.6.7.8": "GitHub",
            "9.10.11.12": "AWS/us-east-1",
            "13.14.15.16": "VPN",
            "17.18.19.20": "bogon",
            "21.22.23.24": "unknown",
        }
    )

    unique_ips = _collect_unique_ips(
        asset_directories=[asset_dir], use_encryption=False, region_resolver=region_resolver
    )

    assert unique_ips == {"1.2.3.4", "17.18.19.20", "21.22.23.24"}


@pytest.mark.ai_generated
def test_summarize_dataset_requester_count_excludes_known_cloud_service_ips(tmpdir: py.path.local) -> None:
    """The requester count written to disk excludes known cloud service/VPN IPs."""
    from s3_log_extraction.summarize._generate_summaries import _summarize_dataset_requester_count

    asset_dir = pathlib.Path(tmpdir) / "asset"
    asset_dir.mkdir(parents=True, exist_ok=True)
    real_ips = [f"10.0.0.{index}" for index in range(60)]
    cloud_ips = [f"192.168.0.{index}" for index in range(60)]
    (asset_dir / "ips.txt").write_text("\n".join(real_ips + cloud_ips))

    region_resolver = MappingRegionResolver(
        {ip: "US/California" for ip in real_ips} | {ip: "GitHub" for ip in cloud_ips}
    )

    summary_file_path = pathlib.Path(tmpdir) / "requester_count.tsv"
    _summarize_dataset_requester_count(
        asset_directories=[asset_dir],
        summary_file_path=summary_file_path,
        region_resolver=region_resolver,
        use_encryption=False,
    )

    assert summary_file_path.read_text().strip() == "60"


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("requests", "expected_number_of_views"),
    [
        # A single streaming request is a single view
        ([("250101000000", _STREAMING, "192.0.2.0")], 1),
        # A burst of streaming requests, as one exploration of a file emits, collapses into a single view
        (
            [
                ("250101000000", _STREAMING, "192.0.2.0"),
                ("250101000001", _STREAMING, "192.0.2.0"),
                ("250101000135", _STREAMING, "192.0.2.0"),
            ],
            1,
        ),
        # A gap just under 8 hours does not cross the session boundary
        (
            [("250101000000", _STREAMING, "192.0.2.0"), ("250101075900", _STREAMING, "192.0.2.0")],
            1,
        ),
        # A gap of exactly 8 hours is not more than the timeout, so it does not split the session
        (
            [("250101000000", _STREAMING, "192.0.2.0"), ("250101080000", _STREAMING, "192.0.2.0")],
            1,
        ),
        # One second beyond 8 hours does split the session
        (
            [("250101000000", _STREAMING, "192.0.2.0"), ("250101080001", _STREAMING, "192.0.2.0")],
            2,
        ),
        # Timestamps are sorted before gaps are measured, so file order does not matter
        (
            [("250101080001", _STREAMING, "192.0.2.0"), ("250101000000", _STREAMING, "192.0.2.0")],
            2,
        ),
        # Three consecutive days of streaming are three views
        (
            [
                ("250101000000", _STREAMING, "192.0.2.0"),
                ("250102000000", _STREAMING, "192.0.2.0"),
                ("250103000000", _STREAMING, "192.0.2.0"),
            ],
            3,
        ),
        # Full downloads are not views
        (
            [("250101000000", _DOWNLOAD, "192.0.2.0"), ("250101120000", _DOWNLOAD, "192.0.2.0")],
            0,
        ),
        # A full download in the middle of a long gap does not bridge two streaming sessions
        (
            [
                ("250101000000", _STREAMING, "192.0.2.0"),
                ("250101040000", _DOWNLOAD, "192.0.2.0"),
                ("250101090000", _STREAMING, "192.0.2.0"),
            ],
            2,
        ),
        # Each IP is sessionized independently, so interleaved requests do not merge
        (
            [
                ("250101000000", _STREAMING, "192.0.2.0"),
                ("250101040000", _STREAMING, "198.51.100.0"),
                ("250101090000", _STREAMING, "192.0.2.0"),
                ("250101093000", _STREAMING, "198.51.100.0"),
            ],
            3,
        ),
        # A gap that would split one IP's session does not split another's
        (
            [
                ("250101000000", _STREAMING, "192.0.2.0"),
                ("250101000000", _STREAMING, "198.51.100.0"),
                ("250102000000", _STREAMING, "192.0.2.0"),
            ],
            3,
        ),
    ],
)
def test_collect_asset_views(
    tmpdir: py.path.local, requests: list[tuple[str, int, str]], expected_number_of_views: int
) -> None:
    """Streaming sessions are counted per IP, split only by gaps of more than 8 hours."""
    from s3_log_extraction.summarize._generate_summaries import _collect_asset_views

    asset_directory = pathlib.Path(tmpdir) / "asset"
    _write_asset(asset_directory=asset_directory, requests=requests)

    views = _collect_asset_views(asset_directory=asset_directory, use_encryption=False)

    assert len(views) == expected_number_of_views


@pytest.mark.ai_generated
def test_collect_asset_views_respects_custom_session_timeout(tmpdir: py.path.local) -> None:
    """The session timeout is configurable, and the default of 8 hours is applied when it is not overridden."""
    from s3_log_extraction.summarize._generate_summaries import _collect_asset_views
    from s3_log_extraction.summarize.globals import SESSION_TIMEOUT_IN_SECONDS

    asset_directory = pathlib.Path(tmpdir) / "asset"
    _write_asset(
        asset_directory=asset_directory,
        requests=[("250101000000", _STREAMING, "192.0.2.0"), ("250101040000", _STREAMING, "192.0.2.0")],
    )

    assert SESSION_TIMEOUT_IN_SECONDS == 28_800
    assert len(_collect_asset_views(asset_directory=asset_directory, use_encryption=False)) == 1
    assert (
        len(
            _collect_asset_views(
                asset_directory=asset_directory, use_encryption=False, session_timeout_in_seconds=3_600
            )
        )
        == 2
    )


@pytest.mark.ai_generated
@pytest.mark.parametrize("missing_file_name", ["timestamps.txt", "download.txt", "ips.txt"])
def test_collect_asset_views_raises_on_missing_files(tmpdir: py.path.local, missing_file_name: str) -> None:
    """Extraction writes every per-request file together, so an absent one is an unusable cache."""
    from s3_log_extraction.summarize._generate_summaries import _collect_asset_views

    asset_directory = pathlib.Path(tmpdir) / "asset"
    _write_asset(
        asset_directory=asset_directory,
        requests=[("250101000000", _STREAMING, "192.0.2.0"), ("250101000001", _STREAMING, "192.0.2.0")],
    )
    (asset_directory / missing_file_name).unlink()

    with pytest.raises(RuntimeError, match="are incomplete"):
        _collect_asset_views(asset_directory=asset_directory, use_encryption=False)


@pytest.mark.ai_generated
def test_collect_asset_views_raises_on_misaligned_files(tmpdir: py.path.local) -> None:
    """Per-asset files that are not line-aligned cannot be sessionized, so they are refused outright."""
    from s3_log_extraction.summarize._generate_summaries import _collect_asset_views

    asset_directory = pathlib.Path(tmpdir) / "asset"
    _write_asset(
        asset_directory=asset_directory,
        requests=[("250101000000", _STREAMING, "192.0.2.0"), ("250101000001", _STREAMING, "192.0.2.0")],
    )
    (asset_directory / "download.txt").write_text("0\n")

    with pytest.raises(RuntimeError, match="are not line-aligned"):
        _collect_asset_views(asset_directory=asset_directory, use_encryption=False)


@pytest.mark.ai_generated
def test_summaries_report_true_number_of_views(tmpdir: py.path.local) -> None:
    """Per-asset view counts are reported as they are, since an asset path names no requester."""
    test_dir = pathlib.Path(tmpdir)
    dataset_directory = test_dir / "extraction" / "ds001"

    # One view from each of sixty distinct requesters
    _write_asset(
        asset_directory=dataset_directory / "popular.nwb",
        requests=[(f"2501010000{index:02d}", _STREAMING, f"192.0.2.{index}") for index in range(60)],
    )
    # Three views from a single requester
    _write_asset(
        asset_directory=dataset_directory / "quiet.nwb",
        requests=[
            ("250101000000", _STREAMING, "198.51.100.0"),
            ("250102000000", _STREAMING, "198.51.100.0"),
            ("250103000000", _STREAMING, "198.51.100.0"),
        ],
    )

    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=False, region_resolver=MappingRegionResolver({})
    )

    by_asset = pandas.read_table(filepath_or_buffer=test_dir / "summaries" / "ds001" / "by_asset.tsv", index_col=0)
    assert by_asset.columns.tolist() == ["bytes_sent", "number_of_requests", "number_of_downloads", "number_of_views"]
    assert by_asset.loc["popular.nwb", "number_of_views"] == 60
    assert by_asset.loc["quiet.nwb", "number_of_views"] == 3


@pytest.mark.ai_generated
def test_summaries_count_views_with_encryption(tmpdir: py.path.local, monkeypatch: pytest.MonkeyPatch) -> None:
    """Views are counted from encrypted IP files, which is how the extraction cache stores them by default."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", secrets.token_urlsafe(32))

    test_dir = pathlib.Path(tmpdir)
    asset_directory = test_dir / "extraction" / "ds001" / "encrypted.nwb"

    # Two views for each of thirty requesters, split by a gap of more than 8 hours
    requests = []
    for index in range(30):
        ip = f"192.0.2.{index}"
        requests.extend(
            [
                (f"2501010000{index:02d}", _STREAMING, ip),
                (f"2501011200{index:02d}", _STREAMING, ip),
                (f"2501011200{index:02d}", _DOWNLOAD, ip),
            ]
        )
    _write_asset(asset_directory=asset_directory, requests=requests, use_encryption=True)

    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=True, region_resolver=MappingRegionResolver({})
    )

    by_asset = pandas.read_table(filepath_or_buffer=test_dir / "summaries" / "ds001" / "by_asset.tsv", index_col=0)
    assert by_asset.loc["encrypted.nwb", "number_of_views"] == 60


@pytest.mark.ai_generated
def test_view_counts_roll_up_to_dataset_and_archive_totals(tmpdir: py.path.local) -> None:
    """View counts sum across the assets of a dataset and across the datasets of the archive."""
    test_dir = pathlib.Path(tmpdir)
    extraction_directory = test_dir / "extraction"

    # Sixty views on each asset, one from each of sixty distinct requesters, spread over ten regions so that
    # every by-region summary clears the disclosure threshold and the tables can be compared against each other
    ip_to_region = {}
    for dataset_id, asset_name, subnet in (
        ("ds001", "first.nwb", "192.0.2"),
        ("ds001", "second.nwb", "198.51.100"),
        ("ds002", "third.nwb", "203.0.113"),
    ):
        _write_asset(
            asset_directory=extraction_directory / dataset_id / asset_name,
            requests=[(f"2501010000{index:02d}", _STREAMING, f"{subnet}.{index}") for index in range(60)],
        )
        ip_to_region.update({f"{subnet}.{index}": f"US/Subdivision {index % 10}" for index in range(60)})

    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=False, region_resolver=MappingRegionResolver(ip_to_region)
    )
    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)

    summary_directory = test_dir / "summaries"

    # Every table of a dataset accounts for the same views
    for dataset_id, expected_number_of_views in (("ds001", 120), ("ds002", 60)):
        for summary_file_name in ("by_asset.tsv", "by_day.tsv", "by_region.tsv"):
            summary = pandas.read_table(filepath_or_buffer=summary_directory / dataset_id / summary_file_name)
            assert summary["number_of_views"].sum() == expected_number_of_views, summary_file_name

    # And the archive accounts for both datasets
    for summary_file_name in ("by_day.tsv", "by_region.tsv"):
        summary = pandas.read_table(filepath_or_buffer=summary_directory / "archive" / summary_file_name)
        assert summary["number_of_views"].sum() == 180, summary_file_name

    totals = json.loads((summary_directory / "totals.json").read_text())
    assert totals["ds001"]["total_number_of_views"] == 120
    assert totals["ds002"]["total_number_of_views"] == 60

    archive_totals = json.loads((summary_directory / "archive_totals.json").read_text())
    assert archive_totals["total_number_of_views"] == 180


@pytest.mark.ai_generated
def test_views_are_counted_on_the_day_the_session_began(tmpdir: py.path.local) -> None:
    """A session that straddles midnight counts once, on the date of its first request."""
    test_dir = pathlib.Path(tmpdir)

    # Each requester streams just before midnight and again just after, which is one session spanning
    # two dates, plus a second session a full day later
    requests = []
    for index in range(60):
        ip = f"192.0.2.{index}"
        requests.extend(
            [
                (f"2501012359{index % 60:02d}", _STREAMING, ip),
                (f"2501020001{index % 60:02d}", _STREAMING, ip),
                (f"2501032359{index % 60:02d}", _STREAMING, ip),
            ]
        )
    _write_asset(asset_directory=test_dir / "extraction" / "ds001" / "straddling.nwb", requests=requests)

    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=False, region_resolver=MappingRegionResolver({})
    )

    by_day = pandas.read_table(filepath_or_buffer=test_dir / "summaries" / "ds001" / "by_day.tsv", index_col=0)
    assert by_day.loc["2025-01-01", "number_of_views"] == 60
    assert by_day.loc["2025-01-02", "number_of_views"] == 0  # Continuation of the session begun on the 1st
    assert by_day.loc["2025-01-03", "number_of_views"] == 60


@pytest.mark.ai_generated
def test_views_are_attributed_to_the_region_of_their_requester(tmpdir: py.path.local) -> None:
    """Each view is attributed to the region of the single IP that made it."""
    test_dir = pathlib.Path(tmpdir)

    californian_ips = [f"192.0.2.{index}" for index in range(60)]
    berliner_ips = [f"198.51.100.{index}" for index in range(80)]
    _write_asset(
        asset_directory=test_dir / "extraction" / "ds001" / "international.nwb",
        requests=[
            (f"2501010000{index % 60:02d}", _STREAMING, ip) for index, ip in enumerate(californian_ips + berliner_ips)
        ],
    )

    region_resolver = MappingRegionResolver(
        {ip: "US/California" for ip in californian_ips} | {ip: "DE/Berlin" for ip in berliner_ips}
    )

    # Two regions is below the default disclosure threshold, which this test is not about
    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=False, region_disclosure_threshold=1, region_resolver=region_resolver
    )

    by_region = pandas.read_table(filepath_or_buffer=test_dir / "summaries" / "ds001" / "by_region.tsv", index_col=0)
    assert by_region.loc["US/California", "number_of_views"] == 60
    assert by_region.loc["DE/Berlin", "number_of_views"] == 80


@pytest.mark.ai_generated
def test_totals_report_no_views_when_view_counts_are_missing(tmpdir: py.path.local) -> None:
    """A cache summarized before views existed reports no views rather than failing."""
    test_dir = pathlib.Path(tmpdir)
    summary_directory = test_dir / "summaries"

    dataset_directory = summary_directory / "ds001"
    dataset_directory.mkdir(parents=True)
    (dataset_directory / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t60\t60\n"
    )
    (dataset_directory / "requester_count.tsv").write_text("60\n")

    archive_directory = summary_directory / "archive"
    archive_directory.mkdir(parents=True)
    (archive_directory / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t60\t60\n"
    )
    (archive_directory / "requester_count.tsv").write_text("60\n")

    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)

    totals = json.loads((summary_directory / "totals.json").read_text())
    archive_totals = json.loads((summary_directory / "archive_totals.json").read_text())
    assert totals["ds001"]["total_number_of_views"] == 0
    assert archive_totals["total_number_of_views"] == 0


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("number_of_regions", "expected_to_be_published"),
    [
        (1, False),
        (5, False),  # The threshold itself is not enough; the update must move more regions than that
        (6, True),
        (12, True),
    ],
)
def test_by_region_summary_is_published_only_above_the_disclosure_threshold(
    tmpdir: py.path.local, number_of_regions: int, expected_to_be_published: bool
) -> None:
    """A by-region summary is created only once its first update spans more regions than the threshold."""
    test_dir = pathlib.Path(tmpdir)

    region_resolver = _write_asset_spanning_regions(
        asset_directory=test_dir / "extraction" / "ds001" / "asset.nwb",
        regions=[f"US/Subdivision {index}" for index in range(number_of_regions)],
    )

    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=False, region_resolver=region_resolver
    )

    summary_directory = test_dir / "summaries" / "ds001"
    assert (summary_directory / "by_region.tsv").exists() == expected_to_be_published

    # The summaries that name no requester location are written either way, with their true values
    by_day = pandas.read_table(filepath_or_buffer=summary_directory / "by_day.tsv")
    assert by_day.loc[0, "number_of_requests"] == number_of_regions


@pytest.mark.ai_generated
def test_unresolved_region_labels_do_not_count_toward_the_disclosure_threshold(tmpdir: py.path.local) -> None:
    """Labels that name no place cannot make up the regions an update needs to be published."""
    test_dir = pathlib.Path(tmpdir)

    region_resolver = _write_asset_spanning_regions(
        asset_directory=test_dir / "extraction" / "ds001" / "asset.nwb",
        regions=["unknown", "undetermined", "missing", "bogon", "GitHub", "VPN", "US/California"],
    )

    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=False, region_resolver=region_resolver
    )

    assert not (test_dir / "summaries" / "ds001" / "by_region.tsv").exists()


@pytest.mark.ai_generated
def test_by_region_summary_is_not_updated_by_a_small_change(tmpdir: py.path.local) -> None:
    """Activity that moves too few regions leaves the published by-region summary exactly as it was."""
    test_dir = pathlib.Path(tmpdir)
    asset_directory = test_dir / "extraction" / "ds001" / "asset.nwb"
    summary_file_path = test_dir / "summaries" / "ds001" / "by_region.tsv"

    regions = [f"US/Subdivision {index}" for index in range(10)]
    region_resolver = _write_asset_spanning_regions(asset_directory=asset_directory, regions=regions)
    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=False, region_resolver=region_resolver
    )

    published = summary_file_path.read_text()

    # Two more requests from two of the regions already published, which is too small an update to disclose
    region_resolver = _write_asset_spanning_regions(asset_directory=asset_directory, regions=[*regions, *regions[:2]])
    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=False, region_resolver=region_resolver
    )

    assert summary_file_path.read_text() == published

    # The by-day summary meanwhile carries the true count, so the two fall out of step
    by_day = pandas.read_table(filepath_or_buffer=test_dir / "summaries" / "ds001" / "by_day.tsv")
    assert by_day.loc[0, "number_of_requests"] == 12


@pytest.mark.ai_generated
def test_by_region_summary_is_updated_by_a_change_across_enough_regions(tmpdir: py.path.local) -> None:
    """Activity that moves more regions than the threshold republishes the by-region summary."""
    test_dir = pathlib.Path(tmpdir)
    asset_directory = test_dir / "extraction" / "ds001" / "asset.nwb"
    summary_file_path = test_dir / "summaries" / "ds001" / "by_region.tsv"

    regions = [f"US/Subdivision {index}" for index in range(10)]
    region_resolver = _write_asset_spanning_regions(asset_directory=asset_directory, regions=regions)
    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=False, region_resolver=region_resolver
    )

    # Six of the ten regions request again, which is more than the threshold of five
    region_resolver = _write_asset_spanning_regions(asset_directory=asset_directory, regions=[*regions, *regions[:6]])
    s3_log_extraction.summarize.generate_summaries(
        cache_directory=test_dir, use_encryption=False, region_resolver=region_resolver
    )

    by_region = pandas.read_table(filepath_or_buffer=summary_file_path, index_col=0)
    assert by_region["number_of_requests"].sum() == 16
    assert by_region.loc["US/Subdivision 0", "number_of_requests"] == 2
    assert by_region.loc["US/Subdivision 9", "number_of_requests"] == 1


@pytest.mark.ai_generated
def test_archive_by_region_summary_is_published_only_above_the_disclosure_threshold(tmpdir: py.path.local) -> None:
    """The archive by-region summary is held to the same threshold as the dataset summaries it aggregates."""
    test_dir = pathlib.Path(tmpdir)
    summary_directory = test_dir / "summaries"

    dataset_directory = summary_directory / "ds001"
    dataset_directory.mkdir(parents=True)
    (dataset_directory / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t3\t3\n"
    )
    (dataset_directory / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n"
        + "".join(f"US/Subdivision {index}\t1\t1\t1\n" for index in range(3))
    )
    (dataset_directory / "requester_count.tsv").write_text("3\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)

    assert not (summary_directory / "archive" / "by_region.tsv").exists()

    # A second dataset brings the archive to seven regions in total, which clears the threshold
    other_dataset_directory = summary_directory / "ds002"
    other_dataset_directory.mkdir(parents=True)
    (other_dataset_directory / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t4\t4\n"
    )
    (other_dataset_directory / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n"
        + "".join(f"DE/Subdivision {index}\t1\t1\t1\n" for index in range(4))
    )
    (other_dataset_directory / "requester_count.tsv").write_text("4\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)

    archive_by_region = pandas.read_table(filepath_or_buffer=summary_directory / "archive" / "by_region.tsv")
    assert len(archive_by_region) == 7
