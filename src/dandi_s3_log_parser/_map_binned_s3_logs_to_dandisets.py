import os
import pathlib

import dandi.dandiapi
import natsort
import pandas
import tqdm
from pydantic import DirectoryPath, validate_call

from ._ip_utils import _load_ip_hash_cache, _save_ip_hash_cache, get_region_from_ip_address


@validate_call
def map_binned_s3_logs_to_dandisets(
    binned_s3_logs_folder_path: DirectoryPath,
    mapped_s3_logs_folder_path: DirectoryPath,
    excluded_dandisets: list[str] | None = None,
    restrict_to_dandisets: list[str] | None = None,
    dandiset_limit: int | None = None,
) -> None:
    """
    Iterate over all dandisets and create a single .tsv per asset per dandiset version.

    Also creates a summary file per dandiset that has binned activity per day.

    Requires the `ipinfo` environment variables to be set (`IPINFO_CREDENTIALS` and `IP_HASH_SALT`).

    Parameters
    ----------
    binned_s3_logs_folder_path : DirectoryPath
        The path to the folder containing the reduced S3 log files.
    mapped_s3_logs_folder_path : DirectoryPath
        The path to the folder where the mapped logs will be saved.
    excluded_dandisets : list of str, optional
        A list of Dandiset IDs to exclude from processing.
    restrict_to_dandisets : list of str, optional
        A list of Dandiset IDs to exclusively process.
    dandiset_limit : int, optional
        The maximum number of Dandisets to process per call.
        Useful for quick testing.
    """
    if "IPINFO_CREDENTIALS" not in os.environ:  # pragma: no cover
        message = "The environment variable 'IPINFO_CREDENTIALS' must be set to import `dandi_s3_log_parser`!"
        raise ValueError(message)

    if "IP_HASH_SALT" not in os.environ:  # pragma: no cover
        message = (
            "The environment variable 'IP_HASH_SALT' must be set to import `dandi_s3_log_parser`! "
            "To retrieve the value, set a temporary value to this environment variable "
            "and then use the `get_hash_salt` helper function and set it to the correct value."
        )
        raise ValueError(message)

    if excluded_dandisets is not None and restrict_to_dandisets is not None:
        message = "Only one of `exclude_dandisets` or `restrict_to_dandisets` can be passed, not both!"
        raise ValueError(message)

    excluded_dandisets = excluded_dandisets or []
    restrict_to_dandisets = restrict_to_dandisets or []

    # TODO: add mtime record for binned files to determine if update is needed

    client = dandi.dandiapi.DandiAPIClient()

    ip_hash_to_region = _load_ip_hash_cache(name="region")
    ip_hash_not_in_services = _load_ip_hash_cache(name="services")

    if len(restrict_to_dandisets) != 0:
        current_dandisets = [client.get_dandiset(dandiset_id=dandiset_id) for dandiset_id in restrict_to_dandisets]
    else:
        current_dandisets = [
            dandiset for dandiset in client.get_dandisets() if dandiset.identifier not in excluded_dandisets
        ]
    current_dandisets = current_dandisets[:dandiset_limit]

    for dandiset in tqdm.tqdm(
        iterable=current_dandisets,
        total=len(current_dandisets),
        desc="Mapping reduced logs to Dandisets...",
        position=0,
        leave=True,
        mininterval=5.0,
        smoothing=0,
    ):
        _map_binned_logs_to_dandiset(
            dandiset=dandiset,
            binned_s3_logs_folder_path=binned_s3_logs_folder_path,
            dandiset_logs_folder_path=mapped_s3_logs_folder_path,
            client=client,
            ip_hash_to_region=ip_hash_to_region,
            ip_hash_not_in_services=ip_hash_not_in_services,
        )

    _save_ip_hash_cache(name="region", ip_cache=ip_hash_to_region)
    _save_ip_hash_cache(name="services", ip_cache=ip_hash_not_in_services)

    return None


def _map_binned_logs_to_dandiset(
    dandiset: dandi.dandiapi.RemoteDandiset,
    binned_s3_logs_folder_path: pathlib.Path,
    dandiset_logs_folder_path: pathlib.Path,
    client: dandi.dandiapi.DandiAPIClient,
    ip_hash_to_region: dict[str, str],
    ip_hash_not_in_services: dict[str, bool],
) -> None:
    dandiset_id = dandiset.identifier
    dandiset_log_folder_path = dandiset_logs_folder_path / dandiset_id

    dandiset_versions = list(dandiset.get_versions())
    for version in tqdm.tqdm(
        iterable=dandiset_versions,
        total=len(dandiset_versions),
        desc=f"Mapping Dandiset {dandiset_id} versions...",
        position=1,
        leave=False,
        mininterval=5.0,
        smoothing=0,
    ):
        version_id = version.identifier
        dandiset_version_log_folder_path = dandiset_log_folder_path / version_id

        dandiset_version = client.get_dandiset(dandiset_id=dandiset_id, version_id=version_id)

        all_activity_for_version = []
        dandiset_version_assets = list(dandiset_version.get_assets())
        for asset in tqdm.tqdm(
            iterable=dandiset_version_assets,
            total=len(dandiset_version_assets),
            desc="Mapping assets...",
            position=2,
            leave=False,
            mininterval=5.0,
            smoothing=0,
        ):
            asset_as_path = pathlib.Path(asset.path)
            asset_suffixes = asset_as_path.suffixes

            # Removing suffixes works fine on NWB Dandisets
            # But the BIDS standard allows files of the same stems to have different suffixes
            # Thus we must keep the suffix information to disambiguate the mapped TSV files
            dandi_filename = asset_as_path.name.replace(".", "_")

            is_asset_zarr = ".zarr" in asset_suffixes
            if is_asset_zarr:
                blob_id = asset.zarr
                binned_s3_log_file_path = binned_s3_logs_folder_path / "zarr" / f"{blob_id}.tsv"
            else:
                blob_id = asset.blob
                binned_s3_log_file_path = (
                    binned_s3_logs_folder_path / "blobs" / blob_id[:3] / blob_id[3:6] / f"{blob_id}.tsv"
                )

            # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
            # Just copy the file over instead of reprocessing

            if not binned_s3_log_file_path.exists():
                continue  # No reduced logs found (possible asset was never accessed); skip to next asset

            reduced_s3_log_binned_by_blob_id = pandas.read_table(filepath_or_buffer=binned_s3_log_file_path, header=0)

            reduced_s3_log_binned_by_blob_id["region"] = [
                get_region_from_ip_address(
                    ip_address=ip_address,
                    ip_hash_to_region=ip_hash_to_region,
                    ip_hash_not_in_services=ip_hash_not_in_services,
                )
                for ip_address in reduced_s3_log_binned_by_blob_id["ip_address"]
            ]

            reordered_reduced_s3_log = reduced_s3_log_binned_by_blob_id.reindex(
                columns=("timestamp", "bytes_sent", "region")
            )
            reordered_reduced_s3_log.sort_values(by="timestamp", key=natsort.natsort_keygen(), inplace=True)
            reordered_reduced_s3_log.index = range(len(reordered_reduced_s3_log))

            dandiset_version_log_folder_path.mkdir(parents=True, exist_ok=True)
            version_asset_file_path = dandiset_version_log_folder_path / f"{dandi_filename}.tsv"
            reordered_reduced_s3_log.to_csv(
                path_or_buf=version_asset_file_path, mode="w", sep="\t", header=True, index=True
            )

            reordered_reduced_s3_log["date"] = [entry[:10] for entry in reordered_reduced_s3_log["timestamp"]]

            reordered_reduced_s3_log_aggregated = reordered_reduced_s3_log.groupby("date", as_index=False)[
                "bytes_sent"
            ].agg([list, "sum"])
            reordered_reduced_s3_log_aggregated.rename(columns={"sum": "bytes_sent"}, inplace=True)

            reordered_reduced_s3_log_binned_per_day = reordered_reduced_s3_log_aggregated.reindex(
                columns=("date", "bytes_sent")
            )
            reordered_reduced_s3_log_binned_per_day.sort_values(by="date", key=natsort.natsort_keygen(), inplace=True)

            all_activity_for_version.append(reordered_reduced_s3_log_binned_per_day)

        if len(all_activity_for_version) == 0:
            continue  # No reduced logs found (possible dandiset version was never accessed); skip to next version

        summary_logs = pandas.concat(objs=all_activity_for_version, ignore_index=True)
        summary_logs.sort_values(by="date", key=natsort.natsort_keygen(), inplace=True)

        summary_file_path = dandiset_version_log_folder_path / "summary.tsv"
        summary_logs.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)

    return None
