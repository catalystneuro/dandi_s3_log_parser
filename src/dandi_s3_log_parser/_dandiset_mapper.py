import os
import pathlib

import dandi.dandiapi
import pandas
import tqdm
from pydantic import DirectoryPath, validate_call

from ._ip_utils import _load_ip_hash_to_region_cache, get_region_from_ip_address


@validate_call
def map_reduced_logs_to_all_dandisets(
    reduced_s3_log_folder_path: DirectoryPath, all_dandiset_logs_folder_path: DirectoryPath
) -> None:
    if "IPINFO_CREDENTIALS" not in os.environ:
        message = "The environment variable 'IPINFO_CREDENTIALS' must be set to import `dandi_s3_log_parser`!"
        raise ValueError(message)  # pragma: no cover

    if "IP_HASH_SALT" not in os.environ:
        message = (
            "The environment variable 'IP_HASH_SALT' must be set to import `dandi_s3_log_parser`! "
            "To retrieve the value, set a temporary value to this environment variable "
            "and then use the `get_hash_salt` helper function and set it to the correct value."
        )
        raise ValueError(message)  # pragma: no cover

    client = dandi.dandiapi.DandiAPIClient()

    ip_hash_to_region = _load_ip_hash_to_region_cache()
    current_dandisets = list(client.get_dandisets())
    for dandiset in tqdm.tqdm(
        iterable=current_dandisets,
        total=len(current_dandisets),
        desc="Mapping reduced logs to Dandisets...",
        position=0,
        mininterval=5.0,
        smoothing=0,
    ):
        _map_reduced_logs_to_dandiset(
            dandiset=dandiset,
            reduced_s3_log_folder_path=reduced_s3_log_folder_path,
            all_dandiset_logs_folder_path=all_dandiset_logs_folder_path,
            client=client,
            ip_hash_to_region=ip_hash_to_region,
        )


def _map_reduced_logs_to_dandiset(
    dandiset: dandi.dandiapi.RemoteDandiset,
    reduced_s3_log_folder_path: pathlib.Path,
    all_dandiset_logs_folder_path: pathlib.Path,
    client: dandi.dandiapi.DandiAPIClient,
    ip_hash_to_region: dict[str, str],
) -> None:
    dandiset_id = dandiset.identifier

    dandiset_log_folder_path = all_dandiset_logs_folder_path / dandiset_id

    for version in dandiset.get_versions():
        version_id = version.identifier

        dandiset_version = client.get_dandiset(dandiset_id=dandiset_id, version_id=version_id)

        all_reduced_logs = []
        for asset in dandiset_version.get_assets():
            asset_id = asset.identifier
            asset_suffixes = pathlib.Path(asset.path).suffixes

            blob_or_zarr = "blobs" if ".zarr" not in asset_suffixes else "zarr"

            reduced_log_file_path = reduced_s3_log_folder_path / f"{blob_or_zarr}_{asset_id}.tsv"

            if not reduced_log_file_path.exists():
                continue  # No reduced logs found (possible asset was never accessed); skip to next asset

            reduced_log = pandas.read_table(filepath_or_buffer=reduced_log_file_path, header=0)
            reduced_log["asset_id"] = [asset_id] * len(reduced_log)
            reduced_log["region"] = [
                get_region_from_ip_address(ip_address=ip_address, ip_hash_to_region=ip_hash_to_region)
                for ip_address in reduced_log["ip_address"]
            ]

            reordered_reduced_log = reduced_log.reindex(columns=("asset_id", "timestamp", "bytes_sent", "region"))
            all_reduced_logs.append(reordered_reduced_log)

        if len(all_reduced_logs) == 0:
            continue  # No reduced logs found (possible dandiset version was never accessed); skip to next version

        mapped_log = pandas.concat(objs=all_reduced_logs, ignore_index=True)
        mapped_log.sort_values(by="timestamp")
        mapped_log.index = range(len(mapped_log))

        dandiset_log_folder_path.mkdir(exist_ok=True)
        version_file_path = dandiset_log_folder_path / f"{version_id}.tsv"
        mapped_log.to_csv(version_file_path, mode="w", sep="\t", header=True, index=True)
