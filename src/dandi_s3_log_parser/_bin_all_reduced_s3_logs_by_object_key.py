"""Bin reduced logs by object key."""

import pathlib

import pandas
from pydantic import DirectoryPath, validate_call


@validate_call
def bin_all_reduced_s3_logs_by_object_key(
    *,
    reduced_s3_logs_folder_path: DirectoryPath,
    binned_s3_logs_folder_path: DirectoryPath,
) -> None:
    """
    Bin reduced S3 logs by object keys.

    Parameters
    ----------
    reduced_s3_logs_folder_path : str
        The path to the folder containing the reduced S3 log files.
    binned_s3_logs_folder_path : str
        The path to write each binned S3 log file to.
    """
    # TODO: add two status tracking YAML files
    # 1) reduced_log_file_paths_started.yaml
    # 2) reduced_log_file_paths_completed.yaml
    # Throw warning on start of this function if they disagree (indicating error occurred, most likely during I/O stage)
    # All reduced logs will likely need to be freshly re-binned in this case
    # (by manually removing or renaming the 'binned' target directory to start off empty)
    #
    # But if all goes well, then use those file paths to skip over already completed binning
    #
    # Although; there was no guarantee that the binned contents were chronological, so maybe also
    # add a final step (with flag to disable) to re-write all binned logs in chronological order?
    #
    # Thought: since we're doing this non-parallel, we could just iterate the reduced logs in chronological order

    reduced_s3_log_files = reduced_s3_logs_folder_path.rglob("*.tsv")
    for reduced_s3_log_file in reduced_s3_log_files:
        reduced_data_frame = pandas.read_csv(filepath_or_buffer=reduced_s3_log_file, sep="\t")
        binned_data_frame = reduced_data_frame.groupby("object_key").agg(
            {
                "timestamp": list,
                "bytes_sent": list,
                "ip_address": list,
            }
        )
        del reduced_data_frame

        object_keys_to_data = {
            row.name: {"timestamp": row["timestamp"], "bytes_sent": row["bytes_sent"], "ip_address": row["ip_address"]}
            for _, row in binned_data_frame.iterrows()
        }
        del binned_data_frame

        for object_key, data in object_keys_to_data.items():
            object_key_as_path = pathlib.Path(object_key)
            binned_s3_log_file_path = (
                binned_s3_logs_folder_path / object_key_as_path.parent / f"{object_key_as_path.stem}.tsv"
            )
            binned_s3_log_file_path.parent.mkdir(exist_ok=True, parents=True)

            data_frame = pandas.DataFrame(data=data)

            header = False if binned_s3_log_file_path.exists() else True
            data_frame.to_csv(path_or_buf=binned_s3_log_file_path, mode="a", sep="\t", header=header, index=False)
