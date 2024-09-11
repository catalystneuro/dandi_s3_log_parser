"""Bin reduced logs by object key."""

import pathlib

import pandas
import tqdm
from pydantic import DirectoryPath, validate_call


@validate_call
def bin_all_reduced_s3_logs_by_object_key(
    *,
    reduced_s3_logs_folder_path: DirectoryPath,
    binned_s3_logs_folder_path: DirectoryPath,
    file_limit: int | None = None,
) -> None:
    """
    Bin reduced S3 logs by object keys.

    Parameters
    ----------
    reduced_s3_logs_folder_path : str
        The path to the folder containing the reduced S3 log files.
    binned_s3_logs_folder_path : str
        The path to write each binned S3 log file to.
        There will be one file per object key.
    file_limit : int, optional
        The maximum number of files to process per call.
    """
    started_tracking_file_path = binned_s3_logs_folder_path / "binned_log_file_paths_started.txt"
    completed_tracking_file_path = binned_s3_logs_folder_path / "binned_log_file_paths_completed.txt"

    if started_tracking_file_path.exists() != completed_tracking_file_path.exists():
        raise FileNotFoundError(
            "One of the tracking files is missing, indicating corruption in the binning process. "
            "Please clean the binning directory and re-run this function."
        )

    completed = None
    if not started_tracking_file_path.exists():
        started_tracking_file_path.touch()
        completed_tracking_file_path.touch()
    else:
        with open(file=started_tracking_file_path, mode="r") as io:
            started = set(pathlib.Path(path.rstrip("\n")) for path in io.readlines())
        with open(file=completed_tracking_file_path, mode="r") as io:
            completed = set(pathlib.Path(path.rstrip("\n")) for path in io.readlines())

        if started != completed:
            raise ValueError(
                "The tracking files do not agree on the state of the binning process. "
                "Please clean the binning directory and re-run this function."
            )
    completed = completed or set()

    print(f"{completed=}")
    reduced_s3_log_files = list(set(reduced_s3_logs_folder_path.rglob("*.tsv")) - completed)[:file_limit]
    print(f"{reduced_s3_log_files=}")
    for reduced_s3_log_file in tqdm.tqdm(
        iterable=reduced_s3_log_files,
        total=len(reduced_s3_log_files),
        desc="Binning reduced logs...",
        position=0,
        leave=True,
        mininterval=3.0,
        smoothing=0,
    ):
        if reduced_s3_log_file.stat().st_size == 0:
            with open(file=started_tracking_file_path, mode="a") as io:
                io.write(f"{reduced_s3_log_file}\n")
            with open(file=completed_tracking_file_path, mode="a") as io:
                io.write(f"{reduced_s3_log_file}\n")

            continue

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

        with open(file=started_tracking_file_path, mode="a") as io:
            io.write(f"{reduced_s3_log_file}\n")

        for object_key, data in tqdm.tqdm(
            iterable=object_keys_to_data.items(),
            total=len(object_keys_to_data),
            desc=f"Binning {reduced_s3_log_file}...",
            position=1,
            leave=False,
            mininterval=3.0,
            smoothing=0,
        ):
            object_key_as_path = pathlib.Path(object_key)
            binned_s3_log_file_path = (
                binned_s3_logs_folder_path / object_key_as_path.parent / f"{object_key_as_path.name}.tsv"
            )
            binned_s3_log_file_path.parent.mkdir(exist_ok=True, parents=True)

            data_frame = pandas.DataFrame(data=data)

            header = False if binned_s3_log_file_path.exists() else True
            data_frame.to_csv(path_or_buf=binned_s3_log_file_path, mode="a", sep="\t", header=header, index=False)

        with open(file=completed_tracking_file_path, mode="a") as io:
            io.write(f"{reduced_s3_log_file}\n")
