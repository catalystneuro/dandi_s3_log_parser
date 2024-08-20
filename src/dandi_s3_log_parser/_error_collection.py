import datetime
import importlib.metadata

from ._config import DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH


def _collect_error(message: str, error_type: str, task_id: str | None = None) -> None:
    """
    Helper function to collect errors in a text file for sharing and reviewing.

    Parameters
    ----------
    message : str
        The error message to be collected.
        This message is automatically padded in the file with some empty lines for readability.
    error_type : str
        The type of error message being collected.
        Added as an identifying tag on the error collection file name.
        Examples include "line", "parallel", and "ipinfo".
    task_id : str or None, optional
        A unique identifier for the task that generated the error.
        Added as an identifying tag on the error collection file name.
    """
    errors_folder_path = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
    errors_folder_path.mkdir(exist_ok=True)

    dandi_s3_log_parser_version = importlib.metadata.version(distribution_name="dandi_s3_log_parser")
    date = datetime.datetime.now().strftime("%y%m%d")

    error_collection_file_name = f"v{dandi_s3_log_parser_version}_{date}_{error_type}_errors"
    if task_id is not None:
        error_collection_file_name += f"_{task_id}"
    error_collection_file_name += ".txt"
    error_collection_file_path = errors_folder_path / error_collection_file_name

    padded_message = f"{message}\n\n"
    with open(file=error_collection_file_path, mode="a") as io:
        io.write(padded_message)

    return None
