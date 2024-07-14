import os
import pathlib
import hashlib

REQUEST_TYPES = ("GET", "PUT", "HEAD")

DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH = pathlib.Path.home() / ".dandi_s3_log_parser"
DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH.mkdir(exist_ok=True)

_IP_ADDRESS_TO_REGION_FILE_PATH = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "ip_address_to_region.yaml"

if "IPINFO_CREDENTIALS" not in os.environ:
    raise ValueError("The environment variable 'IPINFO_CREDENTIALS' must be set to import `dandi_s3_log_parser`!")
IPINFO_CREDENTIALS = os.environ["IPINFO_CREDENTIALS"]

if "IPINFO_HASH_SALT" not in os.environ:
    raise ValueError(
        "The environment variable 'IPINFO_HASH_SALT' must be set to import `dandi_s3_log_parser`! "
        "To retrieve the value, set a temporary value to this environment variable and then use the `get_hash_salt` "
        "helper function and set it to the correct value."
    )
IPINFO_HASH_SALT = bytes(os.environ["IPINFO_HASH_SALT"], "utf-8")


def get_hash_salt(base_raw_s3_log_folder_path: str | pathlib.Path) -> str:
    """
    Calculate the salt (in utf-8 encoded bytes) used for IP hashing.

    Uses actual data from the first line of the first log file in the raw S3 log folder, which only we have access to.

    Otherwise, it would be fairly easy to iterate over every possible IP address and find the SHA1 of it.
    """
    base_raw_s3_log_folder_path = pathlib.Path(base_raw_s3_log_folder_path)

    # Retrieve the first line of the first log file (which only we know) and use that as a secure salt
    first_log_file_path = base_raw_s3_log_folder_path / "2019" / "10" / "01.log"

    with open(file=first_log_file_path, mode="r") as io:
        first_line = io.readline()

    hash_salt = hashlib.sha1(string=bytes(first_line, "utf-8"))

    return hash_salt.digest().decode("utf-8")
