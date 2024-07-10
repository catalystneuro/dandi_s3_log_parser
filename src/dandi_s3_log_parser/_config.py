import os
import pathlib

DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH = pathlib.Path.home() / ".dandi_s3_log_parser"
DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH.mkdir(exist_ok=True)

_IP_ADDRESS_TO_REGION_FILE_PATH = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "ip_address_to_region.yaml"

if "IPINFO_CREDENTIALS" not in os.environ:
    raise ValueError("The environment variable 'IPINFO_CREDENTIALS' must be set to import `dandi_s3_log_parser`!")
IPINFO_CREDENTIALS = os.environ["IPINFO_CREDENTIALS"]
