import pathlib

REQUEST_TYPES = ("GET", "PUT", "HEAD")

REQUEST_TYPES = ("GET", "PUT", "HEAD")

DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH = pathlib.Path.home() / ".dandi_s3_log_parser"
DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH.mkdir(exist_ok=True)

_IP_HASH_TO_REGION_FILE_PATH = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "ip_hash_to_region.yaml"
