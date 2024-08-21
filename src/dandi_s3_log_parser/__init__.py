"""
DANDI S3 log parser
===================

Extraction of minimal information from consolidated raw S3 logs for public sharing and plotting.

Developed for the DANDI Archive.

A few summary facts as of 2024:

- A single line of a raw S3 log file can be between 400-1000+ bytes.
- Some of the busiest daily logs on the archive can have around 5,014,386 lines.
- There are more than 6 TB of log files collected in total.
- This parser reduces that total to around 20 GB of essential information.

The reduced information is then additionally mapped to currently available assets in persistent published Dandiset
versions and current drafts, which only comprise around 100 MB of the original data.
"""

from ._config import DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH
from ._s3_log_file_reducer import reduce_raw_s3_log
from ._buffered_text_reader import BufferedTextReader
from ._dandi_s3_log_file_reducer import reduce_all_dandi_raw_s3_logs
from ._ip_utils import get_region_from_ip_address
from ._dandiset_mapper import map_reduced_logs_to_dandisets
from ._bin_all_reduced_s3_logs_by_object_key import bin_all_reduced_s3_logs_by_object_key

__all__ = [
    "DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH",
    "reduce_raw_s3_log",
    "BufferedTextReader",
    "reduce_all_dandi_raw_s3_logs",
    "get_region_from_ip_address",
    "map_reduced_logs_to_dandisets",
    "bin_all_reduced_s3_logs_by_object_key",
]
