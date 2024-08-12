<p align="center">
  <h3 align="center">DANDI S3 Log Parser</h3>
  <p align="center">
    <a href="https://codecov.io/github/CatalystNeuro/dandi_s3_log_parser?branch=main"><img alt="codecov" src="https://codecov.io/github/CatalystNeuro/dandi_s3_log_parser/coverage.svg?branch=main"></a>
  </p>
  <p align="center">
    <a href="https://pypi.org/project/dandi_s3_log_parser/"><img alt="PyPI latest release version" src="https://badge.fury.io/py/dandi_s3_log_parser.svg"></a>
    <a href="https://pypi.org/project/dandi_s3_log_parser/"><img alt="Ubuntu" src="https://img.shields.io/badge/Ubuntu-E95420?style=flat&logo=ubuntu&logoColor=white"></a>
    <a href="https://pypi.org/project/dandi_s3_log_parser/"><img alt="Supported Python versions" src="https://img.shields.io/pypi/pyversions/dandi_s3_log_parser.svg"></a>
    <a href="https://github.com/catalystneuro/dandi_s3_log_parser/blob/main/license.txt"><img alt="License: BSD-3" src="https://img.shields.io/pypi/l/dandi_s3_log_parser.svg"></a>
  </p>
  <p align="center">
    <a href="https://github.com/psf/black"><img alt="Python code style: Black" src="https://img.shields.io/badge/python_code_style-black-000000.svg"></a>
    <a href="https://github.com/astral-sh/ruff"><img alt="Python code style: Ruff" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json"></a>
  </p>
</p>

Simple reductions of consolidated S3 logs (consolidation step not included in this repository) into minimal information for public sharing and plotting.

Developed for the [DANDI Archive](https://dandiarchive.org/).



## Usage

To iteratively parse all historical logs all at once (parallelization with 10-15 total GB recommended):

```bash
parse_all_dandi_raw_s3_logs \
  --base_raw_s3_log_folder_path < base log folder > \
  --parsed_s3_log_folder_path < output folder > \
  --excluded_ips < comma-separated list of known IPs to exclude > \
  --maximum_number_of_workers < number of CPUs to use > \
  --maximum_buffer_size_in_bytes < approximate amount of RAM to use >
```

For example, on Drogon:

```bash
parse_all_dandi_raw_s3_logs \
  --base_raw_s3_log_folder_path /mnt/backup/dandi/dandiarchive-logs \
  --parsed_s3_log_folder_path /mnt/backup/dandi/dandiarchive-logs-cody/parsed_7_13_2024/GET_per_asset_id \
  --excluded_ips < Drogons IP > \
  --maximum_number_of_workers 3 \
  --maximum_buffer_size_in_bytes 15000000000
```

To parse only a single log file at a time, such as in a CRON job:

```bash
parse_dandi_raw_s3_log \
  --raw_s3_log_file_path < s3 log file path > \
  --parsed_s3_log_folder_path < output folder > \
  --excluded_ips < comma-separated list of known IPs to exclude >
```



## Submit line decoding errors

Please email line decoding errors collected from your local config file to the core maintainer before raising issues or submitting PRs contributing them as examples, to more easily correct any aspects that might require anonymization.



## Developer notes

`.log` file suffixes should typically be ignored when working with Git, so when committing changes to the example log collection, you will have to forcibly include it with

```bash
git add -f <example file name>.log
```
