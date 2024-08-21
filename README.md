<p align="center">
  <h1 align="center">DANDI S3 Log Parser</h3>
  <p align="center">
    <a href="https://pypi.org/project/dandi_s3_log_parser/"><img alt="Ubuntu" src="https://img.shields.io/badge/Ubuntu-E95420?style=flat&logo=ubuntu&logoColor=white"></a>
    <a href="https://pypi.org/project/dandi_s3_log_parser/"><img alt="Supported Python versions" src="https://img.shields.io/pypi/pyversions/dandi_s3_log_parser.svg"></a>
    <a href="https://codecov.io/github/CatalystNeuro/dandi_s3_log_parser?branch=main"><img alt="codecov" src="https://codecov.io/github/CatalystNeuro/dandi_s3_log_parser/coverage.svg?branch=main"></a>
  </p>
  <p align="center">
    <a href="https://pypi.org/project/dandi_s3_log_parser/"><img alt="PyPI latest release version" src="https://badge.fury.io/py/dandi_s3_log_parser.svg?id=py&kill_cache=1"></a>
    <a href="https://github.com/catalystneuro/dandi_s3_log_parser/blob/main/license.txt"><img alt="License: BSD-3" src="https://img.shields.io/pypi/l/dandi_s3_log_parser.svg"></a>
  </p>
  <p align="center">
    <a href="https://github.com/psf/black"><img alt="Python code style: Black" src="https://img.shields.io/badge/python_code_style-black-000000.svg"></a>
    <a href="https://github.com/astral-sh/ruff"><img alt="Python code style: Ruff" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json"></a>
  </p>
</p>

Extraction of minimal information from consolidated raw S3 logs for public sharing and plotting.

Developed for the [DANDI Archive](https://dandiarchive.org/).

A few summary facts as of 2024:

- A single line of a raw S3 log file can be between 400-1000+ bytes.
- Some of the busiest daily logs on the archive can have around 5,014,386 lines.
- There are more than 6 TB of log files collected in total.
- This parser reduces that total to around 100 MB of essential information.



## Installation

```bash
pip install dandi_s3_log_parser
```



## Workflow

The process is comprised of three modular steps.

### 1. **Reduction**

Filter out:

- Non-success status codes.
- Excluded IP addresses.
- Operation types other than the one specified (`REST.GET.OBJECT` by default).

Then, only limit data extraction to a handful of specified fields from each full line of the raw logs; by default, `object_key`, `timestamp`, `ip_address`, and `bytes_sent`.

In summer of 2024, this reduced 6 TB of raw logs to around 200 GB.

The process is designed to be easily parallelized and interruptible, meaning that you can feel free to kill any processes while they are running and restart later without losing most progress.

### 2. **Binning**

To make the mapping to Dandisets more efficient, the reduced logs are binned by their object keys (asset blob IDs) for fast lookup.

This step reduces the total file sizes from step (1) even further by reducing repeated object keys, though it does create a large number of small files.

In summer of 2024, this reduced 200 GB of reduced logs to around 20 GB.

### 3. **Mapping**

The final step, which should be run periodically to keep the desired usage logs per Dandiset up to date, is to scan through all currently known Dandisets and their versions, mapping the asset blob IDs to their filenames and generating the most recently parsed usage logs that can be shared publicly.

In summer of 2024, this reduced 20 GB of binned logs to around 100 MB of Dandiset logs.



## Usage

### Reduction

To reduce:

```bash
reduce_all_dandi_raw_s3_logs \
  --raw_s3_logs_folder_path < base raw S3 logs folder > \
  --reduced_s3_logs_folder_path < reduced S3 logs folder path > \
  --maximum_number_of_workers < number of workers to use > \
  --maximum_buffer_size_in_mb < approximate amount of RAM to use > \
  --excluded_ips < comma-separated list of known IPs to exclude >
```

For example, on Drogon:

```bash
reduce_all_dandi_raw_s3_logs \
  --raw_s3_logs_folder_path /mnt/backup/dandi/dandiarchive-logs \
  --reduced_s3_logs_folder_path /mnt/backup/dandi/reduced-dandiarchive-logs \
  --maximum_number_of_workers 3 \
  --maximum_buffer_size_in_mb 3000 \
  --excluded_ips < Drogons IP >
```

### Binning

To bin:

```bash
bin_all_reduced_s3_logs_by_object_key \
  --reduced_s3_logs_folder_path < reduced S3 logs folder path > \
  --binned_s3_logs_folder_path < binned S3 logs folder path >
```

For example, on Drogon:

```bash
bin_all_reduced_s3_logs_by_object_key \
  --reduced_s3_logs_folder_path /mnt/backup/dandi/reduced-dandiarchive-logs \
  --binned_s3_logs_folder_path /mnt/backup/dandi/binned-dandiarchive-logs
```

### Mapping

The next step, that should also be updated regularly (daily-weekly), is to iterate through all current versions of all Dandisets, mapping the reduced logs to their assets.

```bash
map_binned_logs_to_dandisets \
  --binned_s3_logs_folder_path < binned S3 logs folder path > \
  --dandiset_logs_folder_path < mapped Dandiset logs folder >
```

For example, on Drogon:

```bash
map_binned_logs_to_dandisets \
  --binned_s3_logs_folder_path /mnt/backup/dandi/binned-dandiarchive-logs \
  --dandiset_logs_folder_path /mnt/backup/dandi/mapped-dandiset-logs
```


## Submit line decoding errors

Please email line decoding errors collected from your local config file (located in `~/.dandi_s3_log_parser/errors`) to the core maintainer before raising issues or submitting PRs contributing them as examples, to more easily correct any aspects that might require anonymization.
