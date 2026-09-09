<div align="center">
  <picture>
    <img alt="s3-log-extraction logo" src="https://raw.githubusercontent.com/dandi/s3-log-extraction/main/docs/s3-log-extraction-logo.svg" width="200">
  </picture>

<h1>S3 Log Extraction</h1>
<p>
    <a href="https://pypi.org/project/s3-log-extraction/"><img alt="Supported Python versions" src="https://img.shields.io/pypi/pyversions/s3-log-extraction.svg"></a>
    <a href="https://pypi.org/project/s3-log-extraction/"><img alt="Ubuntu" src="https://img.shields.io/badge/Ubuntu-E95420?style=flat&logo=ubuntu&logoColor=white"></a>
    <a href="https://codecov.io/github/dandi/s3-log-extraction?branch=main"><img alt="codecov" src="https://codecov.io/github/dandi/s3-log-extraction/coverage.svg?branch=main"></a>
    <a href="https://github.com/dandi/s3-log-extraction/actions/workflows/daily_tests.yml/badge.svg"><img alt="Daily tests" src="https://github.com/dandi/s3-log-extraction/actions/workflows/daily_tests.yml/badge.svg"></a>
    <a href="https://github.com/dandi/s3-log-extraction/actions/workflows/daily_remote_tests.yml/badge.svg"><img alt="Daily tests (remote)" src="https://github.com/dandi/s3-log-extraction/actions/workflows/daily_remote_tests.yml/badge.svg"></a>
</p>
<p>
    <a href="https://pypi.org/project/s3-log-extraction/"><img alt="PyPI latest release version" src="https://badge.fury.io/py/s3-log-extraction.svg?id=py&kill_cache=1"></a>
    <a href="https://github.com/dandi/s3-log-extraction/blob/main/LICENSE.txt"><img alt="License: MIT" src="https://img.shields.io/pypi/l/s3-log-extraction.svg"></a>
    <a href="https://zenodo.org/badge/latestdoi/826995164"><img alt="DOI" src="https://img.shields.io/github/v/release/dandi/s3-log-extraction?label=DOI&color=blue"></a>
</p>
<p>
    <a href="https://github.com/psf/black"><img alt="Python code style: Black" src="https://img.shields.io/badge/python_code_style-black-000000.svg"></a>
    <a href="https://github.com/astral-sh/ruff"><img alt="Python code style: Ruff" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json"></a>
</p>
</div>

Fast extraction of access summary data from S3 logs.

Originally developed for the [DANDI Archive](https://dandiarchive.org/).

Read more about [S3 logging on AWS](https://web.archive.org/web/20240807191829/https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html).

⚠️ This package currently only supports processing of access data (`GET`-type requests); if you wish to use this package for other types of requests (`PUT`/`DELETE`/`HEAD`, etc.) please reach out by raising an issue. ⚠️



## Installation

```bash
pip install s3-log-extraction
```

**Note for Windows users:** This package requires GAWK and is not natively supported on Windows. Windows users should use [Windows Subsystem for Linux (WSL)](https://docs.microsoft.com/en-us/windows/wsl/install) to run this package.


# Workflow

```mermaid
flowchart TD
    A[Configure cache<br/><br/>Initialize home and cache directories]
    B[Extract logs<br/><br/>Process raw S3 logs and store minimal extracted data]
    C[Update IP indexes<br/><br/>Generate anonymized indexes for each IP address]
    D[Update region codes<br/><br/>Map IPs to ISO 3166 region codes using the local GeoLite2 database]
    E[Update coordinates<br/><br/>Look up latitude/longitude for each region code in the bundled ISO 3166 tables]
    F[Generate summaries<br/><br/>Create per-dataset summaries for reporting]
    G[Generate totals<br/><br/>Aggregate statistics across datasets or archive]
    H[Share!<br/><br/>Post the summaries and totals in a public data repository]

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
```



## Generic Usage

[Optional] Configure a non-default cache directory on a mounted disk that has sufficient space (the default is placed under `~/.cache`). This will be the main location where extracted logs and other useful information will be stored.

```bash
s3logextraction config cache set <new cache directory>
```

To extract the logs:

```bash
s3logextraction extract <log directory>
```

To override the cache directory for a single extraction run (without changing global config):

```bash
s3logextraction extract <log directory> --cache <cache directory>
```

**NOTE**: If you feel like this command is taking a long time on your system, DO NOT interrupt it via `ctrl+C` or `pkill`. Instead, you can safely interrupt it by running:

```bash
s3logextraction stop
```

This will allow it to finish processing the current batch of logs and then exit gracefully.

After your logs are extracted, generate anonymized indexes for each IP address:

```bash
s3logextraction update ip indexes
````

Next, ensure the environment variables for the geolocation database are set:

- **MAXMIND_ACCOUNT_ID** and **MAXMIND_LICENSE_KEY**
  - Credentials of a free [MaxMind](https://www.maxmind.com/en/geolite2/signup) account, used to download the [GeoLite2-City](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data/) database.
  - The database is queried locally to map each IP address to its ISO 3166-1 alpha-3 country code and ISO 3166-2 subdivision code (e.g. "USA/CA" for California) for anonymized statistics. IP addresses in the published ranges of GitHub, AWS, GCP, and known VPN or datacenter providers are labeled by that service instead.
  - The database is downloaded into the cache directory on first use and refreshed automatically once it is more than a week old, so the credentials only need to be set on machines that update the region codes.

```bash
export MAXMIND_ACCOUNT_ID="your_account_id_here"
export MAXMIND_LICENSE_KEY="your_license_key_here"
```

To update the region codes and their coordinates:

```bash
s3logextraction update ip regions
s3logextraction update ip coordinates
```

The coordinates step needs no credentials: each region code is looked up in ISO 3166 tables bundled with the package, whose coordinates come from the public-domain [Natural Earth](https://www.naturalearthdata.com) dataset. Cloud service regions such as `AWS/us-east-1` are located with the GeoLite2 database.

To force a fresh download of the GeoLite2 database (this happens automatically when it is stale):

```bash
s3logextraction update ip database --force
```

This product includes GeoLite2 Data created by MaxMind, available from https://www.maxmind.com.

To generate top-level summaries and totals (that is, per dataset):

```bash
s3logextraction update summaries
s3logextraction update totals
```

Finally, to generate archive-wide summaries and totals:

```bash
s3logextraction update summaries --mode archive
s3logextraction update totals --mode archive
```



## Remote S3 Bucket Extraction

To extract logs from a remote S3 bucket, use the `--mode remote` flag.  For large buckets, we strongly recommend
setting up [AWS S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html) and
downloading the inventory locally before running the extraction.  Scanning the bucket directly via live network calls
(the default when no `--inventory` path is given) can be extremely slow for buckets with millions of objects.

### Using S3 Inventory (recommended)

AWS S3 Inventory generates periodic snapshots of all objects in your bucket as gzip-compressed CSV files.  Once
downloaded locally, the inventory lets `s3logextraction` enumerate all log files without making any live S3 listing
calls, providing a significant performance improvement over direct bucket scanning.

**Expected inventory directory layout:**

```
<inventory_directory>/
├── <timestamp>/               # e.g. 2026-05-03T01-00Z/
│   ├── manifest.json
│   └── manifest.checksum
├── data/
│   └── <uuid>.csv.gz          # gzip-compressed CSV inventory files
└── hive/
    └── dt=<YYYY-MM-DD-HH-MM>/ # e.g. dt=2026-05-03-01-00/
        └── symlink.txt        # references to data/*.csv.gz
```

Pass the path to the downloaded inventory directory via the `--inventory` option:

```bash
s3logextraction extract s3://my-logs-bucket --mode remote --inventory /path/to/inventory
```

To check how many log files are in the inventory and the total size:

```bash
s3logextraction stats --inventory /path/to/inventory
```

To report what percentage of log files have already been processed:

```bash
s3logextraction completion --inventory /path/to/inventory
```

### Without an S3 Inventory (not recommended)

If you do not have an S3 Inventory available, do **not** use `--mode remote` without `--inventory` — the live bucket
scan will be extremely slow for large buckets.  Instead, use [s5cmd](https://github.com/peak/s5cmd) to manually
download the unprocessed log files to a local directory and then run the local extraction on those files:

```bash
s5cmd cp "s3://my-logs-bucket/*" /path/to/local/logs/
s3logextraction extract /path/to/local/logs/
```





If you're new to using AWS S3 buckets and haven't yet enabled the logging this project utilizes, you can follow these simple instructions to get started.

1) Log into your AWS console.
2) Create a new **PRIVATE** S3 bucket - typically the name of the new bucket is the name of the one you wish to enable logging on with `-logs` added to the end. For example, `dandiarchive-logs`.
   - **NEVER** share this bucket publicly as it contains sensitive information.
3) Navigate back to the S3 bucket you wish to enable logging on.
4) Under the `Properties` tab, scroll down to the section called `Server access logging` and select `Edit`.
5) Toggle the selection to `Enable`, then specify the destination where logs will be stored as the new S3 bucket you created in step (2).
6) Recommended:
   - Specify the `Log object key format` as the nested pattern shown below.
   - Ensure the `Source of date used in log object key format` is the `S3 event time`.
    ![image](https://github.com/user-attachments/assets/8e8de5fc-5a58-4dda-8866-8bf71277e2e7)



## Developer Notes

Throughout the codebase, various processes are referred to in the following ways:

- parallelized: The process can be run in parallel across multiple workers, which increases throughput.
- interruptible: The process can be safely interrupted (`ctrl+C` or `pkill`) with only a very low chance of causing corruption. For parallelized interruption you may have to either `pkill` the main dispatch process or spam `ctrl+C` multiple times.
- updatable: The process can be resumed from the last checkpoint without losing any progress. It can also be run fresh at different times, such as on a CRON cycle, and it will only interact with unprocessed data.

### Performance

By leveraging `GAWK`, this version of the S3 log handling is considerably more efficient than the previous attempts.

The previous attempt used a multistep process which took several days to run (even on multiple workers). It also required an additional ~200 GB cache to allow lazy updates of the per-object bins.

This version requires no intermediate cache, stores only the minimal amount of data to be shared, and takes less than a day to do a fresh run (and is also lazy regarding daily CRON updates).

### Validation

In lieu of attempting fully validated parsing of each and every line from the log files (which is a hard, unsolved problem - see [s3-log-parser](https://github.com/dandi/s3-log-parser)), we instead validate the heuristics in a targeted manner through specific validation scripts.

These can also be used to verify the current state of the extraction process, such as warning about corrupt records or incomplete cache files.

#### Excluded IP regex configuration

The extraction heuristic pre-validator uses an excluded-IP regex.
By default, no IPs are excluded.

Example custom regex:

```bash
export S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX='^(192\.0\.2\.1|198\.51\.100\.2)$'
```

### Submission of line decoding errors

Should you discover any lines in your S3 log files that cause failures in the codebase, please email them to the core maintainer (cody.c.baker.phd@gmail.com) before raising issues or submitting PRs contributing them as examples, to more easily correct any aspects that might require anonymization.
