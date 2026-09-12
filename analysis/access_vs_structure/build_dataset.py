"""
Build a joined table of NWB structural metrics, asset size, and access counts.

For every valid NWB file in the DANDI ``dandi-cache`` structural caches, this
collects four things and joins them into a single CSV:

  * structural metrics  — number of HDF5 groups, number of HDF5 datasets, the total
    cophenetic index (tree balance, defined for arbitrary-degree trees), and
    out-degree statistics, keyed by content ID;
  * asset size (bytes)  — read from an S3 ``HEAD`` on the blob;
  * access counts       — ``number_of_requests`` / ``number_of_downloads`` from
    the published ``dandi/access-summaries`` per-dandiset ``by_asset.tsv``.

The join key chain is:
    content_id  --(content-id-to-nwb-file cache)-->  (dandiset_id, asset_path)
                --(access-summaries by_asset.tsv)-->  request / download counts
    content_id  --(S3 HEAD on blobs/<...>/<content_id>)-->  size in bytes

Output columns (one row per content ID that resolves through the whole chain):
    content_id, dandiset_id, asset_path, groups, datasets, cophenetic_index,
    mean_out_degree, max_out_degree, variance_out_degree, n_internal_nodes,
    size_bytes, number_of_requests, number_of_downloads, requests_censored

``number_of_requests`` / ``number_of_downloads`` are privacy-rounded in the
source; values reported as ``"<N"`` are decoded to ``N/2`` and flagged in the
``requests_censored`` column so downstream code can treat them as censored-low.

Requirements
------------
    pip install requests tqdm

Usage
-----
    python build_dataset.py --out access_structure.csv [--workers 20]

Everything is fetched over HTTPS from public GitHub raw endpoints and the public
DANDI S3 bucket; no DANDI API access is required (it is often firewalled).
"""

import argparse
import concurrent.futures
import csv
import gzip
import io
import json
import pathlib

import requests
import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _make_session() -> requests.Session:
    """A requests Session that retries transient network/5xx errors with backoff."""
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=64)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# Shared session (thread-safe for GET/HEAD) used by all fetch helpers.
SESSION = _make_session()

RAW = "https://raw.githubusercontent.com"
CACHE_ORG = "dandi-cache"
SUMMARIES = f"{RAW}/dandi/access-summaries/main/content/summaries"
S3_BUCKET = "https://dandiarchive.s3.amazonaws.com"

# Scalar structural caches published as gzipped JSON Lines on the `dist` branch.
STRUCTURAL = {
    "groups": ("valid-nwb-file-to-number-of-groups", "valid_nwb_file_to_number_of_groups"),
    "datasets": ("valid-nwb-file-to-number-of-datasets", "valid_nwb_file_to_number_of_datasets"),
}
# Newer caches published as plain JSON Lines on the `derivatives` branch under
# derivatives/derivatives/. `cophenetic_index` is a scalar; `out_degrees` is a dict
# {n_internal_nodes, mean_out_degree, max_out_degree, variance_out_degree, median_out_degree}.
COPHENETIC_REPO = ("valid-nwb-file-to-cophenetic-index", "valid_nwb_file_to_cophenetic_index")
OUT_DEGREES_REPO = ("valid-nwb-file-to-out-degrees", "valid_nwb_file_to_out_degrees")
MAP_REPO = ("content-id-to-nwb-file", "content_id_to_nwb_file")


def _fetch_jsonl_gz(url: str) -> list[dict]:
    """Download a gzipped JSON Lines file and return the parsed objects."""
    response = SESSION.get(url, timeout=120)
    response.raise_for_status()
    text = gzip.decompress(response.content).decode("utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _fetch_jsonl_plain(url: str) -> list[dict]:
    """Download a plain (uncompressed) JSON Lines file and return the parsed objects."""
    response = SESSION.get(url, timeout=120)
    response.raise_for_status()
    return [json.loads(line) for line in response.text.splitlines() if line.strip()]


def load_derivatives_metric(repo: str, stem: str) -> dict[str, object]:
    """Return ``{content_id: value}`` for a cache on the ``derivatives`` branch."""
    url = f"{RAW}/{CACHE_ORG}/{repo}/derivatives/derivatives/{stem}.jsonl"
    out: dict[str, object] = {}
    for obj in _fetch_jsonl_plain(url):
        out.update(obj)
    return out


def load_structural_metric(repo: str, stem: str) -> dict[str, float]:
    """Return ``{content_id: value}`` for one ``dandi-cache`` structural cache."""
    url = f"{RAW}/{CACHE_ORG}/{repo}/dist/derivatives/{stem}.jsonl.gz"
    out: dict[str, float] = {}
    for obj in _fetch_jsonl_gz(url):
        out.update(obj)
    return out


def load_content_id_map(repo: str, stem: str) -> dict[str, tuple[str, str]]:
    """Return ``{content_id: (dandiset_id, asset_path)}`` from ``content-id-to-nwb-file``."""
    url = f"{RAW}/{CACHE_ORG}/{repo}/dist/derivatives/{stem}.jsonl.gz"
    out: dict[str, tuple[str, str]] = {}
    for obj in _fetch_jsonl_gz(url):
        for content_id, dandiset_path in obj.items():
            ((dandiset_id, path),) = dandiset_path.items()
            out[content_id] = (dandiset_id, path)
    return out


def _parse_count(raw: str) -> tuple[float, bool]:
    """Decode a privacy-rounded count. ``"<50"`` -> ``(25.0, True)``; else ``(value, False)``."""
    raw = raw.strip()
    if raw.startswith("<"):
        return float(raw[1:]) / 2.0, True
    return float(raw), False


def load_access_counts(dandiset_ids: set[str], workers: int) -> dict[tuple[str, str], tuple[float, float, bool]]:
    """
    Fetch ``by_asset.tsv`` for each dandiset and return
    ``{(dandiset_id, asset_path): (requests, downloads, censored)}``.

    Dandisets without a published summary (HTTP 404) are skipped.
    """
    counts: dict[tuple[str, str], tuple[float, float, bool]] = {}

    def _one(dandiset_id: str) -> list[tuple[tuple[str, str], tuple[float, float, bool]]]:
        url = f"{SUMMARIES}/{dandiset_id}/by_asset.tsv"
        response = SESSION.get(url, timeout=60)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        rows = []
        reader = csv.DictReader(io.StringIO(response.text), delimiter="\t")
        for row in reader:
            requests_value, r_censored = _parse_count(row["number_of_requests"])
            downloads_value, d_censored = _parse_count(row["number_of_downloads"])
            rows.append(((dandiset_id, row["asset_path"]), (requests_value, downloads_value, r_censored or d_censored)))
        return rows

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_one, d) for d in sorted(dandiset_ids)]
        for future in tqdm.tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="access TSVs"):
            for key, value in future.result():
                counts[key] = value
    return counts


def load_sizes(content_ids: list[str], workers: int) -> dict[str, int]:
    """
    Return ``{content_id: size_bytes}`` via an S3 ``HEAD`` on each blob.

    Blob key layout is ``blobs/<first 3 chars>/<next 3 chars>/<content_id>``.
    """
    sizes: dict[str, int] = {}

    def _one(content_id: str) -> tuple[str, int | None]:
        key = f"blobs/{content_id[:3]}/{content_id[3:6]}/{content_id}"
        response = SESSION.head(f"{S3_BUCKET}/{key}", allow_redirects=True, timeout=60)
        length = response.headers.get("Content-Length")
        return content_id, int(length) if length is not None else None

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_one, c) for c in content_ids]
        for future in tqdm.tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="S3 HEAD sizes"):
            content_id, size = future.result()
            if size is not None:
                sizes[content_id] = size
    return sizes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="access_structure.csv", type=pathlib.Path, help="Output CSV path")
    parser.add_argument("--workers", type=int, default=20, help="Parallel workers for TSV/S3 fetches (default: 20)")
    args = parser.parse_args()

    print("Loading structural metrics...")
    groups = load_structural_metric(*STRUCTURAL["groups"])
    datasets = load_structural_metric(*STRUCTURAL["datasets"])
    cophenetic = load_derivatives_metric(*COPHENETIC_REPO)
    out_degrees = load_derivatives_metric(*OUT_DEGREES_REPO)
    content_ids = sorted(set(groups) & set(datasets) & set(cophenetic) & set(out_degrees))
    print(f"  {len(content_ids):,} content IDs with all structural metrics")

    print("Loading content-id -> (dandiset, asset_path) map...")
    id_map = load_content_id_map(*MAP_REPO)

    resolved = [c for c in content_ids if c in id_map]
    dandiset_ids = {id_map[c][0] for c in resolved}
    print(f"  {len(resolved):,} resolve to an asset across {len(dandiset_ids):,} dandisets")

    counts = load_access_counts(dandiset_ids, workers=args.workers)
    sizes = load_sizes(resolved, workers=args.workers)

    n_written = 0
    with args.out.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "content_id",
                "dandiset_id",
                "asset_path",
                "groups",
                "datasets",
                "cophenetic_index",
                "mean_out_degree",
                "max_out_degree",
                "variance_out_degree",
                "n_internal_nodes",
                "size_bytes",
                "number_of_requests",
                "number_of_downloads",
                "requests_censored",
            ]
        )
        for content_id in resolved:
            dandiset_id, asset_path = id_map[content_id]
            access = counts.get((dandiset_id, asset_path))
            size = sizes.get(content_id)
            if access is None or size is None:
                continue
            requests_value, downloads_value, censored = access
            out_degree = out_degrees[content_id]
            writer.writerow(
                [
                    content_id,
                    dandiset_id,
                    asset_path,
                    int(groups[content_id]),
                    int(datasets[content_id]),
                    cophenetic[content_id],
                    out_degree["mean_out_degree"],
                    out_degree["max_out_degree"],
                    out_degree["variance_out_degree"],
                    out_degree["n_internal_nodes"],
                    size,
                    requests_value,
                    downloads_value,
                    int(censored),
                ]
            )
            n_written += 1

    print(f"\nWrote {n_written:,} fully-joined rows to {args.out}")


if __name__ == "__main__":
    main()
