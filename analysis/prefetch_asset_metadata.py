"""
Pre-fetch structural metadata for NWB/Zarr assets from the DANDI Archive.

For each asset in a dandiset this script records:
  - size_bytes          : total asset size reported by the Dandi API
  - asset_type          : 'hdf5' | 'zarr' | 'other'
  - n_objects           : number of internal objects
                            HDF5  → groups + datasets visited by h5py.File.visititems()
                            Zarr  → number of .zarray / .zgroup metadata files on S3
  - min_path_depth      : minimum number of '/' in any internal object path
  - max_path_depth      : maximum number of '/' in any internal object path
  - avg_path_depth      : mean number of '/' across all internal object paths

Output is a CSV saved to --out (default: asset_metadata.csv).

Requirements
------------
    pip install dandi h5py remfile zarr boto3 tqdm pandas

Usage
-----
    python prefetch_asset_metadata.py --dandiset DANDI:000123 \\
        [--dandiset-version draft] [--out asset_metadata.csv] [--workers 8]

    # Or point at a local text file with one dandiset ID per line:
    python prefetch_asset_metadata.py --dandiset-list dandisets.txt
"""

import argparse
import concurrent.futures
import pathlib
import traceback
import warnings

import pandas as pd
import tqdm

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _path_depth(path: str) -> int:
    """Count '/' characters in an internal object path."""
    return path.count("/")


def _inspect_hdf5(https_url: str) -> dict:
    """Open an NWB/HDF5 file via remfile (HTTP range requests, no local cache) and walk its internals."""
    import h5py
    import remfile

    paths = []
    try:
        rf = remfile.File(https_url)
        with h5py.File(rf, "r") as hf:

            def _visitor(name, _obj):
                paths.append(name)

            hf.visititems(_visitor)
    except Exception as exc:
        warnings.warn(f"HDF5 inspect failed for {https_url}: {exc}")
        return {"n_objects": None, "min_path_depth": None, "max_path_depth": None, "avg_path_depth": None}

    if not paths:
        return {"n_objects": 0, "min_path_depth": 0, "max_path_depth": 0, "avg_path_depth": 0.0}

    depths = [_path_depth(p) for p in paths]
    return {
        "n_objects": len(paths),
        "min_path_depth": min(depths),
        "max_path_depth": max(depths),
        "avg_path_depth": sum(depths) / len(depths),
    }


def _inspect_zarr(s3_bucket: str, s3_prefix: str) -> dict:
    """
    List S3 objects under a Zarr store prefix.

    Counts .zarray / .zgroup metadata files to infer the array/group hierarchy,
    and uses chunk-file key suffixes to derive path depths.
    """
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator("list_objects_v2")

    metadata_paths = []  # .zarray / .zgroup paths (define the hierarchy)
    chunk_depths = []  # depth of chunk files (proxy for array dimensionality)

    try:
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                rel = key[len(s3_prefix) :].lstrip("/")
                if not rel:
                    continue
                if rel.endswith((".zarray", ".zgroup", ".zattrs", ".zmetadata")):
                    metadata_paths.append(rel)
                else:
                    # chunk file — depth is meaningful
                    chunk_depths.append(_path_depth(rel))
    except Exception as exc:
        warnings.warn(f"Zarr S3 listing failed for s3://{s3_bucket}/{s3_prefix}: {exc}")
        return {"n_objects": None, "min_path_depth": None, "max_path_depth": None, "avg_path_depth": None}

    all_paths = metadata_paths
    if not all_paths:
        return {"n_objects": 0, "min_path_depth": 0, "max_path_depth": 0, "avg_path_depth": 0.0}

    depths = [_path_depth(p) for p in all_paths]
    return {
        "n_objects": len(all_paths),
        "min_path_depth": min(depths),
        "max_path_depth": max(depths),
        "avg_path_depth": sum(depths) / len(depths),
    }


def _classify_asset(asset) -> str:
    path = asset.path.lower()
    if path.endswith((".nwb", ".h5", ".hdf5", ".he5")):
        return "hdf5"
    if ".zarr" in path or path.endswith(".zarr"):
        return "zarr"
    return "other"


def _process_asset(asset, dandiset_id: str, s3_bucket: str) -> dict | None:
    asset_type = _classify_asset(asset)
    row = {
        "dandiset_id": dandiset_id,
        "asset_path": asset.path,
        "asset_id": str(asset.identifier),
        "size_bytes": asset.size,
        "asset_type": asset_type,
        "n_objects": None,
        "min_path_depth": None,
        "max_path_depth": None,
        "avg_path_depth": None,
    }

    try:
        if asset_type == "hdf5":
            https_url = asset.download_url
            row.update(_inspect_hdf5(https_url))

        elif asset_type == "zarr":
            s3_key = f"{dandiset_id.replace('DANDI:', 'dandisets/')}/{asset.path}"
            # Strip trailing slash
            s3_prefix = s3_key.rstrip("/")
            row.update(_inspect_zarr(s3_bucket, s3_prefix))

    except Exception:
        traceback.print_exc()

    return row


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _collect_dandiset(dandiset_id: str, version: str, s3_bucket: str, workers: int) -> list[dict]:
    from dandi.dandiapi import DandiAPIClient

    with DandiAPIClient() as client:
        ds = client.get_dandiset(dandiset_id, version_id=version)
        assets = list(ds.get_assets())

    print(f"  {dandiset_id}: {len(assets)} assets")

    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process_asset, a, dandiset_id, s3_bucket): a for a in assets}
        for fut in tqdm.tqdm(
            concurrent.futures.as_completed(futures), total=len(futures), desc=dandiset_id, unit="asset"
        ):
            result = fut.result()
            if result is not None:
                rows.append(result)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dandiset", help="Single dandiset ID, e.g. DANDI:000123")
    group.add_argument("--dandiset-list", type=pathlib.Path, help="Text file with one dandiset ID per line")
    parser.add_argument("--dandiset-version", default="draft", help="Dandiset version string (default: draft)")
    parser.add_argument("--s3-bucket", default="dandiarchive", help="S3 bucket name (default: dandiarchive)")
    parser.add_argument(
        "--out", default="asset_metadata.csv", type=pathlib.Path, help="Output CSV path (default: asset_metadata.csv)"
    )
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers per dandiset (default: 8)")
    args = parser.parse_args()

    if args.dandiset:
        dandiset_ids = [args.dandiset]
    else:
        dandiset_ids = [
            line.strip()
            for line in args.dandiset_list.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]

    all_rows = []
    for did in tqdm.tqdm(dandiset_ids, desc="Dandisets", unit="dandiset"):
        rows = _collect_dandiset(did, version=args.dandiset_version, s3_bucket=args.s3_bucket, workers=args.workers)
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows)
    df.to_csv(args.out, index=False)
    print(f"\nSaved {len(df):,} asset records to {args.out}")

    # Quick summary
    print("\n--- Asset type breakdown ---")
    print(df["asset_type"].value_counts().to_string())
    print("\n--- Size summary (bytes) ---")
    print(df.groupby("asset_type")["size_bytes"].describe().to_string())


if __name__ == "__main__":
    main()
