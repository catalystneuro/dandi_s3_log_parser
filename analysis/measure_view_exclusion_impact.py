"""
Dry-run: measure the impact of excluding cloud/VPN/GitHub IPs from ``number_of_views``.

This does NOT change any production code. It reuses the library's *actual* view
sessionizer (``_collect_asset_views``), the *actual* IP cache (``load_ip_cache``),
and the *actual* exclusion predicate (``is_cloud_service_or_vpn_label``) to compute,
over the whole extraction cache (all datasets by default):

  * total_views   — views exactly as the shipped ``number_of_views`` counts them
                    (this should match the published totals — a consistency check);
  * kept_views    — views that would remain after excluding IPs whose ip_to_region
                    label is a cloud/VPN service (GitHub, AWS/*, GCP/*, VPN);
  * excluded_views and the % drop, broken down by service label and by dataset.

Because it calls the same functions the summaries call, ``kept_views`` is exactly
what ``number_of_views`` would become if the exclusion were wired into
``_collect_asset_views``. Run this BEFORE implementing the change to know the
magnitude, and AFTER to confirm the official output matches.

Efficient reruns (``--cache-parquet``)
--------------------------------------
The sessionization walk is the expensive part (hours to days). With
``--cache-parquet`` the per-(dataset, IP) view counts are written to
``<cache-dir>/analysis_cache/view_exclusion_pairs.parquet`` — co-located with the
original data (falling back to ``view_exclusion_pairs.csv.gz`` if no parquet engine is
installed). The report always prints BEFORE this write, so a missing optional dependency
can never discard a multi-hour walk. The raw IP is NOT stored: it is replaced by a salted keyed hash, and
the coarse ip_to_region *label* (e.g. ``GitHub`` or ``US/California``) is stored so
the exclusion can be re-applied instantly on later runs without another walk. The
stored label is frozen at build time; pass ``--rebuild-cache`` to refresh it after
the ip_to_region cache updates.

IMPORTANT: the exclusion uses the ip_to_region *cache* label, which is a different
(possibly staler, IPv4-only) source than a direct api.github.com/meta check. This
measures the real production predicate, not the meta-API proxy.

Usage
-----
    python measure_view_exclusion_impact.py --cache-dir /path/to/cache [--no-encryption] \\
        [--cache-parquet] [--dataset 000032]

``--dataset`` restricts to one dataset for a fast first pass; omit it for the full
run over every dataset. Encryption password via S3_LOG_EXTRACTION_PASSWORD.
"""

import argparse
import collections
import hashlib
import os
import pathlib
import sys

import pandas as pd
import tqdm


def _load_library():
    """Import the exact production functions so the measurement matches the summaries."""
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
    from s3_log_extraction.ip_utils import is_cloud_service_or_vpn_label, load_ip_cache
    from s3_log_extraction.summarize._generate_summaries import _collect_asset_views

    return _collect_asset_views, load_ip_cache, is_cloud_service_or_vpn_label


def _ip_hash_key() -> bytes:
    salt = (
        os.environ.get("S3_LOG_EXTRACTION_SALT") or os.environ.get("S3_LOG_EXTRACTION_PASSWORD") or "s3_log_extraction"
    )
    return salt.encode("utf-8")[:64]


def _service_of(label: str) -> str:
    """Coarse service name for the breakdown (GitHub / AWS / GCP / VPN / other)."""
    head = label.split("/", 1)[0] if label else ""
    return head if head in {"GitHub", "AWS", "GCP", "VPN"} else "other"


def _persist_pairs(pairs: pd.DataFrame, cache_dir: pathlib.Path) -> pathlib.Path | None:
    """
    Persist the (already privacy-safe: hashed IP + coarse label) pairs next to the data.

    Tries parquet first; if no parquet engine is installed, falls back to gzipped CSV so a
    multi-hour walk is never lost to a missing optional dependency. Returns the path written,
    or None if both attempts fail (the report has already printed by the time this runs).
    """
    base = cache_dir / "analysis_cache"
    base.mkdir(parents=True, exist_ok=True)
    parquet_path = base / "view_exclusion_pairs.parquet"
    try:
        pairs.to_parquet(parquet_path, index=False)
        return parquet_path
    except ImportError:
        csv_path = base / "view_exclusion_pairs.csv.gz"
        pairs.to_csv(csv_path, index=False, compression="gzip")
        print("  (pyarrow/fastparquet not installed — wrote gzipped CSV instead; " "same hashed-IP contents)")
        return csv_path


def _load_pairs(cache_dir: pathlib.Path) -> pd.DataFrame | None:
    """Load a previously persisted cache, preferring parquet then gzipped CSV. None if neither exists."""
    base = cache_dir / "analysis_cache"
    parquet_path = base / "view_exclusion_pairs.parquet"
    csv_path = base / "view_exclusion_pairs.csv.gz"
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path)
        except ImportError:
            pass
    if csv_path.exists():
        return pd.read_csv(csv_path, dtype={"dataset_id": str, "ip_hash": str, "region_label": str})
    return None


def build_view_pairs(
    cache_dir: pathlib.Path,
    use_encryption: bool,
    dataset_filter: str | None,
    collect_asset_views,
    ip_to_region: dict[str, str],
    max_assets: int | None = None,
) -> pd.DataFrame:
    """
    Walk the extraction cache, sessionize with the production code path, and return a
    per-(dataset, IP) view-count table: ``dataset_id``, ``ip_hash``, ``region_label``,
    ``n_views``. ``dataset_id`` is the top-level directory name under ``extraction/``,
    exactly as the production summaries define a dataset (``dataset.name``). The raw IP
    is hashed; the coarse region label is retained so the exclusion can be re-applied
    without another walk.
    """
    extraction_root = cache_dir / "extraction"
    if not extraction_root.exists():
        raise FileNotFoundError(f"No 'extraction' subdirectory under {cache_dir}")

    top_level = [d for d in sorted(extraction_root.iterdir()) if d.is_dir()]
    shown = [d.name for d in top_level][:10]
    more = " ..." if len(top_level) > 10 else ""
    print(f"Top-level directories under extraction/ (production 'datasets'): {shown}{more}")

    asset_dirs = []
    for dataset_dir in top_level:
        if dataset_filter and dataset_filter not in dataset_dir.name:
            continue
        for asset_dir in dataset_dir.rglob("*"):
            if (asset_dir / "timestamps.txt").exists():
                asset_dirs.append(asset_dir)
    if max_assets is not None:
        asset_dirs = asset_dirs[:max_assets]
    print(f"Found {len(asset_dirs)} asset directories{f' (limited to {max_assets})' if max_assets else ''}")

    counts: dict[tuple[str, str], int] = collections.defaultdict(int)
    skipped = 0
    for asset_dir in tqdm.tqdm(asset_dirs, desc="Sessionizing (production code path)"):
        dataset_id = asset_dir.relative_to(extraction_root).parts[0]
        try:
            views = collect_asset_views(asset_directory=asset_dir, use_encryption=use_encryption)
        except RuntimeError:
            # The production reader raises on a corrupt/misaligned asset; a dry-run over the
            # whole cache should not die on one, so record and continue.
            skipped += 1
            continue
        for _view_date, ip in views:
            counts[(dataset_id, ip)] += 1
    if skipped:
        print(f"  Skipped {skipped} asset(s) that raised the strict reader error")

    key = _ip_hash_key()
    rows = [
        {
            "dataset_id": dataset_id,
            "ip_hash": hashlib.blake2b(ip.encode("utf-8"), key=key, digest_size=16).hexdigest(),
            "region_label": ip_to_region.get(ip, ""),
            "n_views": n,
        }
        for (dataset_id, ip), n in counts.items()
    ]
    return pd.DataFrame(rows)


def report(pairs: pd.DataFrame, is_cloud_service_or_vpn_label) -> None:
    excluded_mask = pairs["region_label"].map(is_cloud_service_or_vpn_label)
    total_views = int(pairs["n_views"].sum())
    excluded_views = int(pairs.loc[excluded_mask, "n_views"].sum())
    kept_views = total_views - excluded_views
    pct = 100 * excluded_views / max(total_views, 1)

    print("\n--- View-exclusion impact (production sessionizer + production predicate) ---")
    print(f"  total_views (current number_of_views):      {total_views:,}")
    print(f"  kept_views  (after excluding cloud/VPN/GH):  {kept_views:,}")
    print(f"  excluded_views:                              {excluded_views:,} ({pct:.2f}%)")
    print(f"  distinct excluded IPs:                       {pairs.loc[excluded_mask, 'ip_hash'].nunique():,}")

    print("\n  excluded views by service label:")
    by_service = collections.defaultdict(int)
    for label, n in pairs.loc[excluded_mask, ["region_label", "n_views"]].itertuples(index=False):
        by_service[_service_of(label)] += int(n)
    for service, count in sorted(by_service.items(), key=lambda kv: -kv[1]):
        print(f"    {service:>7}: {count:,}")

    print("\n--- Sensitivity: kept_views under each exclusion tier ---")
    services = pairs["region_label"].map(_service_of)
    tiers = [
        ("GitHub only", {"GitHub"}),
        ("GitHub + VPN", {"GitHub", "VPN"}),
        ("GitHub + VPN + AWS", {"GitHub", "VPN", "AWS"}),
        ("all four (GitHub/VPN/AWS/GCP)", {"GitHub", "VPN", "AWS", "GCP"}),
    ]
    for name, drop in tiers:
        tier_excluded = int(pairs.loc[services.isin(drop), "n_views"].sum())
        tier_kept = total_views - tier_excluded
        tier_pct = 100 * tier_excluded / max(total_views, 1)
        print(f"    {name:<30}: kept {tier_kept:,}  (−{tier_excluded:,}, −{tier_pct:.2f}%)")

    print("\n--- Datasets most affected (by % of views excluded) ---")
    per_total = pairs.groupby("dataset_id")["n_views"].sum()
    per_excluded = pairs[excluded_mask].groupby("dataset_id")["n_views"].sum()
    frac = (per_excluded / per_total).fillna(0.0).sort_values(ascending=False)
    for dataset_id, f in frac.head(15).items():
        exc = int(per_excluded.get(dataset_id, 0))
        tot = int(per_total[dataset_id])
        print(f"    {dataset_id}: {exc:,}/{tot:,} excluded ({100 * f:.1f}%)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cache-dir", required=True, type=pathlib.Path)
    parser.add_argument("--no-encryption", action="store_true")
    parser.add_argument(
        "--dataset", default=None, help="Optional dataset-name substring to restrict to (omit for ALL datasets)"
    )
    parser.add_argument(
        "--cache-parquet",
        action="store_true",
        help="Cache per-(dataset, IP) view counts under <cache-dir>/analysis_cache/ so the sessionization walk "
        "runs once. IPs are stored as a salted hash; only the coarse region label is kept.",
    )
    parser.add_argument("--rebuild-cache", action="store_true", help="Ignore any existing parquet cache and rewalk.")
    parser.add_argument(
        "--max-assets",
        type=int,
        default=None,
        help="Process only the first N asset directories — a layout-independent smoke test that validates the "
        "script end-to-end quickly, regardless of how the cache top level is named.",
    )
    args = parser.parse_args()
    use_encryption = not args.no_encryption

    collect_asset_views, load_ip_cache, is_cloud_service_or_vpn_label = _load_library()

    cached = None
    if args.cache_parquet and not args.rebuild_cache:
        cached = _load_pairs(args.cache_dir)

    if cached is not None:
        pairs = cached
        print(f"Loaded {len(pairs):,} (dataset, IP) rows from analysis_cache (skipped the walk)")
        walked = False
    else:
        print("Loading ip_to_region cache (the exact source the summaries use)...")
        ip_to_region = load_ip_cache(
            cache_type="ip_to_region", cache_directory=args.cache_dir, use_encryption=use_encryption
        )
        print(f"  {len(ip_to_region):,} IPs in the region cache")
        pairs = build_view_pairs(
            cache_dir=args.cache_dir,
            use_encryption=use_encryption,
            dataset_filter=args.dataset,
            collect_asset_views=collect_asset_views,
            ip_to_region=ip_to_region,
            max_assets=args.max_assets,
        )
        walked = True

    if pairs.empty:
        print("No views found.")
        return

    # Print the report BEFORE persisting: the number is the deliverable, and a persistence
    # failure (e.g. a missing parquet engine) must never throw away a multi-hour walk.
    report(pairs, is_cloud_service_or_vpn_label)

    if args.cache_parquet and walked:
        written = _persist_pairs(pairs, args.cache_dir)
        if written is not None:
            print(f"\nCached {len(pairs):,} (dataset, IP) rows to {written} (IPs stored as a salted hash)")


if __name__ == "__main__":
    main()
