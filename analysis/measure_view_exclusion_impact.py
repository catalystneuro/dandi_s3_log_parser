"""
Dry-run: measure which IP origins contribute to ``number_of_views``, and how much
automated (bot/CI) traffic remains after the shipped exclusion.

This does NOT change any production code. It reuses the library's *actual* view
sessionizer (``_collect_asset_views``) and the *actual* region resolver
(``IpRegionResolver`` — the same object ``generate_summaries`` builds), so the counts
match what the summaries produce. Over the whole extraction cache (all datasets by
default) it reports:

  * total_views          — raw streaming sessions, every IP counted;
  * the shipped view count — total minus ``GH-actions`` (GitHub Actions), which is what
                             production ``number_of_views`` now excludes;
  * a per-service origin breakdown (GH-actions / GitHub / AWS / GCP / VPN / geographic)
    and a sensitivity table for broader exclusion tiers.

Testing-asset cross-tab (``--testing-asset-file``)
--------------------------------------------------
The shipped exclusion drops ``GH-actions`` only, deliberately sparing plausibly-human
cloud users (e.g. a Codespace notebook). But traffic to the reserved *testing* assets is
almost entirely automated, so any AWS/GCP/VPN — or high-volume "geographic" — IP hitting
them is a bot still being counted. Pass ``--testing-asset-file`` (fnmatch globs, one per
line, matched against the asset path relative to ``extraction/`` — use
``analysis/testing_blobs.txt`` for a content-addressed blobs/zarr cache) to break the
testing-asset views down by IP origin and list the top non-GitHub-Actions requesters,
the "few non-GitHub bots" that an asset-scoped exclusion would catch.

Efficient reruns (``--cache-parquet``)
--------------------------------------
The sessionization walk is the expensive part (hours). With ``--cache-parquet`` the
per-(dataset, is_testing, IP) view counts are written to
``<cache-dir>/analysis_cache/view_exclusion_pairs_v2.parquet`` — co-located with the
original data (falling back to ``.csv.gz`` if no parquet engine is installed). The report
always prints BEFORE this write, so a missing optional dependency can never discard a
multi-hour walk. The raw IP is NOT stored: it is replaced by a salted keyed hash, and the
resolved region *label* (e.g. ``GH-actions`` or ``USA/CA``) is kept so the breakdown can
be recomputed instantly. The stored label is frozen at build time; pass
``--rebuild-cache`` to rewalk after the resolver's databases update. (The ``_v2`` name
distinguishes this from caches written before the MaxMind/GH-actions labeling change; an
old cache is ignored, not silently reused.)

Usage
-----
    python measure_view_exclusion_impact.py --cache-dir /path/to/cache [--no-encryption] \\
        [--cache-parquet] [--dataset 000032] [--testing-asset-file analysis/testing_blobs.txt]

``--dataset`` restricts to one top-level directory for a fast first pass; omit it for the
full run. Encryption password via S3_LOG_EXTRACTION_PASSWORD. The resolver needs the
GeoLite2 database (MaxMind credentials on first use) and network access for the service
range listings, exactly as ``update summaries`` does.
"""

import argparse
import collections
import fnmatch
import hashlib
import os
import pathlib
import sys

import pandas as pd
import tqdm

_SERVICES = ("GH-actions", "GitHub", "AWS", "GCP", "VPN")


def _load_library():
    """Import the exact production functions so the measurement matches the summaries."""
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
    from s3_log_extraction.ip_utils import IpRegionResolver
    from s3_log_extraction.summarize._generate_summaries import _collect_asset_views

    return _collect_asset_views, IpRegionResolver


def _ip_hash_key() -> bytes:
    salt = (
        os.environ.get("S3_LOG_EXTRACTION_SALT") or os.environ.get("S3_LOG_EXTRACTION_PASSWORD") or "s3_log_extraction"
    )
    return salt.encode("utf-8")[:64]


def _service_of(label: str) -> str:
    """Coarse origin for the breakdown: a known service name, or ``geographic`` for a real place."""
    if not label:
        return "geographic"
    head = label.split("/", 1)[0]
    return head if head in _SERVICES else "geographic"


def _load_testing_globs(testing_asset_file: pathlib.Path | None) -> list[str]:
    """Read fnmatch globs (one per line, ``#`` comments and blanks skipped) identifying testing assets."""
    if testing_asset_file is None:
        return []
    globs = [
        stripped
        for line in testing_asset_file.read_text().splitlines()
        if (stripped := line.strip()) and not stripped.startswith("#")
    ]
    print(f"Loaded {len(globs)} testing-asset glob(s) from {testing_asset_file}")
    return globs


def _persist_pairs(pairs: pd.DataFrame, cache_dir: pathlib.Path) -> pathlib.Path | None:
    """
    Persist the (already privacy-safe: hashed IP + resolved label) pairs next to the data.

    Tries parquet first; if no parquet engine is installed, falls back to gzipped CSV so a
    multi-hour walk is never lost to a missing optional dependency. Returns the path written.
    """
    base = cache_dir / "analysis_cache"
    base.mkdir(parents=True, exist_ok=True)
    parquet_path = base / "view_exclusion_pairs_v2.parquet"
    try:
        pairs.to_parquet(parquet_path, index=False)
        return parquet_path
    except ImportError:
        csv_path = base / "view_exclusion_pairs_v2.csv.gz"
        pairs.to_csv(csv_path, index=False, compression="gzip")
        print("  (pyarrow/fastparquet not installed — wrote gzipped CSV instead; same hashed-IP contents)")
        return csv_path


def _load_pairs(cache_dir: pathlib.Path) -> pd.DataFrame | None:
    """Load a previously persisted v2 cache, preferring parquet then gzipped CSV. None if neither exists."""
    base = cache_dir / "analysis_cache"
    parquet_path = base / "view_exclusion_pairs_v2.parquet"
    csv_path = base / "view_exclusion_pairs_v2.csv.gz"
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path)
        except ImportError:
            pass
    if csv_path.exists():
        return pd.read_csv(
            csv_path, dtype={"dataset_id": str, "ip_hash": str, "region_label": str}, keep_default_na=False
        )
    return None


def build_view_pairs(
    cache_dir: pathlib.Path,
    use_encryption: bool,
    dataset_filter: str | None,
    collect_asset_views,
    resolver,
    testing_globs: list[str],
    max_assets: int | None = None,
) -> pd.DataFrame:
    """
    Walk the extraction cache, sessionize with the production code path, resolve each IP with
    the production resolver, and return a per-(dataset, is_testing, IP) view-count table:
    ``dataset_id``, ``is_testing``, ``ip_hash``, ``region_label``, ``n_views``.

    ``dataset_id`` is the top-level directory under ``extraction/`` (``dataset.name`` in the
    summaries). ``is_testing`` is whether the asset path matches any testing glob. Views are
    collected RAW (no resolver passed to ``_collect_asset_views``), so GitHub Actions traffic is
    measured rather than dropped; the raw IP is hashed and its resolved label retained.
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

    # (dataset_id, is_testing, ip) -> view count
    counts: dict[tuple[str, bool, str], int] = collections.defaultdict(int)
    skipped = 0
    testing_asset_count = 0
    for asset_dir in tqdm.tqdm(asset_dirs, desc="Sessionizing (production code path)"):
        relative_path = asset_dir.relative_to(extraction_root).as_posix()
        dataset_id = relative_path.split("/", 1)[0]
        is_testing = any(fnmatch.fnmatch(relative_path, glob) for glob in testing_globs)
        testing_asset_count += is_testing
        try:
            views = collect_asset_views(asset_directory=asset_dir, use_encryption=use_encryption)
        except RuntimeError:
            # The production reader raises on a corrupt/misaligned asset; a dry-run over the
            # whole cache should not die on one, so record and continue.
            skipped += 1
            continue
        for _view_date, ip in views:
            counts[(dataset_id, is_testing, ip)] += 1
    if skipped:
        print(f"  Skipped {skipped} asset(s) that raised the strict reader error")
    if testing_globs:
        print(f"  {testing_asset_count} of {len(asset_dirs)} assets matched a testing glob")

    # Resolve each distinct IP once (the resolver memoizes internally too).
    distinct_ips = {ip for _dataset_id, _is_testing, ip in counts}
    print(f"Resolving {len(distinct_ips):,} distinct IPs with the production resolver...")
    label_of = {ip: resolver.resolve(ip) for ip in tqdm.tqdm(distinct_ips, desc="Resolving IPs")}

    key = _ip_hash_key()
    rows = [
        {
            "dataset_id": dataset_id,
            "is_testing": bool(is_testing),
            "ip_hash": hashlib.blake2b(ip.encode("utf-8"), key=key, digest_size=16).hexdigest(),
            "region_label": label_of[ip] or "",
            "n_views": n,
        }
        for (dataset_id, is_testing, ip), n in counts.items()
    ]
    return pd.DataFrame(rows)


def _print_origin_breakdown(pairs: pd.DataFrame, indent: str = "    ") -> None:
    """Print views by coarse origin (service or geographic), most first, with percentages."""
    total = int(pairs["n_views"].sum())
    by_service: dict[str, int] = collections.defaultdict(int)
    for label, n in pairs[["region_label", "n_views"]].itertuples(index=False):
        by_service[_service_of(label)] += int(n)
    for service, count in sorted(by_service.items(), key=lambda kv: -kv[1]):
        print(f"{indent}{service:>10}: {count:,} ({100 * count / max(total, 1):.2f}%)")


def report(pairs: pd.DataFrame) -> None:
    services = pairs["region_label"].map(_service_of)
    total_views = int(pairs["n_views"].sum())
    gh_actions_views = int(pairs.loc[services == "GH-actions", "n_views"].sum())
    shipped_views = total_views - gh_actions_views

    print("\n=== View origins over the whole cache ===")
    print(f"  total_views (raw, every IP):                 {total_views:,}")
    print(f"  shipped number_of_views (minus GH-actions):  {shipped_views:,}")
    gh_pct = 100 * gh_actions_views / max(total_views, 1)
    print(f"  GH-actions views excluded by production:     {gh_actions_views:,} ({gh_pct:.2f}%)")
    print("\n  views by origin:")
    _print_origin_breakdown(pairs)

    print("\n--- Sensitivity: number_of_views under broader exclusion tiers ---")
    tiers = [
        ("GH-actions only (shipped)", {"GH-actions"}),
        ("GH-actions + VPN", {"GH-actions", "VPN"}),
        ("GH-actions + all GitHub", {"GH-actions", "GitHub"}),
        ("GH-actions + AWS/GCP/VPN", {"GH-actions", "AWS", "GCP", "VPN"}),
        ("all cloud/VPN/CI", set(_SERVICES)),
    ]
    for name, drop in tiers:
        tier_excluded = int(pairs.loc[services.isin(drop), "n_views"].sum())
        tier_kept = total_views - tier_excluded
        tier_pct = 100 * tier_excluded / max(total_views, 1)
        print(f"    {name:<28}: kept {tier_kept:,}  (−{tier_excluded:,}, −{tier_pct:.2f}%)")

    print("\n--- Datasets most affected (by % of views from GH-actions) ---")
    per_total = pairs.groupby("dataset_id")["n_views"].sum()
    per_gh = pairs[services == "GH-actions"].groupby("dataset_id")["n_views"].sum()
    frac = (per_gh / per_total).fillna(0.0).sort_values(ascending=False)
    for dataset_id, f in frac.head(15).items():
        exc = int(per_gh.get(dataset_id, 0))
        tot = int(per_total[dataset_id])
        print(f"    {dataset_id}: {exc:,}/{tot:,} from GH-actions ({100 * f:.1f}%)")


def report_testing_crosstab(pairs: pd.DataFrame, top_n: int = 25) -> None:
    """Break testing-asset views down by IP origin and surface the non-GitHub-Actions bots."""
    if "is_testing" not in pairs.columns or not pairs["is_testing"].any():
        print("\n(no testing-asset views found — pass --testing-asset-file with globs that match the cache)")
        return

    testing = pairs[pairs["is_testing"]].copy()
    services = testing["region_label"].map(_service_of)
    total = int(testing["n_views"].sum())
    gh_actions = int(testing.loc[services == "GH-actions", "n_views"].sum())
    # Everything automated-looking that the shipped GH-actions filter does NOT remove:
    other_service_mask = services.isin({"GitHub", "AWS", "GCP", "VPN"})
    other_service_views = int(testing.loc[other_service_mask, "n_views"].sum())

    print("\n=== Testing-asset traffic by IP origin ===")
    print(f"  testing-asset views (total):                 {total:,}")
    print(f"  from GH-actions (already excluded):          {gh_actions:,} ({100 * gh_actions / max(total, 1):.2f}%)")
    print(
        f"  from other cloud/VPN, still counted:         {other_service_views:,} "
        f"({100 * other_service_views / max(total, 1):.2f}%)"
    )
    print("\n  testing-asset views by origin:")
    _print_origin_breakdown(testing)

    # The "few non-GitHub bots": individual requesters on testing assets that are NOT GH-actions,
    # ranked by view count. High counts under AWS/GCP/VPN are cloud CI; high counts under a
    # geographic label are candidate residential/monitoring bots (a human would not amass many).
    non_gh = testing[services != "GH-actions"]
    per_ip = (
        non_gh.groupby(["ip_hash", "region_label"], as_index=False)["n_views"]
        .sum()
        .sort_values("n_views", ascending=False)
    )
    print(f"\n  top {top_n} non-GitHub-Actions requesters on testing assets (candidates to exclude):")
    print(f"    {'ip_hash (salted)':<34} {'origin label':<20} {'views':>8}")
    for ip_hash, label, n in per_ip.head(top_n).itertuples(index=False):
        print(f"    {ip_hash:<34} {label or '(unresolved)':<20} {int(n):>8,}")
    distinct_non_gh_service_ips = non_gh.loc[
        non_gh["region_label"].map(_service_of).isin({"GitHub", "AWS", "GCP", "VPN"}), "ip_hash"
    ].nunique()
    print(f"\n  distinct non-GH-actions cloud/VPN IPs on testing assets: {distinct_non_gh_service_ips:,}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cache-dir", required=True, type=pathlib.Path)
    parser.add_argument("--no-encryption", action="store_true")
    parser.add_argument(
        "--dataset", default=None, help="Optional top-level-name substring to restrict to (omit for ALL datasets)"
    )
    parser.add_argument(
        "--testing-asset-file",
        type=pathlib.Path,
        default=None,
        help="File of fnmatch globs (one per line) identifying testing assets, matched against the asset path "
        "relative to extraction/ (e.g. analysis/testing_blobs.txt for a blobs/zarr cache). Enables the cross-tab.",
    )
    parser.add_argument(
        "--cache-parquet",
        action="store_true",
        help="Cache per-(dataset, is_testing, IP) view counts under <cache-dir>/analysis_cache/ so the walk "
        "runs once. IPs are stored as a salted hash; only the resolved region label is kept.",
    )
    parser.add_argument("--rebuild-cache", action="store_true", help="Ignore any existing v2 cache and rewalk.")
    parser.add_argument(
        "--max-assets",
        type=int,
        default=None,
        help="Process only the first N asset directories — a layout-independent smoke test that validates the "
        "script end-to-end quickly, regardless of how the cache top level is named.",
    )
    args = parser.parse_args()
    use_encryption = not args.no_encryption

    collect_asset_views, ip_region_resolver_cls = _load_library()
    testing_globs = _load_testing_globs(args.testing_asset_file)

    cached = None
    if args.cache_parquet and not args.rebuild_cache:
        cached = _load_pairs(args.cache_dir)

    if cached is not None:
        pairs = cached
        print(f"Loaded {len(pairs):,} (dataset, is_testing, IP) rows from analysis_cache (skipped the walk)")
        if testing_globs and "is_testing" not in pairs.columns:
            print("  NOTE: cached rows predate testing-asset support; pass --rebuild-cache to compute the cross-tab.")
        walked = False
    else:
        print("Building the production region resolver (GeoLite2 + service ranges)...")
        with ip_region_resolver_cls(cache_directory=args.cache_dir) as resolver:
            pairs = build_view_pairs(
                cache_dir=args.cache_dir,
                use_encryption=use_encryption,
                dataset_filter=args.dataset,
                collect_asset_views=collect_asset_views,
                resolver=resolver,
                testing_globs=testing_globs,
                max_assets=args.max_assets,
            )
        walked = True

    if pairs.empty:
        print("No views found.")
        return

    # Print the report BEFORE persisting: the number is the deliverable, and a persistence
    # failure (e.g. a missing parquet engine) must never throw away a multi-hour walk.
    report(pairs)
    if testing_globs or ("is_testing" in pairs.columns and pairs["is_testing"].any()):
        report_testing_crosstab(pairs)

    if args.cache_parquet and walked:
        written = _persist_pairs(pairs, args.cache_dir)
        if written is not None:
            print(f"\nCached {len(pairs):,} rows to {written} (IPs stored as a salted hash)")


if __name__ == "__main__":
    main()
