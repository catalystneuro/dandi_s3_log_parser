"""
Characterize automated ("bot") traffic in the extraction cache by temporal regularity.

Monitoring / CI traffic (mostly GitHub Actions hitting testing datasets, for DANDI)
is *metronomic*: an (IP, asset) pair is hit at near-constant intervals, persistently.
Humans are bursty and irregular. This script scores every (IP, asset) time series
for that signature so we can (a) see how cleanly regularity separates bots from
humans, (b) pick thresholds, and (c) quantify how much total activity is automated —
BEFORE any of it is wired into the extraction/summary pipeline.

Per (IP, asset) features (only pairs with >= --min-requests are scored):
  * n_requests, span_seconds
  * cv_gaps          — coefficient of variation of inter-request gaps (std/mean);
                       ~0 = metronomic = bot-like, high = bursty = human-like
  * dominant_fraction — share of gaps within +-tol of the median gap; ~1 = clockwork
  * median_gap, streaming_fraction

A pair is flagged automated when it persists over enough time AND is metronomic:
low cv_gaps, OR a dominant period accounting for most gaps whose length is at least
a floor (so sub-second burst streaming of large zarr stores is not mistaken for a
monitoring bot).

Caveat learned from the first real run: at DANDI scale the dominant automated
traffic is GitHub Actions (the dandi-cache CI streams every asset) which is
IRREGULAR, so temporal regularity misses most of it. The IP label (GitHub Actions,
plus the library's existing cloud/VPN classifier) is the primary bot signal; this
regularity detector is a secondary catch for non-GitHub periodic monitors. See the
label-coverage lines in the report.

Two optional ground-truth labels turn this from "eyeball it" into measured
precision/recall:
  * --testing-dataset-file / --testing-asset-file — DANDI testing datasets/assets,
    one dandiset id (or asset glob) per line (the asset side of the truth).
  * --github-meta — fetch GitHub Actions IP ranges from api.github.com/meta and label
    IPs that fall in them (the requester side of the truth).

Usage
-----
    python assess_bot_activity.py --cache-dir /path/to/cache [--no-encryption] \\
        [--testing-dataset-file testing_datasets.txt] [--github-meta] [--out bot_activity.png]

Requires numpy, pandas, matplotlib, tqdm (requests if --github-meta; pyarrow if
--cache-parquet). With --cache-parquet the scored feature table is written under
<cache-dir>/analysis_cache/ (co-located with the original data) with IP addresses
replaced by a salted keyed hash, so the multi-day walk only runs once.
"""

import argparse
import fnmatch
import hashlib
import ipaddress
import os
import pathlib
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tqdm

TIMESTAMP_FORMAT = "%y%m%d%H%M%S"


def _read_lines(path: pathlib.Path) -> list[str]:
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


def _read_ips(path: pathlib.Path, use_encryption: bool) -> list[str]:
    if not use_encryption:
        return _read_lines(path)
    sys.path.insert(0, str(pathlib.Path(__file__).parent / "src"))
    from s3_log_extraction.utils.encryption import read_text_from_file

    text = read_text_from_file(file_path=path, use_encryption=True)
    return [line.strip() for line in text.splitlines() if line.strip()]


def score_ip_asset_pairs(
    cache_dir: pathlib.Path,
    use_encryption: bool,
    min_requests: int,
    dominant_tol: float,
    testing_globs: list[str],
) -> pd.DataFrame:
    """Return one row per (IP, asset) pair with >= ``min_requests`` requests."""
    extraction_root = cache_dir / "extraction"
    if not extraction_root.exists():
        raise FileNotFoundError(f"No 'extraction' subdirectory under {cache_dir}")

    asset_dirs = []
    for dataset_dir in sorted(extraction_root.iterdir()):
        if not dataset_dir.is_dir():
            continue
        for asset_dir in dataset_dir.rglob("*"):
            if (asset_dir / "timestamps.txt").exists():
                asset_dirs.append(asset_dir)
    print(f"Found {len(asset_dirs)} asset directories")

    rows = []
    for asset_dir in tqdm.tqdm(asset_dirs, desc="Scoring assets"):
        asset_path = str(asset_dir.relative_to(extraction_root))
        downloads_path = asset_dir / "download.txt"
        ips_path = asset_dir / "ips.txt"
        if not (downloads_path.exists() and ips_path.exists()):
            continue
        timestamps_raw = _read_lines(asset_dir / "timestamps.txt")
        downloads_raw = _read_lines(downloads_path)
        ips_raw = _read_ips(ips_path, use_encryption)
        if not (len(timestamps_raw) == len(downloads_raw) == len(ips_raw)):
            continue

        is_testing = any(fnmatch.fnmatch(asset_path, g) for g in testing_globs)

        # Group this asset's requests by IP.
        by_ip: dict[str, list[tuple[int, int]]] = {}
        for ts_str, dl_str, ip in zip(timestamps_raw, downloads_raw, ips_raw):
            ts = pd.to_datetime(ts_str, format=TIMESTAMP_FORMAT, errors="coerce")
            if ts is pd.NaT:
                continue
            by_ip.setdefault(ip, []).append((int(ts.value // 10**9), 1 if dl_str == "0" else 0))

        for ip, events in by_ip.items():
            if len(events) < min_requests:
                continue
            events.sort()
            epochs = np.array([e for e, _ in events], dtype=np.float64)
            streaming = np.array([s for _, s in events], dtype=np.float64)
            gaps = np.diff(epochs)
            gaps = gaps[gaps > 0]
            if gaps.size < 2:
                continue
            mean_gap = gaps.mean()
            cv = float(gaps.std() / mean_gap) if mean_gap > 0 else np.nan
            median_gap = float(np.median(gaps))
            if median_gap > 0:
                dominant = float(np.mean(np.abs(gaps - median_gap) <= dominant_tol * median_gap))
            else:
                dominant = np.nan
            rows.append(
                {
                    "ip": ip,
                    "asset_path": asset_path,
                    "is_testing_asset": is_testing,
                    "n_requests": len(events),
                    "span_seconds": float(epochs[-1] - epochs[0]),
                    "cv_gaps": cv,
                    "dominant_fraction": dominant,
                    "median_gap": median_gap,
                    "streaming_fraction": float(streaming.mean()),
                }
            )

    df = pd.DataFrame(rows)
    print(f"Scored {len(df):,} (IP, asset) pairs with >= {min_requests} requests")
    return df


def label_github_actions_ips(df: pd.DataFrame) -> pd.Series:
    """Label IPs that fall within GitHub Actions CIDR ranges (api.github.com/meta)."""
    import requests

    meta = requests.get("https://api.github.com/meta", timeout=60).json()
    networks = [ipaddress.ip_network(c, strict=False) for c in meta.get("actions", [])]
    print(f"  GitHub Actions ranges: {len(networks)} CIDRs")

    cache: dict[str, bool] = {}

    def _is_gh(ip: str) -> bool:
        if ip not in cache:
            try:
                addr = ipaddress.ip_address(ip)
                cache[ip] = any(addr in net for net in networks)
            except ValueError:
                cache[ip] = False
        return cache[ip]

    return df["ip"].map(_is_gh)


def flag_automated(
    df: pd.DataFrame,
    cv_threshold: float,
    dominant_threshold: float,
    min_span_seconds: float,
    min_period_seconds: float = 60.0,
) -> pd.Series:
    """
    Metronomic-and-persistent (IP, asset) pairs.

    A pair is metronomic if its gaps have low CV, OR if a dominant period accounts
    for most gaps AND that period is at least ``min_period_seconds``. The period
    floor is essential: high-volume burst streaming (e.g. a zarr flooded with
    thousands of chunk requests per second) has a near-zero median gap, so almost
    all gaps fall within +-10% of it and the dominant-fraction test fires spuriously.
    Real monitoring pings on the order of minutes to days, never sub-second, so the
    floor removes those false positives without losing genuine periodic bots.
    """
    persistent = df["span_seconds"] >= min_span_seconds
    low_cv = df["cv_gaps"] <= cv_threshold
    dominant_period = (df["dominant_fraction"] >= dominant_threshold) & (df["median_gap"] >= min_period_seconds)
    return persistent & (low_cv | dominant_period)


def plot(df: pd.DataFrame, out_path: pathlib.Path, has_gh: bool) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(16.5, 5.0))
    fig.suptitle(f"Automated-traffic signature across {len(df):,} (IP, asset) pairs", fontsize=13, fontweight="bold")

    # Panel 1: CV histogram, split by testing-asset label if available.
    ax = axes[0]
    cv = df["cv_gaps"].to_numpy(float)
    cv = cv[np.isfinite(cv) & (cv > 0)]
    if df["is_testing_asset"].any():
        for label, sub, color in [
            ("testing asset", df[df["is_testing_asset"]], "tomato"),
            ("other asset", df[~df["is_testing_asset"]], "steelblue"),
        ]:
            c = sub["cv_gaps"].to_numpy(float)
            c = c[np.isfinite(c) & (c > 0)]
            if c.size:
                ax.hist(np.log10(c), bins=50, alpha=0.6, label=label, color=color, edgecolor="none")
        ax.legend(fontsize=8, loc="upper right")
    else:
        ax.hist(np.log10(cv), bins=60, color="steelblue", alpha=0.85, edgecolor="none")
    ax.set_xlabel("coefficient of variation of gaps (log₁₀)")
    ax.set_ylabel("(IP, asset) pairs")
    ax.set_title("Gap regularity\n(low CV = metronomic = bot-like)", fontsize=9)
    ax.axvline(np.log10(0.1), color="black", ls="--", lw=0.8, label="CV = 0.1")

    # Panel 2: n_requests vs CV, colored by label.
    ax = axes[1]
    color_key = "is_github_actions_ip" if has_gh else "is_testing_asset"
    for flag, color, label in [
        (True, "tomato", color_key.replace("is_", "").replace("_", " ")),
        (False, "steelblue", "other"),
    ]:
        sub = df[df[color_key] == flag]
        c = sub["cv_gaps"].to_numpy(float)
        n = sub["n_requests"].to_numpy(float)
        m = np.isfinite(c) & (c > 0)
        ax.scatter(n[m], c[m], s=6, alpha=0.3, color=color, label=label, linewidths=0)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("requests in the (IP, asset) pair")
    ax.set_ylabel("CV of gaps")
    ax.set_title(f"Count vs. regularity\n(colour = {color_key})", fontsize=9)
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(True, which="both", alpha=0.2)

    # Panel 3: dominant-period fraction histogram.
    ax = axes[2]
    dom = df["dominant_fraction"].to_numpy(float)
    dom = dom[np.isfinite(dom)]
    ax.hist(dom, bins=50, color="darkorange", alpha=0.85, edgecolor="none")
    ax.set_xlabel("fraction of gaps within ±10% of median")
    ax.set_ylabel("(IP, asset) pairs")
    ax.set_title("Period concentration\n(~1.0 = clockwork)", fontsize=9)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved {out_path}")


def report(df: pd.DataFrame, flagged: pd.Series, has_gh: bool) -> None:
    total_requests = int(df["n_requests"].sum())
    flagged_requests = int(df.loc[flagged, "n_requests"].sum())
    pct = 100 * flagged_requests / max(total_requests, 1)
    print("\n--- Automated-traffic flag summary ---")
    print(f"  flagged (IP, asset) pairs: {int(flagged.sum()):,} / {len(df):,}")
    print(f"  requests in flagged pairs: {flagged_requests:,} / {total_requests:,} ({pct:.2f}%)")

    # Label coverage independent of the flag: how much traffic each label alone accounts
    # for. For DANDI the IP label (GitHub Actions) is expected to dominate, since the
    # dandi-cache CI streams every asset — traffic the regularity flag cannot catch.
    if has_gh:
        gh_pairs = int(df["is_github_actions_ip"].sum())
        gh_requests = int(df.loc[df["is_github_actions_ip"], "n_requests"].sum())
        print("\n  GitHub-Actions IP coverage (independent of the flag):")
        print(f"    pairs:    {gh_pairs:,} / {len(df):,} ({100 * gh_pairs / max(len(df), 1):.1f}%)")
        print(f"    requests: {gh_requests:,} / {total_requests:,} ({100 * gh_requests / max(total_requests, 1):.2f}%)")
    if df["is_testing_asset"].any():
        t_pairs = int(df["is_testing_asset"].sum())
        t_requests = int(df.loc[df["is_testing_asset"], "n_requests"].sum())
        print("\n  Testing-asset coverage (independent of the flag):")
        print(f"    pairs:    {t_pairs:,} / {len(df):,} ({100 * t_pairs / max(len(df), 1):.1f}%)")
        print(f"    requests: {t_requests:,} / {total_requests:,} ({100 * t_requests / max(total_requests, 1):.2f}%)")

    if df["is_testing_asset"].any():
        tp = int((flagged & df["is_testing_asset"]).sum())
        print("\n  vs. testing-asset label:")
        print(
            f"    flagged pairs on testing assets:     {tp:,} ({100 * tp / max(int(flagged.sum()), 1):.1f}% of flagged)"
        )
        recall = 100 * int((flagged & df["is_testing_asset"]).sum()) / max(int(df["is_testing_asset"].sum()), 1)
        print(f"    testing-asset pairs that are flagged: {recall:.1f}% (recall on the asset label)")

    if has_gh:
        tp = int((flagged & df["is_github_actions_ip"]).sum())
        gh_total = int(df["is_github_actions_ip"].sum())
        print("\n  vs. GitHub-Actions-IP label:")
        print(
            f"    flagged pairs from GH-Actions IPs:   {tp:,} ({100 * tp / max(int(flagged.sum()), 1):.1f}% of flagged)"
        )
        print(f"    GH-Actions pairs that are flagged:    {100 * tp / max(gh_total, 1):.1f}% (recall on the IP label)")

    print("\n--- CV-threshold sweep (fraction of activity flagged as metronomic) ---")
    for cv in [0.02, 0.05, 0.1, 0.2, 0.5]:
        mask = (df["cv_gaps"] <= cv) & (df["span_seconds"] >= 86400)
        req = int(df.loc[mask, "n_requests"].sum())
        pct_req = 100 * req / max(total_requests, 1)
        print(f"  CV <= {cv:<4} & span >= 1d: {int(mask.sum()):>8,} pairs, {req:>12,} requests ({pct_req:.2f}%)")

    print("\n--- Top flagged (IP, asset) pairs by request count ---")
    top = df[flagged].nlargest(12, "n_requests")
    for _, r in top.iterrows():
        tags = (" [testing]" if r["is_testing_asset"] else "") + (
            " [gh-actions]" if has_gh and r["is_github_actions_ip"] else ""
        )
        gap_h = r["median_gap"] / 3600
        head = f"  {int(r['n_requests']):>8,} reqs  CV={r['cv_gaps']:.3f}  median_gap={gap_h:.1f}h"
        print(f"{head}  {r['asset_path']}{tags}")


def _secure_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of the scored feature frame safe to persist next to the cache.

    The raw ``ip`` column is replaced by a salted keyed hash (``ip_hash``), so no IP
    address is ever written to disk in the clear. The hash key is derived from the
    same ``S3_LOG_EXTRACTION_SALT`` / ``S3_LOG_EXTRACTION_PASSWORD`` environment the
    library uses; grouping/identity is preserved (same IP -> same hash) but the value
    is not reversible. Bot labels (``is_github_actions_ip``) must therefore be
    computed before caching, since they cannot be recovered from the hash.
    """
    key = (
        os.environ.get("S3_LOG_EXTRACTION_SALT") or os.environ.get("S3_LOG_EXTRACTION_PASSWORD") or "s3_log_extraction"
    ).encode("utf-8")[:64]
    out = df.copy()
    out["ip_hash"] = out["ip"].map(lambda ip: hashlib.blake2b(ip.encode("utf-8"), key=key, digest_size=16).hexdigest())
    return out.drop(columns=["ip"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cache-dir", required=True, type=pathlib.Path)
    parser.add_argument("--no-encryption", action="store_true")
    parser.add_argument("--min-requests", type=int, default=5, help="Minimum requests to score a pair (default 5)")
    parser.add_argument("--cv-threshold", type=float, default=0.1, help="Metronomic if CV <= this (default 0.1)")
    parser.add_argument(
        "--dominant-threshold", type=float, default=0.6, help="or dominant fraction >= this (default 0.6)"
    )
    parser.add_argument("--min-span-hours", type=float, default=24.0, help="Persistent if span >= this (default 24h)")
    parser.add_argument(
        "--testing-dataset-file",
        type=pathlib.Path,
        default=None,
        help="Text file of testing dandiset ids, one per line; each becomes a '<id>/*' asset match.",
    )
    parser.add_argument(
        "--testing-asset-file", type=pathlib.Path, default=None, help="File of testing-asset globs, one per line"
    )
    parser.add_argument("--github-meta", action="store_true", help="Label GitHub Actions IPs via api.github.com/meta")
    parser.add_argument(
        "--cache-parquet",
        action="store_true",
        help="Cache the scored (IP, asset) feature table as parquet under <cache-dir>/analysis_cache/ so the "
        "multi-day walk runs once. IPs are stored as a salted keyed hash, never in the clear; the file lives "
        "inside the cache directory alongside the original data. Rebuild with --rebuild-cache if labels change.",
    )
    parser.add_argument("--rebuild-cache", action="store_true", help="Ignore any existing parquet cache and rescore.")
    parser.add_argument("--out", default="bot_activity.png", type=pathlib.Path)
    args = parser.parse_args()

    def _read_lines_file(path: pathlib.Path) -> list[str]:
        return [line.strip() for line in path.read_text().splitlines() if line.strip() and not line.startswith("#")]

    testing_globs: list[str] = []
    if args.testing_dataset_file:
        testing_globs += [f"{ds}/*" for ds in _read_lines_file(args.testing_dataset_file)]
    if args.testing_asset_file:
        testing_globs += _read_lines_file(args.testing_asset_file)

    cache_path = args.cache_dir / "analysis_cache" / "bot_activity_pairs.parquet"

    if args.cache_parquet and cache_path.exists() and not args.rebuild_cache:
        df = pd.read_parquet(cache_path)
        print(f"Loaded {len(df):,} scored pairs from cache {cache_path} (skipped the cache walk)")
    else:
        df = score_ip_asset_pairs(
            cache_dir=args.cache_dir,
            use_encryption=not args.no_encryption,
            min_requests=args.min_requests,
            dominant_tol=0.1,
            testing_globs=testing_globs,
        )
        if df.empty:
            print("No (IP, asset) pairs met the minimum request count.")
            return
        if args.github_meta:
            print("Labeling GitHub Actions IPs...")
            df["is_github_actions_ip"] = label_github_actions_ips(df)
        if args.cache_parquet:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            _secure_feature_frame(df).to_parquet(cache_path, index=False)
            print(f"Cached {len(df):,} scored pairs to {cache_path} (IPs stored as a salted hash)")

    if df.empty:
        print("No (IP, asset) pairs met the minimum request count.")
        return

    has_gh = "is_github_actions_ip" in df.columns

    flagged = flag_automated(
        df,
        cv_threshold=args.cv_threshold,
        dominant_threshold=args.dominant_threshold,
        min_span_seconds=args.min_span_hours * 3600,
    )
    plot(df, out_path=args.out, has_gh=has_gh)
    report(df, flagged, has_gh=has_gh)


if __name__ == "__main__":
    main()
