"""
Plot the relationships between NWB structure, asset size, and web access.

Consumes the CSV produced by ``build_dataset.py`` and emits three figures plus a
printed correlation summary:

  1. ``structure_relationships.png`` — pairwise relationships among the structural
     metrics (groups, datasets, total cophenetic index).
  2. ``consumption_mode.png`` — streaming vs. full downloads vs. download fraction
     as a function of structural complexity (the "scrub vs. download whole" split).
  3. ``size_vs_streaming.png`` — asset size as the dominant predictor of streaming,
     a predictor-strength ranking, and the size-controlled partial relationship of
     complexity.

Correlations are reported as Pearson (linear), Spearman (rank/monotonic), and
log-log Pearson (linear on log-log axes) because the distributions are heavily
right-skewed and the relationships are strongly nonlinear.

Requirements
------------
    pip install numpy pandas matplotlib

Usage
-----
    python plot_relationships.py --data access_structure.csv --out-dir figures/
"""

import argparse
import pathlib

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

plt.rcParams.update(
    {"font.size": 10, "axes.grid": True, "grid.alpha": 0.25, "axes.edgecolor": "#888", "axes.linewidth": 0.8}
)


def _pearson(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.corrcoef(a, b)[0, 1])


def _spearman(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.corrcoef(np.argsort(np.argsort(a)), np.argsort(np.argsort(b)))[0, 1])


def _loglog(a: np.ndarray, b: np.ndarray) -> float:
    mask = (a > 0) & (b > 0)
    return float(np.corrcoef(np.log10(a[mask]), np.log10(b[mask]))[0, 1])


def _hexbin(ax, x, y, *, xlog, ylog, cmap, xlabel, ylabel, title, fit=True):
    mask = np.ones(len(x), bool)
    if xlog:
        mask &= x > 0
    if ylog:
        mask &= y > 0
    x, y = x[mask], y[mask]
    hb = ax.hexbin(
        x,
        y,
        gridsize=40,
        xscale="log" if xlog else "linear",
        yscale="log" if ylog else "linear",
        cmap=cmap,
        mincnt=1,
        bins="log",
        linewidths=0,
    )
    if fit:
        xs = np.log10(x) if xlog else x
        ys = np.log10(y) if ylog else y
        slope, intercept = np.polyfit(xs, ys, 1)
        grid = np.linspace(xs.min(), xs.max(), 50)
        line = np.polyval((slope, intercept), grid)
        ax.plot(10**grid if xlog else grid, 10**line if ylog else line, color="#e8484a", lw=2)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title, fontsize=10)
    return hb


def _annotate(ax, text):
    ax.annotate(
        text,
        xy=(0.04, 0.96),
        xycoords="axes fraction",
        va="top",
        fontsize=8.5,
        bbox=dict(boxstyle="round", fc="white", ec="#bbb", alpha=0.88),
    )


def plot_structure(df: pd.DataFrame, out: pathlib.Path) -> None:
    g, d = df["groups"].to_numpy(float), df["datasets"].to_numpy(float)
    c = df["cophenetic_index"].to_numpy(float)
    fig, axes = plt.subplots(1, 3, figsize=(16.5, 5.2))
    fig.suptitle(f"Structural metrics across {len(df):,} valid NWB files", fontsize=13, fontweight="bold")
    hb = _hexbin(
        axes[0],
        g,
        d,
        xlog=True,
        ylog=True,
        cmap="viridis",
        xlabel="HDF5 groups",
        ylabel="HDF5 datasets",
        title="Groups vs. datasets",
    )
    _annotate(axes[0], f"Pearson {_pearson(g, d):+.2f}\nSpearman {_spearman(g, d):+.2f}")
    fig.colorbar(hb, ax=axes[0], label="file count (log)", pad=0.01)
    hb = _hexbin(
        axes[1],
        d,
        c,
        xlog=True,
        ylog=True,
        cmap="viridis",
        xlabel="HDF5 datasets",
        ylabel="total cophenetic index",
        title="Datasets vs. cophenetic index",
    )
    _annotate(axes[1], f"Pearson {_pearson(d, c):+.2f}\nSpearman {_spearman(d, c):+.2f}")
    fig.colorbar(hb, ax=axes[1], label="file count (log)", pad=0.01)
    hb = _hexbin(
        axes[2],
        g,
        c,
        xlog=True,
        ylog=True,
        cmap="viridis",
        xlabel="HDF5 groups",
        ylabel="total cophenetic index",
        title="Groups vs. cophenetic index",
    )
    _annotate(axes[2], f"Pearson {_pearson(g, c):+.2f}\nSpearman {_spearman(g, c):+.2f}")
    fig.colorbar(hb, ax=axes[2], label="file count (log)", pad=0.01)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Saved {out}")


def plot_consumption_mode(df: pd.DataFrame, out: pathlib.Path) -> None:
    d = df["datasets"].to_numpy(float)
    requests = df["number_of_requests"].to_numpy(float)
    downloads = df["number_of_downloads"].to_numpy(float)
    streaming = np.maximum(requests - downloads, 0.0)
    fraction = downloads / np.maximum(requests, 1.0)
    fig, axes = plt.subplots(1, 3, figsize=(16.5, 5.2))
    fig.suptitle(
        f"How consumption mode depends on file complexity (x = HDF5 datasets, {len(df):,} files)",
        fontsize=13,
        fontweight="bold",
    )
    hb = _hexbin(
        axes[0],
        d,
        streaming,
        xlog=True,
        ylog=True,
        cmap="viridis",
        xlabel="HDF5 datasets",
        ylabel="streaming requests",
        title="Streaming (requests - downloads)",
    )
    _annotate(axes[0], f"Spearman {_spearman(d, streaming):+.2f}\nlog-log {_loglog(d, streaming):+.2f}")
    fig.colorbar(hb, ax=axes[0], label="file count (log)", pad=0.01)
    hb = _hexbin(
        axes[1],
        d,
        downloads,
        xlog=True,
        ylog=True,
        cmap="magma",
        xlabel="HDF5 datasets",
        ylabel="full downloads",
        title="Full downloads",
    )
    _annotate(axes[1], f"Spearman {_spearman(d, downloads):+.2f}")
    fig.colorbar(hb, ax=axes[1], label="file count (log)", pad=0.01)
    hb = _hexbin(
        axes[2],
        d,
        fraction,
        xlog=True,
        ylog=False,
        cmap="cividis",
        xlabel="HDF5 datasets",
        ylabel="download fraction (dl / requests)",
        title="Download fraction",
    )
    _annotate(axes[2], f"Spearman {_spearman(d, fraction):+.2f}")
    fig.colorbar(hb, ax=axes[2], label="file count (log)", pad=0.01)
    fig.text(
        0.5,
        0.005,
        "Streaming rises weakly with complexity; downloads fall. Complex files are read via partial range "
        "requests, simple files downloaded whole. Requests privacy-rounded.",
        ha="center",
        fontsize=8,
        color="#666",
    )
    plt.tight_layout(rect=[0, 0.03, 1, 0.96])
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Saved {out}")


def plot_size_vs_streaming(df: pd.DataFrame, out: pathlib.Path) -> None:
    size = df["size_bytes"].to_numpy(float)
    g, d = df["groups"].to_numpy(float), df["datasets"].to_numpy(float)
    c = df["cophenetic_index"].to_numpy(float)
    streaming = np.maximum(df["number_of_requests"].to_numpy(float) - df["number_of_downloads"].to_numpy(float), 0.0)
    fig = plt.figure(figsize=(16.5, 5.4))
    fig.suptitle(
        f"Asset size is the dominant predictor of streaming requests ({len(df):,} files)",
        fontsize=13,
        fontweight="bold",
    )
    gs = fig.add_gridspec(1, 3, wspace=0.32)

    ax = fig.add_subplot(gs[0, 0])
    hb = _hexbin(
        ax,
        size,
        streaming,
        xlog=True,
        ylog=True,
        cmap="viridis",
        xlabel="asset size (bytes)",
        ylabel="streaming requests",
        title="Size vs. streaming",
    )
    _annotate(ax, f"Spearman {_spearman(size, streaming):+.2f}\nlog-log {_loglog(size, streaming):+.2f}")
    fig.colorbar(hb, ax=ax, label="file count (log)", pad=0.01)

    ax = fig.add_subplot(gs[0, 1])
    names = ["asset size", "groups", "datasets", "cophenetic"]
    vals = [_spearman(size, streaming), _spearman(g, streaming), _spearman(d, streaming), _spearman(c, streaming)]
    order = np.argsort(vals)
    ax.barh(range(len(vals)), [vals[i] for i in order], color="#4c78a8", height=0.62)
    ax.set_yticks(range(len(vals)))
    ax.set_yticklabels([names[i] for i in order])
    ax.set_xlim(0, max(vals) * 1.15)
    ax.set_xlabel("Spearman correlation with streaming requests")
    ax.set_title("Predictor strength (streaming)", fontsize=10)
    ax.grid(axis="y", visible=False)
    for i, idx in enumerate(order):
        ax.text(vals[idx] + 0.012, i, f"{vals[idx]:+.2f}", va="center", fontsize=9, color="#333")

    ax = fig.add_subplot(gs[0, 2])
    mask = (streaming > 0) & (size > 0) & (d > 0)
    log_size, log_stream, log_ds = np.log10(size[mask]), np.log10(streaming[mask]), np.log10(d[mask])
    resid_stream = log_stream - np.polyval(np.polyfit(log_size, log_stream, 1), log_size)
    resid_ds = log_ds - np.polyval(np.polyfit(log_size, log_ds, 1), log_size)
    hb = ax.hexbin(resid_ds, resid_stream, gridsize=40, cmap="cividis", mincnt=1, bins="log", linewidths=0)
    fig.colorbar(hb, ax=ax, label="file count (log)", pad=0.01)
    slope, intercept = np.polyfit(resid_ds, resid_stream, 1)
    grid = np.linspace(resid_ds.min(), resid_ds.max(), 50)
    ax.plot(grid, np.polyval((slope, intercept), grid), color="#e8484a", lw=2)
    ax.set_xlabel("datasets (residual after size)")
    ax.set_ylabel("streaming (residual after size)")
    ax.set_title("Does complexity add signal beyond size?", fontsize=10)
    _annotate(
        ax,
        f"partial r {np.corrcoef(resid_ds, resid_stream)[0, 1]:+.2f}\n"
        f"(size & datasets nearly orthogonal: r {_spearman(size, d):+.2f})",
    )
    fig.text(
        0.5,
        0.005,
        "Size dominates streaming access; structural complexity is nearly independent of size and adds a "
        "smaller, real signal on top. Requests privacy-rounded.",
        ha="center",
        fontsize=8,
        color="#666",
    )
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Saved {out}")


def print_summary(df: pd.DataFrame) -> None:
    size = df["size_bytes"].to_numpy(float)
    g, d = df["groups"].to_numpy(float), df["datasets"].to_numpy(float)
    c = df["cophenetic_index"].to_numpy(float)
    requests = df["number_of_requests"].to_numpy(float)
    downloads = df["number_of_downloads"].to_numpy(float)
    streaming = np.maximum(requests - downloads, 0.0)
    print(f"\n--- Correlation summary ({len(df):,} files) ---")
    print(f"{'predictor':>12} {'target':>16} {'Spearman':>9} {'log-log':>8}")
    for target, tname in [(streaming, "streaming"), (requests, "total requests"), (downloads, "downloads")]:
        for x, xname in [(size, "asset size"), (d, "datasets"), (g, "groups"), (c, "cophenetic")]:
            print(f"{xname:>12} {tname:>16} {_spearman(x, target):+9.3f} {_loglog(x, target):+8.3f}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data", required=True, type=pathlib.Path, help="CSV from build_dataset.py")
    parser.add_argument("--out-dir", default=pathlib.Path("."), type=pathlib.Path, help="Directory for figures")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(args.data)
    print(f"Loaded {len(df):,} joined rows from {args.data}")

    plot_structure(df, args.out_dir / "structure_relationships.png")
    plot_consumption_mode(df, args.out_dir / "consumption_mode.png")
    plot_size_vs_streaming(df, args.out_dir / "size_vs_streaming.png")
    print_summary(df)


if __name__ == "__main__":
    main()
