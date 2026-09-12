# Supplement: tree-shape metrics for NWB/HDF5 hierarchies

*Companion to `WHITEPAPER.md`. Why the binary-tree baseline used by the Sackin
index is a poor fit for NWB structure, and which alternative metrics are more
appropriate.*

## 1. The problem with a binary baseline

Most tree-balance indices — Sackin, Colless — come from **phylogenetics**, where
trees are **bifurcating** (every internal node has exactly two children). Their
"balanced" reference and their normalizations assume that world.

NWB/HDF5 hierarchies are different in ways that matter:

- **Multifurcating / high fan-out.** A group routinely holds tens of datasets as
  direct children. Out-degree is unbounded and *meaningful*.
- **Shallow by convention.** The NWB schema keeps depth small (a handful of levels)
  regardless of how many objects a file contains.
- **Internal nodes carry information.** Groups hold attributes; a group can also be
  an empty leaf. "Leaf = dataset" is an approximation.

Under these conditions the Sackin min-max normalization,
$S_{\text{norm}} = (S - S_{\min})/(S_{\max} - S_{\min})$ with
$S_{\min} = n\lceil\log_2 n\rceil$ (a balanced **binary** tree), compares each file
to a minimum it **cannot reach** — a binary tree is the wrong null for a fan-out
format. The reference, not the data, is the anomaly.

**Why every published Sackin value is negative.** Because NWB groups hold many
datasets as direct children, leaves sit at depth ~2–3 regardless of count, so the
true leaf-depth sum $S$ falls *below* the binary minimum $S_{\min}$, making
$S - S_{\min} < 0$ and the normalized index negative. A negative value therefore
means the file is **flatter/bushier than a balanced binary tree** — as broad-and-
shallow scientific containers should be — not that it is unusual. The magnitude
tracks size (small files are flattest, so most negative; larger files accrue depth
and climb toward 0), i.e. the Sackin value is effectively a nonlinear restatement of
file size rather than an independent axis.

**Two principled fixes** (either removes the negativity and the binary bias):

1. **Correct the achievable minimum.** The shallowest tree with $n$ leaves and
   maximum out-degree $d$ is a balanced $d$-ary tree, giving
   $S_{\min}\approx n\lceil\log_d n\rceil$. Using the file's own observed fan-out
   for $d$ (e.g., max or mean out-degree) makes $S_{\min}$ attainable and lifts the
   index back into $[0, 1]$.
2. **Abandon min-max; standardize against a null.** Report the raw index as a
   **z-score or quantile** relative to a distribution of random trees with the same
   size (and ideally the same degree constraints). This is what phylogenetics
   actually does — comparing to Yule or PDA models — rather than min-max. For NWB a
   format-specific null (e.g., re-attaching the same objects under randomized
   parents, or resampling from the empirical schema) is more meaningful than either
   textbook model.

## 2. Metrics that do not assume binary trees

| Metric | What it captures | Binary-safe? | Notes for NWB |
|---|---|---|---|
| **Mean / max leaf depth** | how deep the hierarchy is | yes | Mean depth = Sackin / $n$; the simplest size-normalized depth summary |
| **Out-degree stats** (mean / max / variance of children per group) | fan-out / breadth | yes | The **NWB-native** shape axis the binary baseline throws away |
| **Total cophenetic index** $\Phi$ (Mir, Rosselló & Rotger, 2013) | balance via $\sum_{\{i,j\}} \mathrm{depth}(\mathrm{LCA}(i,j))$ over leaf pairs | **yes** — defined for arbitrary trees; min = star, max = caterpillar | Better resolution than Sackin/Colless; a drop-in balance index without the binary assumption |
| **Colless-like indices** (Mir, Rosselló & Rotger, 2018) | node-level imbalance generalized to multifurcating trees | yes | Parameterized by a node dissimilarity + weight; principled multifurcating Colless |
| **Shao & Sokal $B_1$** (1990) | balance via $\sum_{\text{internal}} 1/(\text{subtree height})$ | yes | Simple, degree-agnostic |
| **Shao & Sokal $B_2$ / tree entropy** (1990) | Shannon entropy of the leaf-reachability distribution under equiprobable descent | yes | Naturally rewards both depth and fan-out; scale-comparable; arguably the most information-theoretically honest single number |
| **Internal:leaf ratio, max breadth (width)** | compactness / shape | yes | Cheap descriptive complements |

## 3. Recommendation for NWB

Because the interesting structural variation in NWB is **size, depth, and fan-out**
— and balance indices *conflate* these — a single "balance" scalar is the wrong
tool. We suggest:

- **A short descriptive panel** rather than one index: leaf count (≈ datasets),
  mean and max depth, and out-degree mean/max/variance. These are interpretable,
  degree-agnostic, and separate the axes the Sackin index blends together.
- **If a single balance scalar is wanted**, prefer the **total cophenetic index**
  or **$B_2$ (tree entropy)** — both defined for arbitrary-degree trees — and
  report it **size-conditioned** (z-score/quantile against a size-matched null, or
  the residual after regressing on $\log$ leaf count), never min-max against a
  binary extreme.
- **Keep expectations calibrated.** The main white paper shows structural
  complexity (in any of these forms) is nearly uncorrelated with access volume and
  is largely a restatement of size. So the value of these metrics is
  **descriptive / QC** — characterizing or flagging structurally unusual files —
  **not** normalizing view counts. None of them rescues structural complexity as an
  access-normalizer.

## 4. Empirical check: the corrected metrics behave identically

We computed the **total cophenetic index** and **out-degree statistics** over the
same file set (via the `dandi-cache/valid-nwb-file-to-cophenetic-index` and
`…-to-out-degrees` caches) and joined them to asset size and streaming-request
counts (4,493 files with all quantities present). The point of the exercise was to
test whether the binary-baseline critique of the Sackin index was the reason
structural complexity failed to predict access. It was not.

| predictor | Spearman with streaming | log-log |
|---|---|---|
| asset size | **+0.75** | +0.70 |
| n_internal_nodes | +0.18 | +0.42 |
| total cophenetic index | +0.14 | +0.39 |
| max out-degree | −0.06 | +0.19 |
| mean out-degree | +0.04 | +0.11 |
| variance out-degree | −0.08 | +0.09 |

Findings:

- **The "fair" metric matches the "unfair" one.** The total cophenetic index
  correlates with streaming at +0.14 — the same as the raw datasets count (+0.16)
  and the binary-baseline Sackin index (≈+0.12 in an earlier run). Fixing the
  baseline changes nothing: structural complexity genuinely does not drive access,
  however correctly it is measured.
- **The cophenetic index is complexity/size restated.** It is rank-correlated 0.86
  with internal-node count and near-orthogonal to byte-size (−0.11), and adds the
  same size-controlled partial signal (partial $r \approx 0.49$) that the dataset
  count did — no new axis.
- **Out-degree is even weaker.** The NWB-native breadth axis (mean/max/variance of
  children per group) is essentially uncorrelated with access (all $|\rho| \le 0.08$).
  Interestingly, max out-degree is *negatively* related to size (−0.30) — bushier
  files tend to be smaller — but neither predicts who accesses them.

This closes the loop the main paper opened: **none of the degree-appropriate
metrics rescues structural complexity as an access-normalizer.** Size remains the
only real predictor, so the recommendation stands unchanged.

## 5. Caveats

- The metrics above treat the hierarchy as an unlabeled rooted tree; they ignore
  dataset *shapes*, dtypes, and chunking, which may matter more than tree shape for
  some questions (e.g., streaming cost).
- Attributions here are to the phylogenetics literature (Sackin 1972; Colless 1982;
  Shao & Sokal 1990; total cophenetic index, Mir–Rosselló–Rotger 2013;
  Colless-like multifurcating indices, Mir–Rosselló–Rotger 2018); consult the
  primary sources for exact definitions and extremal-tree proofs before
  implementing.
