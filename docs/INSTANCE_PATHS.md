# Enter instance paths

On Design Setup, enable **Skip hierarchy discovery; enter instance paths** before
loading or selecting a Test. Crescendo shows these controls in Pro mode. Load
and Inspect still reads saved Tests; selecting a Test in this mode does not
start a cells-only tree scan. Open **Enter instance paths…**, validate the input,
then use **Apply Selected Cells** to inspect parameters in the accepted scopes.
The traditional tree workflow remains available when this option is off.

Each nonblank line accepts either `/I0/IARR<3>/M1` or
`lib/cell/view /I0/IARR<3>/M1` (the separating space is optional). The identity
prefix identifies the selected Test's root design, including its config view,
not the master of the final instance. Unqualified paths require exactly one
root design. Paths are absolute, case-sensitive, and match complete components:
`/I1` never includes `/I10`. `/` selects the whole design. A module includes its
subtree; a leaf selects only that device. Blank lines are ignored, original line
numbers are retained, and duplicate scopes are deduplicated. Failed lines retain
an individual reason; valid lines remain selected and can be applied.

Names start with an ASCII letter, `_` or `$`, followed by ASCII letters, digits,
`_`, `$`, `.` or `-`, optionally ending in one `<integer>` scalar index.
`<3>` and `<-1>` are literal OA names. They do not select an individual bit of a
stored iterated instance such as `IARR<3:0>`. Ranges `<a:b>`, multidimensional
indices, square brackets, wildcards, escaped names, trailing/double slashes and
other forms are unsupported and are reported per line. Expand or rename the
source hierarchy explicitly if individual iterated bits are required. Existing
config restrictions, including unsupported bit-specific HDB rules, still apply;
there is no schematic substitution for an unresolved config.

The native probe visits only ancestors needed to reach requested scopes and
those scopes' descendants. It filters before child-master discovery, HDB
occurrence binding, instance CDF access and parameter evaluation. Ancestor OA
cells and config PCDB metadata must still be read to resolve a path. Exact OA
instance lookup avoids examining unrelated ancestor-instance masters; matching
siblings retain OA order. Native process startup and ancestor/config opening are
fixed costs, so three shallow paths on a small design can still take nearly as
long as tree discovery;
this is not a constant-time lookup or a full-tree discovery. Missing paths may
have reachable ancestors, so the service seals a fresh probe of the accepted
scopes before Apply. Reference-library internals and ignored or opaque instances
retain the existing safety policy. Top-level source controls and parameters
retain their existing eligibility rules.

The scope IDs and selected module records are exactly those used by tree
selection. Configurations persist canonical design/path requests in optional
`instance_path_selection` metadata with schema `crescendo.instance_paths.v1`, in
addition to the normal selected-cell evidence. Configuration loading validates
and resolves those paths again before matching and Apply. Checkpoints use the
existing `cell_selection` fields, checksums and source/config identity guards;
entry method does not become a new optimization dimension. Manual checkpoints
also record `cell_selection.discovery` with schema `crescendo.scoped_discovery.v1`
so a reconstruction without a frozen baseline repeats the same branch-only
probe and retains its hierarchy/source fingerprints. Old configurations
without the optional metadata and existing checkpoints retain their semantics.
A checkpoint without the new discovery marker keeps its original full metadata
scan; a manually scoped checkpoint repeats its scoped probe when reconstruction
is needed. Neither path drops the source/config identity checks.

Regression coverage includes line-level errors, exact design/view identity,
scalar indices, no fallback on empty input, early native pruning, module scope
and checkpoint equivalence, configuration round trips, huge synthetic selection,
real Tk interaction, plot invalidation and corner-name display. Native fixture
and performance receipts are produced separately from dependency-free unit tests.

The synthetic huge demo still constructs deterministic topology and advances its
fixture RNG to preserve existing values, but a cold manual request materializes
parameter records only for the requested subtrees. Apply reuses that scoped
source, rather than constructing the full parameter catalogue. Measurements
separate this fixture cost from native OA probing.

Crescendo Pareto cards draw into the page's shared canvas viewport. Wheel and
scrollbar movement retain item IDs and move the viewport only; candidate clicks
use the same candidate IDs. Data changes replace only the affected plot's tagged
items, and page-width changes coalesce before relayout. A standalone plot retains
the same resize debounce and unchanged-data cache.
