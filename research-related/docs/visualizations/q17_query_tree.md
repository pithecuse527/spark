# TPC-H Q17 — SQL Query Tree (execution 17)

- **App**: `spark-6554076254d44fe891c6fb6e664c3673`
- **SQL execution id**: 17 — `count(1)` over the Q17-style 7-table join, duration 179.8 s
- **Source**: `get_sql_execution(details=true, plan_description=true)`. Tree shape taken from the
  authoritative `== Final Plan ==` (plan numbers `(1)`–`(61)` + `AdaptiveSparkPlan (97)`); metrics
  pulled from the runtime `nodes[]` array, matched by operator + output-row count.
- **SVG**: `q17_query_tree.svg` (self-contained, dark mode, top-down: AdaptiveSparkPlan at top,
  6 base-table scans at the bottom).

## Counts

- **Nodes**: 62 boxes (61 Final-Plan operators + the `AdaptiveSparkPlan` root wrapper).
- **Edges**: 61 (it is a tree; every node except the root has exactly one parent).
- **Scan leaves**: 6 — part, lineitem, supplier, partsupp, orders, nation.
  (Q17 uses 7 logical tables; `part` and the part-derived broadcast appear once. There is no
  `region`/`customer` scan in this execution — those are not part of this query's final plan.)

## Join operators (type + build / stream side)

| Plan # | Operator | Join keys | Build side | Stream side |
|-------:|----------|-----------|------------|-------------|
| (11) | BroadcastHashJoin Inner | `p_partkey = l_partkey` | **BuildLeft** = part (BroadcastQueryStage 0) | lineitem |
| (18) | BroadcastHashJoin Inner | `l_suppkey = s_suppkey` | **BuildRight** = supplier (BroadcastQueryStage 1) | part⋈lineitem |
| (31) | SortMergeJoin Inner | `l_suppkey,l_partkey = ps_suppkey,ps_partkey` | — | left=part⋈lineitem⋈supplier, right=partsupp |
| (44) | SortMergeJoin Inner | `l_orderkey = o_orderkey` | — | left=above, right=orders |
| (51) | BroadcastHashJoin Inner | `s_nationkey = n_nationkey` | **BuildRight** = nation (BroadcastQueryStage 4) | the SMJ result |

Two BroadcastHashJoins build on the small dimension side; the part-join uniquely uses **BuildLeft**
(part is broadcast and is the join's left input). The two large fact-side joins are SortMergeJoins.

## Query-stage (AQE) boundaries

Broadcast stages (small build sides):
- **BroadcastQueryStage 0** — part filter `Contains(p_name,'green')` → 2.18E6-row hashed relation (192.0 MiB)
- **BroadcastQueryStage 1** — supplier → 79.3 MiB (2.00E6 rows)
- **BroadcastQueryStage 4** — nation → 1024.2 KiB (25 rows)

Shuffle stages (hash-repartition for SMJ / aggregate), with the `ShuffleQueryStage (N)` arg = stage tag:
- **ShuffleQueryStage 5** (Exchange 20, hash `l_suppkey,l_partkey`) — 2.4 GiB, 6.53E7 rows — feeds SMJ (31)
- **ShuffleQueryStage 2** (Exchange 27, hash `ps_suppkey,ps_partkey`) — 3.6 GiB, 1.60E8 rows — partsupp side of SMJ (31)
- **ShuffleQueryStage 6** (Exchange 33, hash `l_orderkey`) — 1493.9 MiB, 6.53E7 rows — feeds SMJ (44)
- **ShuffleQueryStage 3** (Exchange 40, hash `o_orderkey`) — 6.7 GiB, 3.00E8 rows — orders side of SMJ (44)
- **ShuffleQueryStage 7** (Exchange 54, hash `nation,o_year`) — 289.8 KiB, 8.75E3 rows — after BHJ (51) + partial agg
- **ShuffleQueryStage 8** (Exchange 59, SinglePartition) — 16.0 B, 1 row — final count shuffle

`AQEShuffleRead` nodes after each ShuffleQueryStage show the coalesced partition counts
(50 / 67 / 67 / 67 / 1 / 1).

## Inferred / guessed

- **Stage-container boxes**: only the 6 scan-side / count stages are drawn as translucent containers
  (BroadcastQueryStage 0,1,4 and ShuffleQueryStage 2,3,8). The three large shuffle stages
  (5, 6, 7) span subtrees that interleave horizontally with their join siblings, so their bounding
  boxes would overlap and mislead — per spec, skipped rather than drawn wrong. Their boundary is
  still visible from the green `ShuffleQueryStage` node itself.
- **Table names on scans** are inferred from the `planDescription` detail sections
  (`Location: .../tpch-scale-200/<table>`) and confirmed by output-row counts
  (part 40M, lineitem 1.2B, supplier 2M, partsupp 160M, orders 300M, nation 25).
- **Build/stream side** is taken verbatim from the plan (`BuildLeft`/`BuildRight`); nothing guessed.
- **Node↔metric matching**: runtime `nodeId`s differ from plan numbers, so metrics were matched by
  operator type + distinctive output-row count (all unambiguous here).
