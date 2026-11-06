# Sizing: the measured numbers

Companion to `SKILL.md`. Every number here is from one real build or one dated incident
(all in 2026, on DuckDB 1.4 and a SLURM cluster with 2 TB large-memory nodes). Use them to
size the next build of the same shape, and replace them when a newer run measures
differently.

## The memory rules

| Rule | Number | Evidence |
|---|---|---|
| DuckDB `memory_limit` as a share of the job's memory | 0.93 | Process RSS runs about 6.6% over the limit (one build: 663G limit, 706G MaxRSS). 0.93 puts the peak near 99% of the allocation. |
| Too low a share | 0.85 spills | The same build at 780G with an 85% cap: 186G spilled, 4 of 32 cores busy for an hour, killed. |
| Too high a share | 0.99 OOM-kills | Crosses the cgroup limit; the job dies instead of finishing. |
| Memory request for an exact rerun | 1.2 x MaxRSS | MaxRSS must come from a run that did not spill. Not 2x: an oversized reservation blocks a large-memory node whether or not the memory is touched. |
| Memory request for an unmeasured shape | one proof run first | Sample buckets or a short-queue debug partition; never a round number because the form allows it. |
| Build-log RSS vs accounting MaxRSS | log understates | 84G logged after the heaviest statement vs 427G MaxRSS, same job. |
| MaxRSS of a run that spilled | understates | 1292G MaxRSS under a 1395G limit while writing 1038G of spill. |
| DuckDB's own default limit | 80% of RAM | DuckDB blog, "Memory Management in DuckDB", 2024. Always set it explicitly. |
| Memory per thread (DuckDB guidance) | 1-4 GB; join-heavy 3-4 GB | DuckDB docs 2.0, Performance Guide, Environment. These builds carry 10-15 GB per thread; the intermediates bind, not the threads. |
| Rows per thread for full parallelism | 122,880 x threads | Default row group size; a scan of fewer rows than that per thread runs on fewer threads. Performance Guide, How to Tune Workloads. |

## Spill detectors

| Signal | Clean run | Spilling run |
|---|---|---|
| `MaxDiskWrite` vs output size | 12G written for a 12G panel | 1038G written for a 12G panel |
| `TotalCPU / Elapsed` vs `NCPUS` | 20-30 of 32 on big steps | 10-14 of 32; 4 of 32 at worst |
| Per-step "CPU=... cores" in the build log | 20-30 | drops to about 10 on the spilling step |
| Spill directory listing | empty | growing while the job runs |
| MaxRSS vs memory_limit | below | pinned at the limit |
| Job tail after the SQL finished | seconds | 30 min `rm -rf` of the spill directory on a parallel network filesystem |

On SLURM, `sacct -j <id> -o JobID,MaxRSS,MaxDiskWrite,Elapsed,TotalCPU,NCPUS --units=G`
gives all of it in one line. On another scheduler, find peak resident memory, bytes
written, and CPU time over elapsed.

## Group-size formula

```
memory_limit_gb = int(0.93 * mem_gb)
g_raw = int((memory_limit_gb - LOOKUP_GB) / (BUCKET_GB * SAFETY))
g     = clamp(g_raw, 1, N)
g     = ceil(N / ceil(N / g))      # even groups
```

- `N` = 64 buckets, so one bucket is 1/64 of the people (bucket expression in
  `patterns.md`).
- `LOOKUP_GB` = resident size of everything held across groups: the `once` tables plus
  whatever the group query keeps alongside them. Measure as the RSS after `once` (that
  figure is a true resident number, not an in-flight peak) or from a `once`-only job.
- `BUCKET_GB` = in-flight peak per bucket = `(accounting MaxRSS of a group run - LOOKUP_GB)
  / buckets in that group`.
- `SAFETY` = 1.5 when `BUCKET_GB` was derived from single-bucket runs (memory is not quite
  linear in the slice); 1.0 once it comes from a full-size group's MaxRSS.

Check against the two builds: position table at a 465G limit, (465 - 60) / (16 x 1.5)
= 16.9, so 16 and 4 groups; panel at a 465G limit, (465 - 40) / 13 = 32.7, so 32 and 2
groups. Both matched the measured peaks (319G and 427G) within the headroom.

More memory means bigger groups and fewer passes. It does not make a pass faster:
per-bucket throughput at 16 buckets per group equalled the single-bucket proof (0.5 min per
6M-row bucket at 32 threads).

## Anatomy of a sliced build: the position table

389M positions x 110 columns, one row per employment position enriched with role, firm,
experience, education, patent, proximity, labour-flow and demographic fields.

Single pass (2 TB node, 4 h limit): the final 20-way join held the position table two or
three times over, about 2 TB wanted, 375G spilled, sat 2 h in that join, killed at the
limit. Never finished on the full table.

Sliced (32 cores, 500G):

| Section | What | Time | Memory |
|---|---|---|---|
| `once` | top-N role list, role / firm / firm-year lookups, firm and occupation move pairs from all positions, cosine proximities, entropies, a 2-hop labour-flow term over SEMI-JOINed edges, concentration indexes | 8 min | 57G peak, 30G resident |
| `stage` | positions, education, patent filings, user demographics, a third-party demographic table, each `COPY ... PARTITION_BY (user_bucket)` | 6 min | |
| `bucket` x 4 groups of 16 | per group: positions with LEAD(firm), base join, entity counts (running sum of first occurrences), experience years (DECIMAL running sums), role flags, DISTINCT ON + ASOF education lookups, patent lags, demographics, final 20-way join in the COPY | about 8 min per group (0.5 min per bucket) | 16G per bucket in flight |
| total | | 46 min | 319G MaxRSS, no spill |

Proof: reference SQL on 4 buckets (24.3M rows) vs the sliced build of the same buckets,
35 min, 351G. Differences: education columns on 2.8% of positions (same-day degree ties;
the build takes the higher degree, the reference let the engine choose) and about 200
experience-year values 0.1 apart (DECIMAL sums round x.x5 up, the DOUBLE sums fell either
side). Nothing else.

Constants in its `build.py`: `N_BUCKETS 64`, `LOOKUP_GB 60`, `BUCKET_GB 16`, `SAFETY 1.5`.

## Anatomy of a sliced build: the user-year panel

252M user-years, a balanced 2000-2025 grid for every person who ever held a qualifying
role, one position per user-year plus patents, firm context and treatment clocks.

Single pass (1500G): exploded every ~120-column position row into one row per calendar
year it spans, carried all of it through five materialised steps (position-years, the pick,
the balanced grid join, the firm-context join, the final with six user-level window MINs).
1h18m, MaxRSS 1292G, MaxDiskWrite 1038G against 12G of output, 10-14 of 32 cores on the
big steps, then 30 min removing the spill directory.

Sliced (32 cores, 500G):

| Section | What | Time | Memory |
|---|---|---|---|
| `once` | periods, patents per user-year, firm-year context (172M rows) | about 1 min | |
| `stage` | the qualifying people's positions, wide, by user bucket | 7 min (reused on the measured run) | |
| `bucket` x 2 groups of 32 | `pos` wide (kept for the join-back), `pos_key` narrow (16 columns), year explosion of `pos_key` only, `QUALIFY ROW_NUMBER` pick with a full tie-break, clocks as a per-user `GROUP BY MIN`, grid, final join back to `pos` by position id inside the COPY | 6 min per group | 13G per bucket in flight |
| total | | 15 min | 427G MaxRSS, disk written = output |

Proof: 4 buckets vs the live single-pass table. Identical except 28 user-years where two
concurrent positions tied on in-year departure and pay; the build takes the most recent,
the old build let the engine pick.

Constants in its `build.py`: `N_BUCKETS 64`, `LOOKUP_GB 40`, `BUCKET_GB 13`, `SAFETY 1.0`.

## The first bucketed build: a 227M-row worker panel

Materialised five times in the single pass (about 940 GB to 1.1 TB at peak). On a 480G
node it spilled and timed out at 3 h and at 10 h, and because it cleared the live output
before writing, each timeout left the panel in pieces. Bucketed: 21G MaxRSS, the whole
chain (three builds of 10G, 21G and 81G) at 100G / 16 cores in about 1.5-2 h. One bucket
per query there, since the slice was sized to fit a standard node; the group formula came
later.

## Other measured costs

| Operation | Measured | Note |
|---|---|---|
| Constant predicate in a `LEFT JOIN ... ON` | 24 s became a 98 min non-finish (227M x 5.5M) | `BLOCKWISE_NL_JOIN`; gate in the SELECT instead |
| Correlated 3-column `EXISTS` on a big parquet scan | 90 min at zero CPU | `IN (subquery)` form: 8 min |
| 256-way `UNION ALL` of per-subset queries | 10 h on one core | per-subset queries: 4.5 min |
| Arrow-sharding Python writer, 227M x 98 columns | 1.38 TB RSS, dropped about 15% of rows | DuckDB COPY: 7-29x faster, about 3x lower peak, all rows |
| `DROP TABLE` of a spilled temp on a parallel network filesystem | about 90 s per 30 GB | instant when resident; node-local scratch about 30x faster but 0.75-6 TB |
| `rm -rf` of about 1 TB spill on the same filesystem | 30 min | counted inside the job's walltime |
| fixest on a `dbGetQuery` frame, 118M rows | segfault at 37G, 60G, 180G | plain R vectors: 1h04m, 27.8G |
| 19 R jobs started at once | 11 died in under 2 min loading packages | stagger `--begin=now+$((k*40))` |
| 1.69B-row LEAD window, firm-month panels | 2000G, 8-11 h | the next slicing candidate (by firm bucket) |
| Converting a 1.7B-row SAS table to parquet with a parallel page reader | 34 min | the whole table, validated against a second reader |
| Parquet queries vs loaded DuckDB tables, TPC-H SF30 | 1.1-4.1x slower on parquet, 2.8x median | DuckDB docs 2.0, Performance Guide, File Formats; relevant only for repeated join-heavy queries |
