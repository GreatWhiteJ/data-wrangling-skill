---
name: data-wrangling
description: Build, size, debug and speed up large data transformations with DuckDB and Parquet, on one machine or as a batch job on a cluster. Covers choosing the engine (DuckDB vs Polars, pandas, R, Spark or Dask), sizing a job's memory from measurement, keeping a build in RAM instead of spilling to disk, slicing a table by person or firm into memory-sized groups, writing SQL that the engine runs well (temp tables, narrow-then-wide, pure equijoins, ASOF and semi joins, deterministic picks), writing and reading Parquet, resumable and promotable outputs, and proving a rewrite against the old output. Use this skill whenever the user is building, sizing, debugging or speeding up a large data transformation, a DuckDB, Parquet or SQL pipeline, a job that spills to disk or runs out of memory, a SLURM or other batch build, a panel or join over hundreds of millions of rows, or asks which engine or how much memory to use, even if they do not say "skill" or name DuckDB.
---

# Data wrangling

How to move and reshape large data: which engine, one node or many, how to size and run
a build, how to keep it in RAM, how to write the SQL, how to write the files, and how to
prove a rewrite. Every rule here was paid for by a named incident or a measured number.
Two real builds supply most of the numbers: a 389M-row position table (110 columns) that
needed about 2 TB single-pass and spilled on a 2 TB node, rebuilt sliced at 500G in
46 min; and a 252M-row user-year panel that spilled about 1 TB at 1500G, rebuilt at 500G
in 15 min.

## Read the file for the task at hand

Paths are relative to this skill's folder. Read only what the task needs.

| the user wants to | read |
| --- | --- |
| choose an engine, decide one node or a cluster, plan a batch job | this file, sections 1-3 |
| size memory, diagnose a spill or an OOM, pick a slice size | this file, sections 4-5, then `sizing.md` |
| write or fix the SQL of a build, read an EXPLAIN | this file, section 6, then `patterns.md` |
| write Parquet, stage inputs by bucket, promote an output | this file, section 7, then `patterns.md` |
| prove a rewrite gives the same output | this file, section 8, then `patterns.md` (compare script) |
| see the measured numbers behind any rule | `sizing.md` |

DuckDB doc citations are the 2.0 pages (`duckdb.org/docs/2.0/...`). The SQL assumes DuckDB
1.3 or later; confirm a setting exists in `duckdb_settings()` before relying on it.

## 1. Platform

| Work | Tool | Why |
|---|---|---|
| Scan, filter, join, window, aggregate, reshape, write | DuckDB SQL over Parquet | Streams sources, parallelises per row group, spills every blocking operator, one process, no cluster. Bind every table as a view over its parquet files in one in-memory connection. |
| Row-wise Python logic, list/string expression pipelines on a frame DuckDB already reduced | Polars (`conn.sql(...).pl()`) | Arrow handoff is zero-copy; Polars expressions beat Python UDFs. Not a scan engine. |
| Handoff to sklearn / statsmodels / an optimiser | pandas, on a frame that fits with room to spare | Only at the end. Never as the engine that reads parquet. |
| Econometrics | R (fixest), data via DuckDB in R | Copy every `dbGetQuery` column to a plain R vector before fixest: a 118M-row fit segfaulted at 37G, 60G and 180G straight from the query result, and finished in 1h04m at 27.8G on plain vectors. |
| Many independent heavy units | One batch job running a worker pool over the units | One submission, workers pull pending units, resume-safe on `file.exists`. Not one submission per unit: that reserves cores that sit idle after the quick units finish. |
| Spark / Dask | Not used | See section 2. |

Rules:
- DuckDB + Parquet is the default for everything that touches more than a few GB.
  Python only for the iterative-numeric job (ML, fuzzy matching, optimisation), and
  even then DuckDB does the filtering and joining and hands Python a small frame.
- Heavy work runs on the compute cluster, not on the workstation someone is using.
  Locally: transfers, version control, small reads.
- A local copy of a table goes stale invisibly. One copy sat four months behind with the
  same row count (365 rows apart out of 389M) and a treated-flag count 36% different.
  Compare a FLAG count against the canonical copy before trusting a local number.

## 2. One node or many

One big node, always, until the slice pattern (section 5) cannot fit the work.

- The largest build to date, 389M rows x 110 columns, needed about 2 TB single-pass and
  spilled on a 2 TB node; sliced by person it ran at 500G in 46 min with no spill. A
  1.69B-row LEAD window elsewhere in the same pipeline (2000G, 8-11 h) is the same shape
  and the fix is the same: slice by firm bucket, not a cluster.
- The engine's internal parallelism has always beaten hand-rolled external
  parallelisation here, by a wide margin. The only reason to split work at all is to keep
  it in RAM instead of spilling to disk. So: never a pool of DuckDB processes over one
  table. Slice only to stay in RAM, run the slices sequentially, each slice gets all the
  threads.
- Distributed engines earn their shuffle only when a single node's RAM plus the slice
  pattern cannot hold the working set, or when units are independent AND each unit is
  itself heavy. Neither has happened here. A shuffle across nodes also runs over the
  parallel network filesystem, which is what falls over under many processes opening many
  files.
- One task, one node, `cpus-per-task` = DuckDB threads. DuckDB does not span nodes.
- Many simultaneous R starts die loading packages from a parallel network filesystem
  (19 jobs started at once, 11 dead in under two minutes). Past about 6 R jobs, stagger
  the starts by 40 s (`sbatch --begin=now+$((k*40))`).

## 3. Thinking about a batch job

SLURM is the example scheduler because the diagnostics (`sacct`, MaxRSS, MaxDiskWrite,
TotalCPU) are what the numbers below come from. On another scheduler, find the
equivalents: peak resident memory, bytes written to disk, CPU time over elapsed time.

1. Measure before you size. A sample run, or the last run of this exact shape:
   `sacct -j <id> -o JobID,MaxRSS,MaxDiskWrite,Elapsed,TotalCPU,NCPUS --units=G`.
   Request 1.2x the MaxRSS of a run that did not spill. Not 2x: on a small large-memory
   partition an oversized reservation blocks a node whether or not the job touches the
   memory, and a flat billing formula makes the waste invisible on the invoice. An
   unmeasured shape gets one proof run first, on a sample or on a short-queue debug
   partition, never a round number because the form allows it.
2. Size the work unit to the allocation, not the allocation to the work. Speed within a
   memory constraint is the thing being optimised. Pick the memory request by what starts
   promptly and bills sanely, then size the slice group from it (section 5). More memory
   means bigger groups and fewer passes, not a faster pass.
3. Proof on a sample that is exact. A sample of people (buckets) run through both old and
   new SQL proves a rewrite in minutes (section 8). A `LIMIT` is not exact for anything
   with a window or a lookup built from the rows.
4. Chain with `--dependency=afterok:` and put `set -euo pipefail` right after the
   `#SBATCH` block. A wrapper that ends in `echo "done"` reports exit 0 after a Python
   failure and releases the chain onto stale input (three 18 h builds were released onto a
   months-old table this way; caught only because a "build" finished in 9 seconds).
5. Resumable outputs. One file per unit, written to `.tmp` and renamed, skipped if present.
   Key the resume on content (buckets covered, date range), not on a chunk name: a build
   that matched on `chunk{i}` found every name present and exited 0 having built nothing
   when the end date was raised.
6. A spill directory per job, named with the job id, with
   `trap 'rm -rf "$DUCKDB_TEMP_DIR"' EXIT`. Delete only the spill directories from a
   shared scratch area; a 56 KB input parked in the same area was deleted as "spill" and
   killed a launch.
7. `python -u`, per-step timing with RSS in the log, mail on END and FAIL, walltime from a
   measured run plus margin. A running job cannot usually be extended.
8. Wait with ONE held connection that loops remotely and prints the accounting line when
   the job ends, backgrounded, not a poller. A controller that must keep a multi-hour
   pipeline moving is a detached OS process, never an agent's background shell: a
   memory-pressure reaper killed two such watchers and a pull sat idle for 8 h.
9. Deploy code by version control (commit, push, pull on the cluster). Move data with a
   bulk transfer tool. No scp of either.

## 4. Memory model

What `memory_limit` is:
- It caps DuckDB's buffer pool and intermediates, nothing else. The process runs above it:
  one build, 663G limit, 706G MaxRSS, +6.6%.
- Set `memory_limit = int(0.93 x the job's memory allocation)`. 0.93 lands the real peak
  near 99% of the allocation. 0.99 crosses the cgroup and the job dies. 0.85 leaves 15%
  idle and spills: the same build at 780G with an 85% cap spilled 186G and ran on 4 of 32
  cores for an hour.
- Never size from the machine's total RAM (`psutil.virtual_memory()`): that is the node,
  not the job, and gets the job killed on a shared node.
- DuckDB's own default is 80% of physical RAM (DuckDB blog, "Memory Management in
  DuckDB", 2024). Set it explicitly every time.

What spills and what cannot:
- The blocking operators (GROUP BY, JOIN, ORDER BY, window) all support larger-than-memory
  execution and spill to `temp_directory` adaptively ("How to Tune Workloads",
  Larger-than-Memory Workloads).
- `string_agg()`, `list()` and therefore `PIVOT` cannot offload; sorting-based aggregates
  can OOM; and "if multiple blocking operators appear in the same query, DuckDB may still
  throw an out-of-memory exception" (same page, Limitations). A five-way join over a window
  over a sort in one statement is exactly that query. Stage it (section 6).
- `SET preserve_insertion_order = false` for every build: lets the engine reorder results
  without ORDER BY and cuts memory on large import and export (same page).

Threads:
- 1-4 GB per thread; join-heavy work 3-4 GB per thread ("Environment", Memory). The
  builds here carry 10-15 GB per thread; the intermediates bind, not the threads.
- Parallelism is per row group: k threads need at least k x 122,880 rows in the scan (same
  page, Parallelism). A small lookup step on 10 cores is normal; a big step on 10 of 32
  cores is a spill.

Reading a run (full table with examples in `sizing.md`):

| Signal | Healthy | Spilling |
|---|---|---|
| MaxDiskWrite vs output size | about equal | far above (1038G written for 12G of output) |
| TotalCPU / Elapsed vs NCPUS | 20-30 of 32 on the big steps | 10-14 of 32, or 4 of 32 |
| spill directory | empty | growing |
| MaxRSS vs memory_limit | below | sitting at the limit |

- The RSS logged after each statement understates the in-flight peak badly: 84G logged,
  427G MaxRSS, same job. Size from the scheduler's accounting, never from the build log.
- A run that spilled reports MaxRSS at or under its limit (1292G under a 1395G limit while
  writing 1 TB of spill). That number is not what the build wanted; the only valid MaxRSS
  for sizing comes from a run that did not spill.
- `DROP TABLE` of a spilled temp on a parallel network filesystem takes about 90 s per
  30 GB; of a resident temp it is instant. Removing 1 TB of spill files at job exit took
  30 min. Spill costs you twice.

## 5. The slice pattern

When: a single-pass build that would need more than the node you can get promptly, or
that spilled. Not for speed: per-bucket throughput in a group of 16 equalled a
single-bucket run, so a bigger slice only cuts the number of passes.

Why by person: every person-level step (running counts, experience sums, LEAD/LAG,
education ASOF, patent lags, one-position-per-year picks, treatment clocks) is
`PARTITION BY user_id`, and every firm- or occupation-level value is a lookup keyed by
firm, firm pair, occupation pair or firm-year. So a group of people at a time yields
exactly those people's rows of the full result, and a reference run on 4 of 64 buckets is
an exact proof. Firm-keyed panels slice by a firm bucket on the same argument.

Shape (`sql_steps.sql` with `-- @@ once`, `-- @@ stage`, `-- @@ bucket` sections; a
`build.py` runs them, skeleton in `patterns.md`):

1. `once`: the lookups over all rows (firm pairs, occupation pairs, firm-year context,
   patents per user-year). Small, keyed, held for the whole job. Measure their resident
   size: that is `LOOKUP_GB`.
2. `stage`: each large person-keyed input written ONCE as hive-partitioned parquet by
   `user_bucket` (bucket expression in `patterns.md`), `COPY ... PARTITION_BY (user_bucket)`.
   Marker file `_STAGED`; `--reuse-stage` skips it on reruns. Use a fresh stage directory
   when an upstream table changed.
3. `bucket`: for each GROUP of buckets, one DuckDB statement sequence: read the group's
   partitions as an explicit glob list with `hive_partitioning = false` (an empty bucket
   gets a zero-row file first), run the person pipeline, write
   `<table>_bNN_MM.parquet` to `.tmp`, rename. Existing files are skipped, so a rerun
   resumes.
4. Output goes to a STAGED sibling directory (`<table>_staged/`), never the live
   directory, because readers glob the live directory and a half-written table is read as
   the table. `--promote` checks bucket coverage and row count, renames live to `_prev`,
   staged to live.

Group size, from the memory limit:

```
memory_limit_gb = int(0.93 * mem_gb)
g = int((memory_limit_gb - LOOKUP_GB) / (BUCKET_GB * SAFETY)); clamp to 1..N
g = ceil(N / ceil(N / g))          # even groups, so the last one is not a runt
```

`BUCKET_GB` is the in-flight peak per bucket, from accounting:
`(MaxRSS of a group run - LOOKUP_GB) / buckets in the group`. `SAFETY` = 1.5 when
`BUCKET_GB` comes from single-bucket runs (memory is not quite linear in the slice), 1.0
once it comes from a full-size group.

| Build | mem | LOOKUP_GB | BUCKET_GB x SAFETY | groups | time | peak | spill |
|---|---|---|---|---|---|---|---|
| position table (389M rows) | 500G | 60 | 16 x 1.5 | 4 x 16 | 46 min | 319G | none |
| same, single pass | 2 TB | | | 1 | killed at 4 h | | 375G |
| user-year panel (252M rows) | 500G | 40 | 13 x 1.0 | 2 x 32 | 15 min | 427G | none |
| same, single pass | 1500G | | | 1 | 1h18m | 1292G | ~1 TB |

The anatomy with timings per section is in `sizing.md`. The first bucketed build in this
lineage, a 227M-row worker panel, went from about 1.1 TB single pass to 21G MaxRSS on
100G / 16 cores, about 1.5 h for the whole chain.

## 6. SQL craft

Structure:
- Temp tables, not CTEs. `CREATE OR REPLACE TEMP TABLE` per step, even single-use. A CTE
  hides cost, can be re-evaluated, cannot be inspected, and folds several blocking
  operators into one statement (the OOM case in section 4). DuckDB's own join guide names
  staged temp tables as the way to fix a join order ("Join Operations", Create Temporary
  Tables). `DROP` each temp after its last use.
- Project and filter before the first big step. Carry the columns the step needs, not the
  table. The 110-column year explosion carried everything through five materialised steps
  and spilled 1 TB; the same panel on a 16-column key table joined the wide columns back
  once, by position id, inside the output COPY, and wrote only the output.
- Narrow the build side. SEMI JOIN the large table down to the keys you hold before the
  hash join builds (a 14 GB table became 2 GB this way). A 2-hop network term computed
  only for first hops out of a used origin and second hops into a used destination, with
  SEMI JOINs, replaced an all-pairs product that materialised about 11B rows.
- Per-user aggregates are a GROUP BY, not a window over the wide table. Treatment clocks:
  `MIN(CASE WHEN flag THEN period END) ... GROUP BY user_id`, then one join back. The old
  form ran six `MIN(...) OVER (PARTITION BY user_id)` over 120 columns.
- Running totals are one window with `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`,
  not a self-join on `seq <= p.seq` (quadratic per person).
- One materialisation per stage. An old build created two of its temps twice each and
  sorted outputs nothing read.
- A UNION ALL of hundreds of subqueries serialises: a 256-way UNION ran 10 h on one core;
  the same work as per-subset queries took 4.5 min.
- No `ORDER BY` inside a build except the one the output needs.

Joins:
- `ON` is a pure equijoin. A constant or range predicate in a `LEFT JOIN ... ON` drops the
  planner to `BLOCKWISE_NL_JOIN`: one `departed = 1` in an ON turned a 24-second join into
  a 98-minute non-finish on 227M x 5.5M rows. Gate with `CASE WHEN` in the SELECT.
- `EXPLAIN` the sample first and grep the operator. `HASH_JOIN` good; `BLOCKWISE_NL_JOIN`
  / `IE_JOIN` means the equijoin is lost; a cardinality that explodes to billions is a bad
  join order ("How to Tune Workloads", Profiling). The optimiser enumerates exactly up to
  `approximate_join_order_threshold` = 12 tables and goes greedy beyond; Parquet carries no
  HyperLogLog statistics, so join orders over parquet views are worse than over loaded
  tables ("File Formats", Handling Parquet Files). Staging temp tables fixes the order by
  construction.
- Key uniqueness before any LEFT JOIN: `SELECT count(*) - count(DISTINCT (k1, k2)) FROM
  right_side` must be 0, and the joined count must equal the left count. Know the real
  cardinality of both sides; "small" is a number, not a feeling.
- Align key types. Ids stored as DOUBLE in one source and VARCHAR in another are integers
  underneath: join as `TRY_CAST(... AS BIGINT)` on both sides. DuckDB 1.4.0's pushed-down
  string filter returned 0 rows in 0 s against files written by fastparquet 2024.2.0
  although the value was present; a BIGINT join or
  `SET disabled_optimizers = 'filter_pushdown'` returned them. Any instant "0 matches" is
  suspect.
- Semi joins: `SEMI JOIN` or `WHERE k IN (SELECT k FROM ...)`. Never a multi-column
  correlated `EXISTS` against a big scan: 90 min at zero CPU; the `IN` form took 8 min.
  SEMI JOIN is the `IN` operator; ANTI JOIN is `NOT IN` that ignores right-side NULLs
  ("FROM and JOIN Clauses", Semi and Anti Joins). Zero CPU for a minute means a stalled
  plan: kill it.
- ASOF JOIN for "latest row at or before a date" and "first row after": one inequality on
  the ordering column, every other condition an equality, at most one right row per left
  row (same page, As-Of Joins). Replaces the correlated subquery and the window-then-filter.

Determinism:
- Every pick is total-ordered. `QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY
  <rule cols>, seq DESC, id) = 1`, or `DISTINCT ON (...) ORDER BY ...` with the tie-break
  written out. The engine's arbitrary choice on ties (same-day degrees: 2.8% of positions;
  concurrent positions equal on the rule columns: 28 user-years in a sample) makes a
  correct rewrite look wrong, and differs between runs of the same SQL.
- Document the accepted diff class in the SQL header and the compare script (section 8).
- `DECIMAL(18,4)` sums round `x.x5` differently from `DOUBLE` sums. Pick one for a column
  and keep it through the chain; expect about 200 rows 0.1 apart when a rewrite changes it.
- Pin `CURRENT_DATE` to a literal in both SQL files before a proof
  (`sed "s/CURRENT_DATE/DATE '$D'/g"`).

Semantics traps:
- `NOT flag` on a nullable column is NULL and `SUM` skips it. Validation queries use
  `flag IS NOT TRUE` or `COALESCE(flag, FALSE)`. A check that returns exactly 0 on a big
  table is tested before it is believed (a 16 h chain was nearly cancelled over a check
  that could only return 0). Emit `COALESCE(..., FALSE)` for boolean flags at source.
- Explicit SELECT lists drop columns. After adding columns upstream, diff every downstream
  step's output list; a final `p.*` carries only what the last explicit list kept
  (8 columns vanished from a 1.5 h build this way).
- Period windows are half-open: `enddate >= period_start AND enddate < period_end`.
- Never read forward in time in a panel measure: a forward window on a right-hand-side
  variable is lookahead, not a fidelity trade-off. Coverage of any open-web source grows
  by construction; a rising count is composition until a balanced panel says otherwise.
- `file_search_path` replaces the working directory for relative globs; set it to the
  project root and keep read paths root-relative.
- A view created in one in-memory connection dies with its process; bind the project's
  views in a shared helper so the next stage sees the same names.

## 7. I/O and filesystems

Writing:
- The writer is DuckDB `COPY ... TO (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE
  200000)`, directly or through a small shared helper that adds `FILE_SIZE_BYTES '384MB'`
  and `FILENAME_PATTERN '<prefix>_{i}'`. Never a writer that pulls the result into an Arrow
  table and shards it in Python: ours dropped about 15% of rows silently (193.3M of
  226.95M) at 1.38 TB RSS; COPY was 7-29x faster at about 3x lower peak.
- Row groups of 100K-1M rows; at least as many row groups across the files as threads that
  will read them; files of 100 MB-10 GB ("File Formats", Parquet Row Group Sizes and File
  Sizes). 200,000 is in range. `ROW_GROUP_SIZE_BYTES` only applies with
  `preserve_insertion_order = false` ("COPY Statement", Parquet Options).
- `PARTITION_BY` cannot take an expression: compute the bucket column in the SELECT. One
  file per thread per partition directory; `partitioned_write_max_open_files` defaults to
  100, so 64 buckets fit; aim for at least 100 MB per partition ("Partitioned Writes").
- Without `FILENAME_PATTERN` every chunk is `data_0.parquet` and `OVERWRITE_OR_IGNORE`
  keeps only the last chunk: a 20-chunk job silently kept one.
- No ART indexes; zone maps are automatic and an imperfect pre-order by the filter column
  still prunes ("Indexing"). Sort the output by its key only when readers filter on it.

Reading:
- Per-bucket reads pass an explicit list of globs with `hive_partitioning = false`, so the
  schema matches the reference and no partition column appears. Firm-keyed panels read
  `data/**/*.parquet` with auto-detection, which appends the bucket column and prunes on it
  ("Hive Partitioning").
- Parquet views carry no HLL statistics; for a join-heavy query over the same big tables
  many times, loading them into a DuckDB file is 1.1-4.1x faster (2.8x median, TPC-H SF30;
  "File Formats"). Builds run once, so they stay on parquet.

Parallel network filesystems (BeeGFS in our case):
- Large sequential parquet reads from one node are fine. Many processes opening many small
  files are not (the R package-load failures, section 2).
- Deleting about 1 TB of spill took 30 min; `DROP` of a spilled temp about 90 s per 30 GB.
  Node-local scratch is about 30x faster for spill but small (0.75-6 TB on our nodes). The
  plan is not to spill at all.
- DuckDB's guidance: network filesystems are fine read-only, not for a read-write DuckDB
  database file ("Environment", Network-Attached Disks). In-memory connections over parquet
  are the read-only case.

Promotion and copies:
- `.tmp` then rename, per group file. Staged sibling, then `--promote`. Keep finished group
  files so a rerun resumes. A killed single-pass build that cleared its live output before
  writing left a panel in pieces twice.
- Clear the destination before copying a table between machines. A copy laid over an older
  build leaves the old shards and `**/*.parquet` reads both (duplicate edges in a network
  table until noticed; a transfer in "skip existing" mode would have doubled a panel whose
  shard names changed from `_0000..0038` to `_0..48`).

## 8. Proving a rewrite

1. Freeze the reference: copy the old SQL to `sql_steps_ref_<short hash>.sql` and name it
   in the new SQL's header.
2. Pick the sample: 4 of 64 user buckets (for example `0,17,42,63`). Run the new build on
   those buckets (`--buckets ... --out-dir`), and the reference on the same people
   (`--ref-sample ...`, which replaces the input view with a bucket-filtered one). Pin
   `CURRENT_DATE` in both.
3. Compare with the script in `patterns.md`: schema (names, types, order), row counts, key
   uniqueness on both sides, `EXCEPT ALL` both ways with DOUBLEs rounded to 6 dp, then
   per-column mismatch counts on the key join with 5 examples each, DOUBLEs equal within
   1e-9 relative. Exit 1 on anything not in the accepted class.
4. Accepted classes are written down, in the SQL header and the diff wrapper, with their
   measured size: same-day degree ties (2.8% of positions), DECIMAL vs DOUBLE rounding
   (about 200 rows), engine-arbitrary concurrent-position ties (28 user-years). Anything
   else is a bug.
5. Chain the real build behind the proof with `afterok`, so a failed proof stops it.
6. For SQL small enough to run on stubs, a synthetic-fixture test (old SQL from version
   control vs new, run through the same statement parser, full EXCEPT both ways) does the
   same in seconds.

A proof costs minutes (position table: 24.3M rows, 35 min at 351G including the reference;
panel: 4 buckets against the live table). The single-pass reference could not finish on
the full table at all; the proof is the only evidence there is.

## 9. Getting data in

- Never page a database with `LIMIT ... OFFSET`: scan order is not stable, so pages overlap
  and leave holes. One such pull produced a 56% sample with 44% duplicate rows and went
  unnoticed for a year. Use a single server-side cursor, or the provider's bulk export.
- Published rate limits are the rule, not what succeeds. A burst that worked measures what
  you got away with: one benchmark at 936 requests per second succeeded and the whole
  address was blocked minutes later; two workers at the published 10 per second with
  concurrency were blocked within 2.5 minutes each; the provider's bulk path from the
  right region sustained 164 per second with no blocks. Follow the pattern they publish
  (rate, sleep between calls, one thread), not just the number.
- Compute nodes often have no outbound network; run pulls on a login node or off-cluster.

## 10. Checklist

Before submitting a build:
- [ ] Engine is DuckDB over parquet; Python/R only receive a reduced frame.
- [ ] `memory_limit = 0.93 x` the job's memory, `preserve_insertion_order = false`,
      `temp_directory` per job with a trap, threads = cores.
- [ ] Memory request = 1.2x the MaxRSS of a non-spilling run of this shape, or a proof
      run exists.
- [ ] Single pass would exceed that: sliced by person (or firm), `once` / `stage` /
      `bucket`, group sized from the formula.
- [ ] Every step a temp table, projected and filtered first; wide columns joined back once
      at the end.
- [ ] Every `ON` a pure equijoin; `EXPLAIN` on the sample shows `HASH_JOIN`; right-side
      keys unique; ids joined as BIGINT.
- [ ] Every pick total-ordered; `CURRENT_DATE` pinned for proofs; accepted diff classes
      written down.
- [ ] Output COPY: SNAPPY, ROW_GROUP_SIZE 200000, `.tmp` then rename, staged sibling,
      `--promote`.
- [ ] Wrapper: `set -euo pipefail`, `python -u`, mail on END and FAIL, `afterok` chain,
      resume keyed on content.
- [ ] Waiter: one held connection, backgrounded; no polling.

After it runs:
- [ ] Accounting: MaxDiskWrite about the output size; TotalCPU/Elapsed near NCPUS on big
      steps; spill directory empty.
- [ ] Row count equals distinct source key; a flag count matches expectation; new columns
      are in the output schema.
- [ ] MaxRSS recorded in the build's docstring for the next sizing.
