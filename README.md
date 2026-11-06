# Data wrangling skill

A file-based skill for building, sizing, debugging and speeding up large data
transformations with DuckDB and Parquet, on one machine or as a batch job on a cluster.
It covers:

- **Choosing the engine**: when DuckDB over Parquet is the answer (almost always), when
  Polars, pandas or R get a reduced frame, and when a distributed engine is actually
  warranted (so far, never).
- **One node or many**: why a single big node with the work sliced to fit RAM beats a
  worker pool or a cluster shuffle, and how to think about a batch job (size from
  measurement, proof on an exact sample, chain, resume, one waiter).
- **The memory model**: what `memory_limit` covers, why 0.93 of the allocation, why the
  logged RSS understates the peak, how to recognise a spill from the scheduler's
  accounting.
- **The slice pattern**: lookups once, inputs staged by bucket, buckets processed in
  memory-sized groups, one query per group, staged output promoted at the end, with the
  group-size formula.
- **SQL craft**: temp tables not CTEs, narrow before wide, pure equijoins, key
  uniqueness, ASOF and semi joins, deterministic picks, DECIMAL vs DOUBLE, NULL logic,
  reading an EXPLAIN.
- **I/O**: the Parquet writer options, row groups and file sizes, hive partitioning,
  behaviour on a parallel network filesystem, tmp-and-rename, staged-and-promote.
- **Proving a rewrite**: frozen reference SQL, an exact sample, a compare script, and
  written-down accepted diff classes.

## The evidence it rests on

Two real builds supply most of the numbers, both run in 2026 on DuckDB 1.3.2 on a SLURM
cluster:

- A **389M-row position table** with 110 columns. The single-pass SQL needed about 2 TB,
  spilled 375G on a 2 TB node, sat two hours in its final join and was killed at the
  4-hour limit. Rebuilt sliced by person (64 buckets, 4 groups of 16) it ran at 500G in
  46 minutes with a 319G peak and no spill. Proof against the old SQL on 4 buckets:
  identical except same-day degree ties and x.x5 rounding.
- A **252M-row user-year panel**. The single-pass SQL exploded every 120-column position
  row into one row per year and carried all of it through five materialised steps: at
  1500G it ran 1h18m, reported 1292G MaxRSS and wrote about 1 TB of spill for 12G of
  output. Rebuilt on a narrow key table with the wide columns joined back once, sliced
  into 2 groups of 32 buckets, it ran at 500G in 15 minutes with a 427G peak and wrote
  only the output. Proof: identical except 28 user-years of engine-arbitrary ties.

The lessons: the engine's own parallelism is the fast path and slicing exists only to stay
in RAM; the work unit is sized to the allocation, not the other way round; the RSS a build
logs is not the peak and a spilled run's MaxRSS is not what it wanted; every pick must be
total-ordered or a correct rewrite looks wrong; narrow tables through the blocking
operators and join the wide columns back once. Around them sit a dozen smaller incidents,
each with its number: a constant in a `LEFT JOIN ... ON` that turned a 24-second join into
a 98-minute non-finish, a correlated `EXISTS` that sat 90 minutes at zero CPU, a Python
Parquet writer that silently dropped 15% of rows, a trailing `echo` that released three
18-hour builds onto stale input.

## Installing it as a Claude skill

`SKILL.md` is the entry point Claude reads: its frontmatter (`name`, `description`)
decides when the skill is used, and its body routes to the other files. To install:

- **Claude Code, for all your projects**: put this folder at
  `~/.claude/skills/data-wrangling/` (for example
  `git clone https://github.com/GreatWhiteJ/data-wrangling-skill ~/.claude/skills/data-wrangling`).
- **Claude Code, for one project**: put it at `<project>/.claude/skills/data-wrangling/`
  and commit it with the project.
- **Claude apps (claude.ai, desktop)**: zip this folder (with the folder itself at the top
  of the zip) and upload it under Settings > Capabilities > Skills.
- **Other agents**: point the agent at `SKILL.md`; it links to the two companion files.

Claude then uses it whenever a conversation is about building, sizing, debugging or
speeding up a large data transformation; you can also ask for it by name. Keep the folder
name `data-wrangling`, matching `name:` in `SKILL.md`.

The skill is plain Markdown. The SQL patterns assume DuckDB 1.3 or later (ASOF JOIN,
QUALIFY, DISTINCT ON, `PARTITION_BY` writes, `preserve_insertion_order`); the DuckDB
documentation it cites is the 2.0 pages. The Python in `patterns.md` needs Python 3 and
the `duckdb` package; `psutil` for the memory helper.

## When to use this skill

Use it when a transformation touches more than a few GB, when a job spilled or ran out of
memory, when a join or window runs over hundreds of millions of rows, when a build must be
proved identical to the one it replaces, or when someone asks which engine or how much
memory to use. For a frame that fits in a notebook with room to spare it is overkill.

## Files

| file | what |
| --- | --- |
| `SKILL.md` | the skill: platform, one node or many, batch-job thinking, memory model, the slice pattern, SQL craft, I/O, proving a rewrite, checklist |
| `sizing.md` | the measured numbers: memory rules, spill detectors, the group-size formula, the anatomy of the two sliced builds, other measured costs |
| `patterns.md` | worked code: bucket expression, sectioned build script, staging and group reads, narrow-key panel SQL, running totals, ASOF, semi joins, equijoin gating, proof wrapper, compare script, validation queries, batch wrapper |
| `LICENSE` | MIT |

## Ground rules

- Size memory from a measured run that did not spill, at 1.2x its peak. Never from habit
  and never from the node's total RAM.
- Slice to stay in RAM, never for parallelism. One DuckDB process, all the threads, groups
  in sequence.
- Every step a temp table, projected and filtered first. Every `ON` a pure equijoin. Every
  pick total-ordered.
- Outputs go to a `.tmp` and a staged sibling; the live table is replaced by a rename after
  a coverage check.
- A rewrite is proved on an exact sample against the frozen reference before it replaces
  anything, and its accepted differences are written down with their size.
