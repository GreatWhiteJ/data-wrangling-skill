# Patterns

Worked SQL and build-script patterns, companion to `SKILL.md`. Each one is lifted from a
real build and trimmed to the shape that matters. Column names are generic
(`flag_a`, `flag_b`, `firm_id`); substitute your own.

## The bucket expression

Fixed in one place and used everywhere a table is staged or read by bucket. Always in
0..N-1: negatives and NULLs land in the last bucket, so no `__HIVE_DEFAULT_PARTITION__`
directory appears and no row is lost.

```python
def bucket_expr(col: str, n: int = 64) -> str:
    """SQL for the bucket of `col`."""
    return f"COALESCE(((CAST({col} AS BIGINT) % {n}) + {n}) % {n}, {n - 1})"

# ids stored as DOUBLE or VARCHAR: cast first
bucket_expr("TRY_CAST(user_id AS BIGINT)")
# -> COALESCE(((CAST(TRY_CAST(user_id AS BIGINT) AS BIGINT) % 64) + 64) % 64, 63)
```

## Build script skeleton

```python
import math, re, shutil, time
from pathlib import Path
import duckdb

N_BUCKETS = 64
LOOKUP_GB = 40      # resident `once` tables + what the group keeps alongside
BUCKET_GB = 13      # in-flight peak per bucket, from accounting MaxRSS of a group run
SAFETY = 1.0        # 1.5 when BUCKET_GB came from single-bucket runs
SECTION_RE = re.compile(r"^-- @@ (\w+)\s*$", re.M)

def load_sections(sql_file: Path) -> dict[str, str]:      # {"once": ..., "stage": ..., "bucket": ...}
    parts = SECTION_RE.split(sql_file.read_text(encoding="utf-8"))
    return {parts[i]: parts[i + 1] for i in range(1, len(parts), 2)}

def job_mem_gb() -> float:                                # the JOB's allocation, not the node's RAM
    import os, psutil
    mb = os.environ.get("SLURM_MEM_PER_NODE")
    return int(mb) / 1024 if mb else psutil.virtual_memory().total / 2**30

def connect(temp_dir: Path):
    mem = int(job_mem_gb() * 0.93)
    conn = duckdb.connect(config={"memory_limit": f"{mem}GB"})
    conn.execute(f"SET temp_directory = '{temp_dir.as_posix()}'")
    conn.execute("SET preserve_insertion_order = false")
    # bind every input as a view over its parquet files, same names in every stage
    for name, glob in VIEWS.items():
        conn.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{glob}')")
    return conn, mem

def group_size(mem_gb: int, n: int) -> int:
    g = max(1, min(n, int((mem_gb - LOOKUP_GB) / (BUCKET_GB * SAFETY))))
    return math.ceil(n / math.ceil(n / g))                # even groups

def run(conn, sql_text: str, subs: dict[str, str], tag: str) -> None:
    for k, v in subs.items():
        sql_text = sql_text.replace("{{" + k + "}}", v)
    if re.findall(r"\{\{(\w+)\}\}", sql_text):
        raise ValueError(f"{tag}: unsubstituted placeholders")
    for stmt in split_statements(sql_text):               # your statement splitter
        t0 = time.time()
        conn.execute(stmt)
        print(f"[{tag}] {time.time() - t0:.1f}s rss {rss_gb():.0f}G  {' '.join(stmt.split())[:70]}", flush=True)
```

The main loop:

```python
conn, mem = connect(temp_dir)
g = args.group or group_size(mem, len(buckets))
groups = [buckets[i:i + g] for i in range(0, len(buckets), g)]

run(conn, sections["once"], {}, "once")

marker = stage / "_STAGED"
if not (args.reuse_stage and marker.exists()):
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True)
    run(conn, sections["stage"],
        {"bucket": bucket_expr("TRY_CAST(user_id AS BIGINT)", N_BUCKETS), "stage": stage.as_posix()}, "stage")
    marker.write_text(time.strftime("%Y-%m-%d %H:%M:%S"))

for grp in groups:
    tag = f"b{grp[0]:02d}_{grp[-1]:02d}"
    out = out_dir / f"panel_{tag}.parquet"
    if out.exists():
        continue                                          # resume
    tmp = out.with_suffix(".parquet.tmp")
    subs = {"s_pos": "[" + ", ".join(f"'{ensure_partition(stage, 'pos', b)}'" for b in grp) + "]",
            "out": tmp.as_posix()}
    run(conn, sections["bucket"], subs, tag)
    tmp.rename(out)
```

An empty partition (a bucket with no rows of some input) must still yield a file, or the
glob list fails:

```python
def ensure_partition(stage: Path, name: str, b: int) -> str:
    d = stage / name / f"user_bucket={b}"
    if not d.exists() or not any(d.glob("*.parquet")):
        d.mkdir(parents=True, exist_ok=True)
        duckdb.connect().execute(
            f"COPY (SELECT * EXCLUDE (user_bucket) FROM read_parquet('{(stage / name).as_posix()}/*/*.parquet', "
            f"hive_partitioning = true) LIMIT 0) TO '{(d / 'empty.parquet').as_posix()}' (FORMAT PARQUET)")
    return (d / "*.parquet").as_posix()
```

Promotion, after a coverage check (every bucket present in the file names) and a row
count:

```python
def covered_buckets(out_dir: Path) -> set[int]:
    got = set()
    for f in out_dir.glob("panel_b*.parquet"):
        m = re.fullmatch(r"panel_b(\d\d)_(\d\d)\.parquet", f.name)
        if m:
            got.update(range(int(m.group(1)), int(m.group(2)) + 1))
    return got

def promote(live: Path, staged: Path, prev: Path) -> None:
    missing = set(range(N_BUCKETS)) - covered_buckets(staged)
    if missing:
        raise SystemExit(f"staged output misses buckets {sorted(missing)}: not promoting")
    n = duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{staged.as_posix()}/*.parquet')").fetchone()[0]
    print(f"staged: {n:,} rows")
    if prev.exists():
        shutil.rmtree(prev)
    if live.exists():
        live.rename(prev)
    staged.rename(live)
```

## Staging an input by bucket

```sql
-- @@ stage
COPY (
    SELECT user_id, position_id, startdate, enddate, firm_id, ...,   -- only the columns the bucket section uses
           {{bucket}} AS user_bucket                                 -- PARTITION_BY takes no expression
    FROM positions
) TO '{{stage}}/pos' (FORMAT PARQUET, COMPRESSION SNAPPY, PARTITION_BY (user_bucket), OVERWRITE_OR_IGNORE true);
```

A join-side alias needs its own placeholder (`{{bucket_im}}` for `im.user_id`).

Reading a group:

```sql
-- @@ bucket
CREATE OR REPLACE TEMP TABLE pos AS
SELECT * FROM read_parquet({{s_pos}}, hive_partitioning = false);
-- {{s_pos}} = ['.../pos/user_bucket=0/*.parquet', '.../pos/user_bucket=1/*.parquet', ...]
```

`hive_partitioning = false` keeps the schema identical to the reference (no `user_bucket`
column). Firm-keyed panels do the opposite on purpose: `read_parquet('<table>/data/**/*.parquet')`
with auto-detection appends `firm_bucket` and a `WHERE firm_bucket = 7` reads one directory.

## Narrow key table, wide join-back

The year explosion, the pick and the clocks run on 16 columns; the 110 wide columns are
read once and joined once, inside the COPY.

```sql
CREATE OR REPLACE TEMP TABLE pos AS                      -- wide, kept for the join-back
SELECT * FROM read_parquet({{s_pos}}, hive_partitioning = false);

CREATE OR REPLACE TEMP TABLE pos_key AS                  -- narrow
SELECT user_id, position_id, seq, firm_id, startdate, enddate,
       pay, departed, flag_a, flag_b, flag_c, birthday,
       MIN(startdate) OVER (PARTITION BY user_id, firm_id) AS firm_first_start
FROM pos
WHERE startdate < DATE '2026-01-01' AND enddate >= DATE '2000-01-01';

CREATE OR REPLACE TEMP TABLE position_years AS           -- one row per position x year it spans
SELECT p.user_id, d.period, p.position_id, p.seq, p.pay,
       CASE WHEN COALESCE(p.departed, FALSE) AND p.enddate >= d.period_start AND p.enddate < d.period_end
            THEN TRUE ELSE FALSE END AS departed,
       CASE WHEN COALESCE(p.flag_a, FALSE) AND p.enddate >= d.period_start AND p.enddate < d.period_end
            THEN TRUE ELSE FALSE END AS flag_a,
       ...
FROM pos_key p
CROSS JOIN periods d
WHERE p.startdate < d.period_end AND p.enddate >= d.period_start;
DROP TABLE pos_key;

CREATE OR REPLACE TEMP TABLE top_years AS                -- one position per user-year, deterministic
SELECT *, COUNT(*) OVER (PARTITION BY user_id, period) AS num_concurrent_positions
FROM position_years
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id, period
    ORDER BY departed DESC, pay DESC, seq DESC, position_id) = 1;
DROP TABLE position_years;

CREATE OR REPLACE TEMP TABLE clocks AS                   -- GROUP BY, not a window over the wide table
SELECT user_id,
       MIN(CASE WHEN flag_a OR flag_b OR flag_c THEN period END) AS first_any,
       MIN(CASE WHEN flag_a THEN period END)                     AS first_a
FROM top_years GROUP BY user_id;

CREATE OR REPLACE TEMP TABLE grid AS                     -- balanced: every person x every year
SELECT u.user_id, d.period, d.period_start, d.period_end
FROM (SELECT DISTINCT user_id FROM top_years) u CROSS JOIN periods d;

COPY (
SELECT g.user_id, w.firm_id, g.period, ..., t.departed, ..., c.first_any,
       (c.first_any IS NOT NULL AND g.period > c.first_any) AS post_any, ...
FROM grid g
LEFT JOIN top_years t ON g.user_id = t.user_id AND g.period = t.period
LEFT JOIN pos w       ON t.user_id = w.user_id AND t.position_id = w.position_id   -- wide, once
LEFT JOIN firm_years f ON w.firm_id = f.firm_id AND g.period = f.year
LEFT JOIN clocks c    ON g.user_id = c.user_id
ORDER BY g.user_id, g.period
) TO '{{out}}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 200000);
```

Contrast: the single-pass version selected about 110 columns in `position_years` and
carried them through the pick, the grid join, the firm-context join and a final step where
six `MIN(...) OVER (PARTITION BY user_id)` ran over the full width. 1h18m, 1292G, 1 TB
spilled, against 15 min, 427G, no spill.

## Running totals, not self-joins

Distinct entities seen to date: the first occurrence of each value per user, counted per
position, then a running sum. The old form self-joined positions on `seq <= p.seq` and
counted DISTINCT.

```sql
CREATE OR REPLACE TEMP TABLE entity_counts AS
WITH flags AS (
    SELECT user_id, seq, 'COUNTRY' AS entity_type, CAST(country AS VARCHAR) AS entity_value
    FROM position_base WHERE country IS NOT NULL
    UNION ALL
    SELECT user_id, seq, 'FIRM', CAST(firm_id AS VARCHAR) FROM position_base WHERE firm_id IS NOT NULL
),
firsts AS (
    SELECT user_id, first_seq AS seq,
           COUNT(*) FILTER (WHERE entity_type = 'COUNTRY') AS n_country,
           COUNT(*) FILTER (WHERE entity_type = 'FIRM')    AS n_firm
    FROM (SELECT user_id, entity_type, entity_value, MIN(seq) AS first_seq
          FROM flags GROUP BY user_id, entity_type, entity_value)
    GROUP BY user_id, first_seq
),
keys AS (SELECT DISTINCT user_id, seq FROM position_base)
SELECT k.user_id, k.seq,
       CAST(SUM(COALESCE(f.n_country, 0)) OVER w AS BIGINT) AS countries_count,
       CAST(SUM(COALESCE(f.n_firm, 0))    OVER w AS BIGINT) AS firms_count
FROM keys k
LEFT JOIN firsts f ON f.user_id = k.user_id AND f.seq = k.seq
WINDOW w AS (PARTITION BY k.user_id ORDER BY k.seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
```

The CTEs here sit inside one statement that produces one temp table, with a single window
as the only blocking operator, and it runs well. The rule is still a temp table per step:
stage a CTE as one whenever it is reused, needs a row count, or would add a second
blocking operator to the statement. Experience
years use the same window over `DECIMAL(18,4)` per-position years, so `ROUND(SUM(...), 1)`
is exact.

## ASOF and DISTINCT ON for date lookups

```sql
-- one row per (user, end date); the higher degree wins a same-day tie
CREATE OR REPLACE TEMP TABLE edu_asof AS
SELECT DISTINCT ON (user_id, enddate) *
FROM edu
ORDER BY user_id, enddate, has_advanced_degree DESC, has_bachelors DESC, degree_name;

-- latest degree at or before the position end
... FROM p
ASOF LEFT JOIN edu_asof e ON p.user_id = e.user_id AND p.edu_thr >= e.enddate

-- first degree strictly after the position end
SELECT q.position_id, e.enddate AS next_degree_date
FROM q
ASOF LEFT JOIN (SELECT user_id, enddate FROM edu) e ON q.user_id = e.user_id AND q.thr < e.enddate;
```

One inequality, on the ordering column, every other condition an equality; at most one
right row per left row. The DISTINCT ON before the ASOF is what makes the tie
deterministic.

## Semi joins and the restricted 2-hop

```sql
-- keys you hold, then the scan
SELECT e.* FROM flows e
SEMI JOIN (SELECT DISTINCT firm_id, window_year FROM pairs) o
  ON e.firm_from = o.firm_id AND e.window_year = o.window_year;

-- never this against a big scan: 90 min at zero CPU
-- WHERE EXISTS (SELECT 1 FROM sel s WHERE s.host = x.host AND s.year = x.year AND s.source = x.source)
-- this instead: 8 min
WHERE host IN (SELECT host FROM sel_hosts)
```

The 2-hop term `(W^2)_ab = SUM_k w_ak w_kb` is computed only over `e1` (first hops out of
a used origin) joined to `e2` (second hops into a used destination), then LEFT JOINed to
the pairs. Same sums as the full product for every pair that is looked up; the all-pairs
form materialised about 11B rows.

## Equijoin gating

```sql
-- wrong: constant in ON, BLOCKWISE_NL_JOIN, 98 min and counting on 227M x 5.5M
LEFT JOIN flags e ON p.firm_id = e.firm_id AND p.state = e.state AND p.departed = 1

-- right: equijoin, gate in the SELECT, 24 s
LEFT JOIN flags e ON p.firm_id = e.firm_id AND p.state = e.state AND p.end_month = e.month
...
CASE WHEN p.departed = 1 THEN e.flag_a END AS flag_a
```

Before the LEFT JOIN: `SELECT count(*) - count(DISTINCT (firm_id, state, month)) FROM flags`
must be 0.

## Window flags in one pass

Next-position leads feed the departure flags directly through a named window; no
materialised `with_leads` copy, no sort.

```sql
SELECT user_id, position_id, firm_id, enddate, ...,
       LEAD(job_category) OVER nxt AS next_job_category,
       CASE WHEN LEAD(firm_id) OVER nxt <> firm_id THEN 1 ELSE 0 END AS departed,
       CASE WHEN LEAD(firm_id) OVER nxt IS NOT NULL AND LEAD(firm_id) OVER nxt <> firm_id
             AND LEAD(startdate) OVER nxt >= enddate
            THEN (DATE_PART('year', LEAD(startdate) OVER nxt) - DATE_PART('year', enddate)) * 12
               + (DATE_PART('month', LEAD(startdate) OVER nxt) - DATE_PART('month', enddate)) END AS months_to_next
FROM selected_positions
WINDOW nxt AS (PARTITION BY user_id ORDER BY seq);
```

Project to the shipped columns before the window scan (the old build ran three windows over
all 24 raw columns). Dedup ties end in `position_id`, not `random()`.

## Proof wrapper

```bash
set -euo pipefail
SAMPLE=${SAMPLE:-0,17,42,63}
D=$(date +%F)
sed "s/CURRENT_DATE/DATE '$D'/g" $P/sql_steps_ref_<hash>.sql > $W/ref.sql
sed "s/CURRENT_DATE/DATE '$D'/g" $P/sql_steps.sql            > $W/new.sql
python -u $P/build.py --buckets $SAMPLE --sql-file $W/new.sql --stage-dir $STAGE --out-dir $W/new
python -u $P/build.py --ref-sample $SAMPLE --sql-file $W/ref.sql --out-dir $W/ref
python -u $P/compare.py --mem 400GB --threads 32 --a "$W/ref/*.parquet" --b "$W/new/*.parquet"
```

`--ref-sample` replaces the input view with the same parquet filtered to the sample
buckets, so the single-pass reference runs on those people only:

```python
conn.execute("CREATE OR REPLACE VIEW positions AS "
             f"SELECT * FROM read_parquet('{glob}') WHERE {bucket_expr('TRY_CAST(user_id AS BIGINT)')} IN ({buckets})")
```

## Compare script

Schema (names, types, order), row counts, distinct keys, `EXCEPT ALL` both ways with
DOUBLEs rounded to 6 dp, per-column `IS DISTINCT FROM` counts (DOUBLEs within 1e-9
relative) with 5 examples, exit 1 on any difference.

```python
"""python compare.py --a "<ref glob>" --b "<new glob>" [--key user_id,position_id]"""
import argparse, sys
import duckdb

ap = argparse.ArgumentParser()
ap.add_argument("--a", required=True); ap.add_argument("--b", required=True)
ap.add_argument("--key", default="user_id,position_id")
ap.add_argument("--mem", default="400GB"); ap.add_argument("--threads", type=int, default=32)
args = ap.parse_args()
key = [k.strip() for k in args.key.split(",")]

con = duckdb.connect(config={"memory_limit": args.mem, "threads": args.threads})
con.execute(f"CREATE VIEW a AS SELECT * FROM read_parquet('{args.a}')")
con.execute(f"CREATE VIEW b AS SELECT * FROM read_parquet('{args.b}')")

ok = True
ta = [(r[0], r[1]) for r in con.execute("DESCRIBE a").fetchall()]
tb = [(r[0], r[1]) for r in con.execute("DESCRIBE b").fetchall()]
if ta == tb:
    print(f"schema: identical, {len(ta)} columns")
else:
    ok = False
    print("schema: DIFFERENT")
    for i in range(max(len(ta), len(tb))):
        x = ta[i] if i < len(ta) else None
        y = tb[i] if i < len(tb) else None
        if x != y:
            print(f"  col {i}: ref={x} new={y}")
cols = [c for c, t in ta if (c, t) in tb]
dbl = {c for c, t in ta if t in ("DOUBLE", "FLOAT")}
k = ", ".join(key)

na = con.execute("SELECT COUNT(*) FROM a").fetchone()[0]
nb = con.execute("SELECT COUNT(*) FROM b").fetchone()[0]
ka = con.execute(f"SELECT COUNT(DISTINCT ({k})) FROM a").fetchone()[0]
kb = con.execute(f"SELECT COUNT(DISTINCT ({k})) FROM b").fetchone()[0]
print(f"rows: ref={na:,} new={nb:,}; distinct key: ref={ka:,} new={kb:,}", flush=True)
ok &= na == nb

proj = ", ".join(f"ROUND({c}, 6) AS {c}" if c in dbl else c for c in cols)
e1 = con.execute(f"SELECT COUNT(*) FROM (SELECT {proj} FROM a EXCEPT ALL SELECT {proj} FROM b)").fetchone()[0]
e2 = con.execute(f"SELECT COUNT(*) FROM (SELECT {proj} FROM b EXCEPT ALL SELECT {proj} FROM a)").fetchone()[0]
print(f"EXCEPT ALL (doubles at 6 dp): ref-new={e1:,} new-ref={e2:,}", flush=True)

if ka == na and kb == nb:
    con.execute(f"CREATE TEMP TABLE j AS SELECT * FROM a JOIN b USING ({k})")   # non-key cols come through as c / c_1
    nj = con.execute("SELECT COUNT(*) FROM j").fetchone()[0]
    print(f"joined on key: {nj:,}", flush=True)
    ok &= nj == na
    bad = []
    for c in cols:
        if c in key:
            continue
        r = f"{c}_1"
        if c in dbl:
            cond = (f"NOT ((j.{c} IS NULL AND j.{r} IS NULL) OR "
                    f"ABS(j.{c} - j.{r}) <= 1e-9 * GREATEST(1, ABS(j.{c})))")
        else:
            cond = f"j.{c} IS DISTINCT FROM j.{r}"
        n = con.execute(f"SELECT COUNT(*) FROM j WHERE {cond}").fetchone()[0]
        if n:
            bad.append((c, n, cond))
    if not bad:
        print("per-column: every column equal")
    for c, n, cond in sorted(bad, key=lambda x: -x[1]):
        ok = False
        print(f"per-column MISMATCH {c}: {n:,} rows")
        for row in con.execute(f"SELECT {k}, {c}, {c}_1 FROM j WHERE {cond} LIMIT 5").fetchall():
            print(f"    {row}")
else:
    print("keys not unique; per-column comparison skipped")
    ok = False

print("RESULT:", "IDENTICAL" if ok else "DIFFERENCES (see above)")
sys.exit(0 if ok else 1)
```

For a build with a known accepted tie class, add the tie logic: a different pick is
accepted only when both builds saw 2+ candidates equal on every rule column, and
downstream columns derived from the pick (clocks) are compared only for people without
such a tie.

## Validation queries that cannot lie

```sql
-- wrong: NULL flags vanish, result 0, reads as "bug confirmed"
SELECT SUM((is_key AND NOT (flag_a OR flag_b))::INT) FROM t;
-- right
SELECT COUNT(*) FILTER (WHERE is_key AND (flag_a OR flag_b) IS NOT TRUE) FROM t;

-- rows equal distinct key, or the writer / a join fanned out or dropped
SELECT COUNT(*), COUNT(DISTINCT (user_id, position_id)) FROM read_parquet('.../*.parquet');

-- a flag count, not a row count, tells vintages apart
SELECT COUNT(*) FILTER (WHERE flag_a), COUNT(*) FILTER (WHERE flag_b) FROM t;
```

## Batch wrapper for a build (SLURM)

```bash
#!/bin/bash
#SBATCH --ntasks=1 --cpus-per-task=32 --mem=500G --time=03:00:00
#SBATCH --mail-type=END,FAIL
set -euo pipefail
export DUCKDB_TEMP_DIR=<scratch>/duckdb_temp_<name>_${SLURM_JOB_ID}
export TMPDIR=$DUCKDB_TEMP_DIR
mkdir -p "$DUCKDB_TEMP_DIR"
trap 'rm -rf "$DUCKDB_TEMP_DIR"' EXIT
# activate the environment with its full path; compute nodes do not source your shell profile
cd <repo root>
python -u $P/build.py --stage-dir <scratch>/<name>_stage --reuse-stage "$@"
if [ $# -eq 0 ]; then python -u $P/build.py --promote; fi
```

No trailing `echo`: bash returns the last command's status, and a trailing echo turns a
failed build into a COMPLETED one that releases its `afterok` dependents.

## Waiting on a job

One held connection, backgrounded, that returns once:

```bash
ssh <cluster> 'while squeue -h -j <ids> | grep -q .; do sleep 60; done; sacct -j <ids> -o JobID,State,Elapsed,MaxRSS,MaxDiskWrite,TotalCPU,NCPUS --units=G'
```

From PowerShell, write the loop to a file with LF line endings and pipe it:
`Get-Content wait.sh -Raw | ssh <cluster> "bash -s"`.
