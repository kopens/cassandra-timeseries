#!/bin/bash
#
# Docker integration test for the time-series CQL functions.
#
# Boots the cassandra-timeseries image, loads a deterministic data set and asserts the
# result of every time-series function family against hand-computed values. This is the
# release gate: the unit tests exercise the functions in-process, this exercises them
# through a real node (schema, read path, aggregation, native protocol, cqlsh output).
#
#   ./docker/integration-test.sh [image]           # default: cassandra-timeseries:6.0.0
#   CONTAINER_RUNTIME=podman ./docker/integration-test.sh
#
# Build the image first:
#   docker build -t cassandra-timeseries:6.0.0 -f docker/Dockerfile .
#
set -uo pipefail

IMAGE="${1:-cassandra-timeseries:6.0.0}"
RUNTIME="${CONTAINER_RUNTIME:-}"
CONTAINER="cassandra-ts-it-$$"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

if [ -z "$RUNTIME" ]; then
    if command -v docker > /dev/null 2>&1; then RUNTIME=docker
    elif command -v podman > /dev/null 2>&1; then RUNTIME=podman
    else echo "FATAL: neither docker nor podman found"; exit 2
    fi
fi

cleanup() { $RUNTIME rm -f "$CONTAINER" > /dev/null 2>&1; }
trap cleanup EXIT

echo "== cassandra-timeseries time-series CQL integration test =="
echo "   runtime: $RUNTIME   image: $IMAGE"

$RUNTIME run -d --name "$CONTAINER" "$IMAGE" > /dev/null || { echo "FATAL: container did not start"; exit 2; }

# The node is ready once it answers CQL on the native protocol. A function rather than an inline
# loop because the restart section at the end of this script waits the same way.
wait_for_cql() {
    local ready=0
    printf '   waiting for CQL'
    for _ in $(seq 1 $((READY_TIMEOUT / 5))); do
        if $RUNTIME exec "$CONTAINER" cqlsh -e "SELECT release_version FROM system.local" > /dev/null 2>&1; then
            ready=1; break
        fi
        printf '.'
        sleep 5
    done
    echo
    if [ "$ready" != 1 ]; then
        echo "FATAL: node did not become ready within ${READY_TIMEOUT}s"
        $RUNTIME logs "$CONTAINER" 2>&1 | tail -40
        exit 2
    fi
}
wait_for_cql

cql() { $RUNTIME exec "$CONTAINER" cqlsh --no-color -e "$1" 2>&1; }

# Each check runs its own cqlsh, so a fixed startup cost sits in every measurement. Measure it
# once on a trivial query and report it, so the per-check numbers can be read honestly.
baseline_ms() {
    local t0 t1
    t0=$(date +%s%3N); cql "SELECT key FROM system.local;" > /dev/null 2>&1; t1=$(date +%s%3N)
    echo $((t1 - t0))
}
BASELINE=$(baseline_ms)

# HTML report (CI artifact): every assertion with the CQL it ran and the rows it got back.
REPORT="${IT_REPORT:-build/timeseries-it-report.html}"
REPORT_MD="${IT_REPORT_MD:-${REPORT%.html}.md}"
ROWS=$(mktemp); MDROWS=$(mktemp)
trap 'cleanup; rm -f "$ROWS" "$MDROWS"' EXIT
esc() { sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'; }

section() {
    printf '%s\n' "-- $1"
    printf '<tr class="section"><td colspan="5">%s</td></tr>\n' "$(printf '%s' "$1" | esc)" >> "$ROWS"
    printf '\n## %s\n' "$1" >> "$MDROWS"
}

PASS=0; FAIL=0
# check <description> <cql> <extended regex the output must match>
# Prints the assertion, the CQL it ran and the rows it got back, so a CI log (or a reviewer)
# can see exactly what was executed and what the node answered.
check() {
    local desc="$1" query="$2" expect="$3" out status t0 t1 ms
    t0=$(date +%s%3N)
    out=$(cql "$query")
    t1=$(date +%s%3N); ms=$((t1 - t0))
    if printf '%s' "$out" | grep -qE -- "$expect"; then
        PASS=$((PASS + 1)); status="ok  "
    else
        FAIL=$((FAIL + 1)); status="FAIL"
    fi
    local flat; flat=$(printf '%s' "$query" | tr '\n' ' ' | tr -s ' ')
    printf '\n   [%s] %-56s %6s ms\n' "$status" "$desc" "$ms"
    printf '        cql> %s\n' "$flat"
    printf '%s\n' "$out" | grep -vE '^[[:space:]]*$' | sed 's/^/        /'
    [ "$status" = FAIL ] && printf '        !! expected to match: /%s/\n' "$expect"

    {
        printf '<tr class="%s"><td class="st">%s</td><td>%s</td><td><pre>%s</pre></td><td><pre>%s</pre></td><td class="ms">%s ms</td></tr>\n' \
            "$([ "$status" = FAIL ] && echo fail || echo pass)" \
            "$([ "$status" = FAIL ] && echo '✗ FAIL' || echo '✓ pass')" \
            "$(printf '%s' "$desc" | esc)" \
            "$(printf '%s' "$flat" | esc)" \
            "$(printf '%s' "$out" | grep -vE '^[[:space:]]*$' | esc)" \
            "$ms"
    } >> "$ROWS"
    {
        printf '\n### %s %s — `%s ms`\n\n```sql\n%s\n```\n\n```\n%s\n```\n' \
            "$([ "$status" = FAIL ] && echo '❌' || echo '✅')" "$desc" "$ms" "$flat" \
            "$(printf '%s\n' "$out" | grep -vE '^[[:space:]]*$')"
    } >> "$MDROWS"
    return 0
}

ntool() { $RUNTIME exec "$CONTAINER" nodetool "$@" 2>&1; }

# check_nodetool <description> <extended regex, or '' for exit-code-only> <nodetool args...>
# Same contract and reporting as check, but runs nodetool in the container instead of cqlsh;
# the command must exit 0 AND (if a regex is given) its output must match it.
check_nodetool() {
    local desc="$1" expect="$2"; shift 2
    local out rc status t0 t1 ms
    t0=$(date +%s%3N)
    out=$(ntool "$@"); rc=$?
    t1=$(date +%s%3N); ms=$((t1 - t0))
    if [ "$rc" -eq 0 ] && { [ -z "$expect" ] || printf '%s' "$out" | grep -qE -- "$expect"; }; then
        PASS=$((PASS + 1)); status="ok  "
    else
        FAIL=$((FAIL + 1)); status="FAIL"
    fi
    local flat="nodetool $*"
    printf '\n   [%s] %-56s %6s ms\n' "$status" "$desc" "$ms"
    printf '        cmd> %s\n' "$flat"
    printf '%s\n' "$out" | grep -vE '^[[:space:]]*$' | sed 's/^/        /'
    [ "$status" = FAIL ] && printf '        !! exit=%s, expected to match: /%s/\n' "$rc" "$expect"

    {
        printf '<tr class="%s"><td class="st">%s</td><td>%s</td><td><pre>%s</pre></td><td><pre>%s</pre></td><td class="ms">%s ms</td></tr>\n' \
            "$([ "$status" = FAIL ] && echo fail || echo pass)" \
            "$([ "$status" = FAIL ] && echo '✗ FAIL' || echo '✓ pass')" \
            "$(printf '%s' "$desc" | esc)" \
            "$(printf '%s' "$flat" | esc)" \
            "$(printf '%s' "$out" | grep -vE '^[[:space:]]*$' | esc)" \
            "$ms"
    } >> "$ROWS"
    {
        printf '\n### %s %s — `%s ms`\n\n```sql\n%s\n```\n\n```\n%s\n```\n' \
            "$([ "$status" = FAIL ] && echo '❌' || echo '✅')" "$desc" "$ms" "$flat" \
            "$(printf '%s\n' "$out" | grep -vE '^[[:space:]]*$')"
    } >> "$MDROWS"
    return 0
}

# assert <description> <actual> <expected> -- for values this script computed itself rather than
# read out of a cqlsh table (sstable counts, and anything else that comes from nodetool parsing).
# Reports the value either way, so a passing run says what it saw instead of just "ok".
assert() {
    local desc="$1" actual="${2:-<none>}" expected="$3" status
    if [ "$actual" = "$expected" ]; then PASS=$((PASS + 1)); status="ok  "
    else FAIL=$((FAIL + 1)); status="FAIL"; fi
    printf '\n   [%s] %-56s\n' "$status" "$desc"
    printf '        got> %s   (expected: %s)\n' "$actual" "$expected"

    {
        printf '<tr class="%s"><td class="st">%s</td><td>%s</td><td><pre>%s</pre></td><td><pre>%s</pre></td><td class="ms">%s</td></tr>\n' \
            "$([ "$status" = FAIL ] && echo fail || echo pass)" \
            "$([ "$status" = FAIL ] && echo '✗ FAIL' || echo '✓ pass')" \
            "$(printf '%s' "$desc" | esc)" \
            "computed by the harness (expected: $(printf '%s' "$expected" | esc))" \
            "$(printf '%s' "$actual" | esc)" \
            ""
    } >> "$ROWS"
    {
        printf '\n### %s %s\n\n```\nexpected: %s\ngot:      %s\n```\n' \
            "$([ "$status" = FAIL ] && echo '❌' || echo '✅')" "$desc" "$expected" "$actual"
    } >> "$MDROWS"
    return 0
}

write_report() {
    mkdir -p "$(dirname "$REPORT")"
    {
        cat <<HTML
<!doctype html>
<meta charset="utf-8">
<title>cassandra-timeseries · time-series CQL integration test</title>
<style>
  :root { color-scheme: light dark; --bg:#fff; --fg:#1a1a1a; --muted:#666; --line:#e3e3e3;
          --pass:#0a7d33; --fail:#c62828; --code:#f6f7f9; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#14161a; --fg:#e6e6e6; --muted:#9aa0a6; --line:#2c3036;
            --pass:#5ec97f; --fail:#ff6b6b; --code:#1c1f24; }
  }
  body { background:var(--bg); color:var(--fg); margin:0 auto; padding:2rem 1.25rem; max-width:1100px;
         font:14px/1.5 ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif; }
  h1 { font-size:1.35rem; margin:0 0 .25rem; }
  .meta { color:var(--muted); font-size:.85rem; margin-bottom:1.25rem; }
  .meta code { background:var(--code); padding:.1rem .35rem; border-radius:4px; }
  .summary { display:flex; gap:1.5rem; align-items:baseline; margin:1rem 0 1.5rem;
             padding:.9rem 1.1rem; background:var(--code); border-radius:8px; }
  .summary b { font-size:1.6rem; }
  .ok-n { color:var(--pass); } .fail-n { color:var(--fail); }
  table { border-collapse:collapse; width:100%; }
  td { border-top:1px solid var(--line); padding:.55rem .6rem; vertical-align:top; }
  tr.section td { background:var(--code); font-weight:600; letter-spacing:.02em; }
  td.st { white-space:nowrap; font-weight:600; }
  tr.pass td.st { color:var(--pass); } tr.fail td.st { color:var(--fail); }
  tr.fail { background:color-mix(in srgb, var(--fail) 8%, transparent); }
  pre { margin:0; background:var(--code); padding:.5rem .6rem; border-radius:6px; overflow-x:auto;
        font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace; white-space:pre; }
  td.ms { white-space:nowrap; text-align:right; font-variant-numeric:tabular-nums; color:var(--muted); }
  th { color:var(--muted); font-weight:600; font-size:.78rem; text-transform:uppercase; letter-spacing:.04em;
       text-align:left; padding:.4rem .6rem; }
  td:nth-child(3) { width:32%; } td:nth-child(4) { width:34%; }
</style>
<h1>time-series CQL integration test</h1>
<div class="meta">
  image <code>$IMAGE</code> · runtime <code>$RUNTIME</code> · $(date -u '+%Y-%m-%d %H:%M UTC')
</div>
<div class="summary">
  <span><b class="ok-n">$PASS</b> passed</span>
  <span><b class="fail-n">$FAIL</b> failed</span>
  <span style="color:var(--muted)">assertions run against a live node booted from the image</span>
</div>
<p class="meta">Times are the full cqlsh round trip for one query; a trivial query on this host
costs <b>${BASELINE} ms</b> of that (process startup + connect), so subtract roughly that much
to read the CQL execution time. Server-side timings on a 100M-row data set are in the
scale-test report.</p>
<table>
<tr><th></th><th>assertion</th><th>cql</th><th>result</th><th>elapsed</th></tr>
HTML
        cat "$ROWS"
        printf '</table>\n'
    } > "$REPORT"

    # Markdown twin of the same report — this is the copy that gets committed and linked
    # from the README, since GitLab renders Markdown but not HTML blobs.
    {
        printf '# time-series CQL integration test\n\n'
        printf 'image `%s` · runtime `%s` · %s\n\n' "$IMAGE" "$RUNTIME" "$(date -u '+%Y-%m-%d %H:%M UTC')"
        printf '**%s passed, %s failed** — assertions run against a live node booted from the image.\n\n' \
            "$PASS" "$FAIL"
        printf '> Times are the full cqlsh round trip for one query; a trivial query on this host costs\n'
        printf '> %s ms of that (process startup + connect), so subtract roughly that much to read the CQL\n' "$BASELINE"
        printf '> execution time. Server-side timings on a 100M-row data set are in the scale-test report.\n'
        cat "$MDROWS"
    } > "$REPORT_MD"
    echo "   report: $REPORT"
    echo "   report: $REPORT_MD"
}

# The image under test is built from the repo (docker/Dockerfile compiles it in a builder stage), so
# the version the node reports is the one thing that distinguishes "I tested this change" from "I
# tested whatever was tagged cassandra-timeseries:6.0.0 last month". It also pins the fork's
# versioning rule -- an upstream merge that resets base.version to an alpha would land here.
section "image identity"
check "the node reports release_version 6.0.0" \
    "SELECT release_version FROM system.local;" '^ *6\.0\.0 *$'

section "schema & fixture data"
cql "
CREATE KEYSPACE IF NOT EXISTS it WITH replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE IF NOT EXISTS it.metrics (
    series text, ts timestamp, value double,
    PRIMARY KEY (series, ts)
) WITH CLUSTERING ORDER BY (ts ASC)
   AND compaction = {'class':'UnifiedCompactionStrategy',
                     'scaling_parameters':'T4',
                     'target_sstable_size':'1GiB',
                     'expired_sstable_check_frequency_seconds':600};

CREATE TABLE IF NOT EXISTS it.counters (
    series text, ts timestamp, total counter,
    PRIMARY KEY (series, ts)
);

CREATE TABLE IF NOT EXISTS it.xy (
    k text, ts timestamp, x double, y double,
    PRIMARY KEY (k, ts)
);
" > /dev/null

# 'cpu': 10 @09:05, 30 @09:35, 50 @10:15, 70 @10:45  (span 6000s, delta 60)
# 'gap': 60 @09:30, 120 @12:30                       (10:00 and 11:00 buckets empty)
cql "
INSERT INTO it.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:05:00+0000', 10);
INSERT INTO it.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:35:00+0000', 30);
INSERT INTO it.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:15:00+0000', 50);
INSERT INTO it.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:45:00+0000', 70);
INSERT INTO it.metrics (series, ts, value) VALUES ('gap', '2024-01-01 09:30:00+0000', 60);
INSERT INTO it.metrics (series, ts, value) VALUES ('gap', '2024-01-01 12:30:00+0000', 120);

UPDATE it.counters SET total = total + 100 WHERE series='api' AND ts='2024-01-01 09:00:00+0000';
UPDATE it.counters SET total = total + 300 WHERE series='api' AND ts='2024-01-01 09:01:00+0000';
UPDATE it.counters SET total = total +  50 WHERE series='api' AND ts='2024-01-01 09:02:00+0000';

INSERT INTO it.xy (k, ts, x, y) VALUES ('r', '2024-01-01 09:00:00+0000', 1, 3);
INSERT INTO it.xy (k, ts, x, y) VALUES ('r', '2024-01-01 09:01:00+0000', 2, 5);
INSERT INTO it.xy (k, ts, x, y) VALUES ('r', '2024-01-01 09:02:00+0000', 3, 7);
INSERT INTO it.xy (k, ts, x, y) VALUES ('r', '2024-01-01 09:03:00+0000', 4, 9);
" > /dev/null

section "time_bucket / downsampling"
# hourly avg: 09:00 -> (10+30)/2 = 20, 10:00 -> (50+70)/2 = 60
check "time_bucket(1h) hourly avg = 20 / 60" \
    "SELECT time_bucket(1h, ts), avg(value) FROM it.metrics WHERE series='cpu' GROUP BY series, time_bucket(1h, ts);" \
    '09:00:00.*\| *20'
check "time_bucket(1h) second bucket = 60" \
    "SELECT time_bucket(1h, ts), avg(value) FROM it.metrics WHERE series='cpu' GROUP BY series, time_bucket(1h, ts);" \
    '10:00:00.*\| *60'
check "time_bucket scalar assigns bucket start" \
    "SELECT ts, time_bucket(1h, ts) AS b FROM it.metrics WHERE series='cpu' LIMIT 1;" \
    '09:05:00.*09:00:00'

section "first / last / delta / rate / derivative"
check "first(value, ts) = 10" \
    "SELECT first(value, ts) FROM it.metrics WHERE series='cpu';" '^ *10'
check "last(value, ts) = 70" \
    "SELECT last(value, ts) FROM it.metrics WHERE series='cpu';" '^ *70'
check "delta(value, ts) = 60" \
    "SELECT delta(value, ts) FROM it.metrics WHERE series='cpu';" '^ *60'
check "rate(value, ts) = 60/6000s = 0.01" \
    "SELECT rate(value, ts) FROM it.metrics WHERE series='cpu';" '^ *0\.01'
# least-squares slope over (0s,10) (1800s,30) (4200s,50) (6000s,70) = 204000/20880000
check "derivative(value, ts) ~ 0.00977 (least squares, != rate)" \
    "SELECT derivative(value, ts) FROM it.metrics WHERE series='cpu';" '^ *0\.0097'

section "counters (reset aware)"
# counter samples 100 -> 300 -> 50: +200, then the drop is a reset so the new value counts
# in full => 250 over 120s. Plain rate() would see a -250 step instead.
check "counter_delta detects the reset (= 250)" \
    "SELECT counter_delta(total, ts) FROM it.counters WHERE series='api';" '^ *250'
check "counter_rate = 250/120s ~ 2.083" \
    "SELECT counter_rate(total, ts) FROM it.counters WHERE series='api';" '^ *2\.08'

section "percentile / spread / distribution"
check "percentile(value, 0.5) = 40" \
    "SELECT percentile(value, 0.5) FROM it.metrics WHERE series='cpu';" '^ *40'
check "percentile(value, 0.95) = 67" \
    "SELECT percentile(value, 0.95) FROM it.metrics WHERE series='cpu';" '^ *67'
check "variance(value) = 666.66 (sample)" \
    "SELECT variance(value) FROM it.metrics WHERE series='cpu';" '^ *666\.6'
check "stddev(value) = 25.81" \
    "SELECT stddev(value) FROM it.metrics WHERE series='cpu';" '^ *25\.8'
check "approx_count_distinct = 4" \
    "SELECT approx_count_distinct(value) FROM it.metrics WHERE series='cpu';" '^ *4'
check "histogram returns nbuckets+2 entries" \
    "SELECT histogram(value, 0, 100, 10) FROM it.metrics WHERE series='cpu';" '\[0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0\]'

section "integral / time-weighted average"
# trapezoids: (10+30)/2*1800 + (30+50)/2*2400 + (50+70)/2*1800 = 240000 value-seconds
# cqlsh prints large doubles in scientific notation, so accept both spellings of 240000
check "integral(value, ts) = 240000 value-seconds" \
    "SELECT integral(value, ts) FROM it.metrics WHERE series='cpu';" '^ *(240000|2\.4e\+05)'
check "time_weighted_average = 240000/6000 = 40" \
    "SELECT time_weighted_average(value, ts) FROM it.metrics WHERE series='cpu';" '^ *40'

section "two-variable statistics / regression (y = 2x + 1)"
check "regr_slope(y, x) = 2" "SELECT regr_slope(y, x) FROM it.xy WHERE k='r';" '^ *2'
check "regr_intercept(y, x) = 1" "SELECT regr_intercept(y, x) FROM it.xy WHERE k='r';" '^ *1'
check "regr_r2(y, x) = 1" "SELECT regr_r2(y, x) FROM it.xy WHERE k='r';" '^ *1'
check "corr(y, x) = 1" "SELECT corr(y, x) FROM it.xy WHERE k='r';" '^ *1'
check "covar_pop(y, x) = 2.5" "SELECT covar_pop(y, x) FROM it.xy WHERE k='r';" '^ *2\.5'
check "covar_samp(y, x) = 3.33" "SELECT covar_samp(y, x) FROM it.xy WHERE k='r';" '^ *3\.3'

section "gap-fill: time_bucket_gapfill + locf + interpolate"
GAPFILL="time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000')"
# 6 buckets 08:00..13:00; real data only in 09:00 (60) and 12:00 (120)
check "gapfill materialises every bucket (6 rows)" \
    "SELECT $GAPFILL, avg(value) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '\(6 rows\)'
check "gapfill empty bucket is null by default" \
    "SELECT $GAPFILL, avg(value) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '10:00:00.*\| *null'
check "locf carries the previous bucket forward (10:00 -> 60)" \
    "SELECT $GAPFILL, locf(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '10:00:00.*\| *60'
check "locf leaves buckets before the first value null (08:00)" \
    "SELECT $GAPFILL, locf(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '08:00:00.*\| *null'
check "interpolate ramps 60 -> 120 linearly (10:00 -> 80)" \
    "SELECT $GAPFILL, interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '10:00:00.*\| *80'
check "interpolate ramps 60 -> 120 linearly (11:00 -> 100)" \
    "SELECT $GAPFILL, interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '11:00:00.*\| *100'
check "interpolate leaves the trailing bucket null (13:00)" \
    "SELECT $GAPFILL, interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '13:00:00.*\| *null'

section "dashboard query (all together)"
check "combined OHLC + change + p95 query runs" \
    "SELECT time_bucket(1h, ts) AS bucket, count(value) AS samples, first(value, ts) AS open,
            last(value, ts) AS close, min(value) AS low, max(value) AS high, avg(value) AS mean,
            delta(value, ts) AS change, rate(value, ts) AS per_second, percentile(value, 0.95) AS p95
     FROM it.metrics WHERE series='cpu' AND ts >= '2024-01-01 00:00:00+0000' AND ts < '2024-01-02 00:00:00+0000'
     GROUP BY series, time_bucket(1h, ts);" \
    '\(2 rows\)'

section "full-text search - SAI LIKE with index_analyzer"
cql "
CREATE TABLE IF NOT EXISTS it.logs (
    device text, ts timestamp, msg text,
    PRIMARY KEY (device, ts)
) WITH CLUSTERING ORDER BY (ts ASC);
CREATE INDEX IF NOT EXISTS logs_msg_idx ON it.logs(msg) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'ngram' };

INSERT INTO it.logs (device, ts, msg) VALUES ('pump-01', '2024-01-01 09:00:00+0000', 'connection timeout on port 9042');
INSERT INTO it.logs (device, ts, msg) VALUES ('pump-01', '2024-01-01 09:05:00+0000', '펌프 정지 알림');
INSERT INTO it.logs (device, ts, msg) VALUES ('pump-01', '2024-01-01 09:10:00+0000', 'connection refused');
INSERT INTO it.logs (device, ts, msg) VALUES ('pump-02', '2024-01-01 09:00:00+0000', 'timeout waiting for pump-02');
" > /dev/null
sleep 3   # wait for the index to become queryable

check "LIKE '%timeout%' within partition + time range" \
    "SELECT ts, msg FROM it.logs WHERE device='pump-01'
       AND ts >= '2024-01-01 09:00:00+0000' AND ts < '2024-01-01 10:00:00+0000'
       AND msg LIKE '%timeout%';" \
    'connection timeout on port 9042'
check "LIKE mid-word fragment '%imeou%' (true substring)" \
    "SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%imeou%';" \
    'connection timeout on port 9042'
check "LIKE korean fragment '%정지%'" \
    "SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%정지%';" \
    '펌프 정지 알림'
check "LIKE korean fragment crossing a space '%프 정%'" \
    "SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%프 정%';" \
    '펌프 정지 알림'
check "LIKE excludes non-matching rows (1 row only)" \
    "SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%refused%';" \
    '\(1 rows\)'
check "LIKE prefix 'connection%'" \
    "SELECT count(*) FROM it.logs WHERE device='pump-01' AND msg LIKE 'connection%';" \
    '^ *2'
check "LIKE suffix '%9042'" \
    "SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%9042';" \
    'connection timeout on port 9042'
check "'=' keeps exact whole-value semantics on the analyzed column" \
    "SELECT count(*) FROM it.logs WHERE device='pump-01' AND msg = 'connection refused';" \
    '^ *1'
check "LIKE combines with time_bucket aggregation" \
    "SELECT time_bucket(1h, ts), count(*) FROM it.logs
      WHERE device='pump-01' AND ts >= '2024-01-01 00:00:00+0000' AND ts < '2024-01-02 00:00:00+0000'
        AND msg LIKE '%connection%'
      GROUP BY device, time_bucket(1h, ts);" \
    '^ *2024-01-01 09:00:00.* 2'

section "tiered storage: policy, retier, chunk re-encode, late merge"
# The simplest supported shape (TieringPolicy.unsupportedSchemaError accepts far more -- composite
# partition keys, many regular columns, static columns): one text partition key, timestamp
# clustering, one double value, which is also all the re-encoder can encode until it goes columnar.
cql "
CREATE TABLE IF NOT EXISTS it.sensor (
    tag_id text, timestamp timestamp, value double,
    PRIMARY KEY (tag_id, timestamp)
);
" > /dev/null

# Policy: hot_window 1h / chunk_window 1h, so the rows inserted 4 hours in the past below are
# already closed. The policy JSON goes in as a plain string (this fork stores non-0x extension
# values as their UTF-8 bytes), so no hex encoding step is needed.
TIERING_JSON='{"hot_window":"1h","chunk_window":"1h","interval":"5m"}'

# Hour-aligned chunk window 4 hours in the past (epoch-millis CQL timestamp literals throughout,
# so the assertions do not depend on any date-formatting tool).
NOW_MS=$(date +%s%3N)
WIN_MS=$(( (NOW_MS / 3600000 - 4) * 3600000 ))
WIN_END_MS=$(( WIN_MS + 3600000 ))

check "ALTER TABLE installs the tiering policy (plain JSON string)" \
    "ALTER TABLE it.sensor WITH extensions = {'timeseries_tiering': '$TIERING_JSON'};
     SELECT extensions FROM system_schema.tables WHERE keyspace_name='it' AND table_name='sensor';" \
    'timeseries_tiering'

cql "
INSERT INTO it.sensor (tag_id, timestamp, value) VALUES ('pump-01', $(( WIN_MS +  300000 )), 10);
INSERT INTO it.sensor (tag_id, timestamp, value) VALUES ('pump-01', $(( WIN_MS +  900000 )), 20);
INSERT INTO it.sensor (tag_id, timestamp, value) VALUES ('pump-01', $(( WIN_MS + 1500000 )), 30);
" > /dev/null

check_nodetool "nodetool retier runs one re-encode cycle" '' retier it sensor
check "chunk row created for the window (samples = 3)" \
    "SELECT samples FROM it.sensor__chunks WHERE tag_id='pump-01' AND window_start=$WIN_MS;" \
    '^ *3'
check "re-encoded rows stay transparently readable (SP3: merged from the chunk)" \
    "SELECT count(*) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *3'
check "transparent point lookup returns the encoded value" \
    "SELECT value FROM it.sensor WHERE tag_id='pump-01' AND timestamp = $(( WIN_MS + 900000 ));" \
    '^ *20'
check "window remains queryable via the chunk table" \
    "SELECT tag_id, window_start, codec, samples FROM it.sensor__chunks WHERE tag_id='pump-01';" \
    '\(1 rows\)'

# Late row into the already-encoded window: its server-side writetime is newer than the range
# tombstone the first cycle issued (USING TIMESTAMP maxWt), so it survives to be merged.
cql "INSERT INTO it.sensor (tag_id, timestamp, value) VALUES ('pump-01', $(( WIN_MS + 2100000 )), 40);" > /dev/null
check "late row joins the merged view (3 chunk samples + 1 live late row)" \
    "SELECT count(*) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *4'

check_nodetool "nodetool retier merges the late row" '' retier it sensor
check "chunk re-encoded merged (samples 3 -> 4)" \
    "SELECT samples FROM it.sensor__chunks WHERE tag_id='pump-01' AND window_start=$WIN_MS;" \
    '^ *4'
check "merged view still complete after the late row is re-encoded (4 chunk samples)" \
    "SELECT count(*) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *4'
check "aggregate spans the chunk transparently (avg of 10,20,30,40)" \
    "SELECT avg(value) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *25'

check_nodetool "nodetool tieringstatus lists it.sensor (interval 5m)" 'it +sensor +300000' tieringstatus
check "system_views.timeseries_tiering exposes policy and run stats" \
    "SELECT keyspace_name, table_name, windows_encoded, rows_encoded, late_merges
       FROM system_views.timeseries_tiering;" \
    'it \| +sensor \| +1 \| +1 \| +1'

# The delete timestamp: the re-encoder tombstones the encoded window with USING TIMESTAMP
# max_row_writetime, so a chunk that recorded no writetime could not have deleted anything safely.
check "chunk records max_row_writetime (the delete's USING TIMESTAMP)" \
    "SELECT max_row_writetime FROM it.sensor__chunks WHERE tag_id='pump-01' AND window_start=$WIN_MS;" \
    '^ *1[0-9]{15}'

# --- the deletion gate ---
# Every assertion above is satisfied whether or not the re-encoder ever deleted the base rows:
# transparent reads reconcile the chunk's rows with any live base rows at the same clustering, so
# count(*) is 4 and avg(value) is 25 either way. A regression that silently stopped issuing the
# range delete -- the thing that actually reclaims the disk -- would pass all of them green.
#
# A shell script cannot reach TransparentReads.enterInternalBypass to read the raw hot rows, so the
# one signal available from cqlsh that the merge cannot fabricate is to take the chunk away: with
# the chunk row deleted there is nothing left to reconstruct from, and anything the base table still
# returns for that window is a row the re-encoder failed to delete. 0 rows means the delete ran;
# 4 rows means it did not.
cql "DELETE FROM it.sensor__chunks WHERE tag_id='pump-01' AND window_start=$WIN_MS;" > /dev/null
check "re-encoded base rows are physically gone (window empty once the chunk is removed)" \
    "SELECT count(*) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *0$'

section "tiered storage: the production table shape (tm_tag_point)"
# pp.tm_tag_point's exact shape -- 7 statics, 7 regular columns spanning text/int/double/boolean and
# a frozen map, DESC clustering -- written with pp's measured column behaviour: quality constantly
# 192, error_code constantly 0, attribute constantly {}, value_numeric and value_boolean never
# written, latency high-entropy, value a short numeric-looking text. Those are exactly the v3
# columnar format's CONSTANT / all-null / dictionary paths.
cql "
CREATE TABLE IF NOT EXISTS it.tm_tag_point (
    tag_id text,
    timestamp timestamp,
    area_id text static, asset_id text static, line_id text static,
    opc_id text static, site_id text static, tag_name text static, type text static,
    attribute frozen<map<text,text>>,
    error_code int, latency int, quality int,
    value text, value_boolean boolean, value_numeric double,
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
" > /dev/null

check "ALTER TABLE installs the tiering policy on the production shape" \
    "ALTER TABLE it.tm_tag_point WITH extensions = {'timeseries_tiering': '$TIERING_JSON'};
     SELECT extensions FROM system_schema.tables WHERE keyspace_name='it' AND table_name='tm_tag_point';" \
    'timeseries_tiering'

cql "
INSERT INTO it.tm_tag_point (tag_id, area_id, asset_id, line_id, opc_id, site_id, tag_name, type)
     VALUES ('TAG-001', 'A1', 'AS1', 'L1', 'OPC1', 'S1', 'boiler.temp', 'analog');
INSERT INTO it.tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value)
     VALUES ('TAG-001', $(( WIN_MS +  60000 )), {}, 0,  17, 192, '23.4');
INSERT INTO it.tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value)
     VALUES ('TAG-001', $(( WIN_MS + 120000 )), {}, 0, 431, 192, '23.6');
INSERT INTO it.tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value)
     VALUES ('TAG-001', $(( WIN_MS + 180000 )), {}, 0,   3, 192, '23.5');
INSERT INTO it.tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value)
     VALUES ('TAG-001', $(( WIN_MS + 240000 )), {}, 0, 902, 192, '23.9');
INSERT INTO it.tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value)
     VALUES ('TAG-001', $(( WIN_MS + 300000 )), {}, 0,  44, 192, '24.1');
" > /dev/null

check_nodetool "nodetool retier encodes the production-shaped window" '' retier it tm_tag_point
check "chunk row created for the production shape (samples = 5)" \
    "SELECT samples FROM it.tm_tag_point__chunks WHERE tag_id='TAG-001' AND window_start=$WIN_MS;" \
    '^ *5'

check "every column of a re-encoded row reads back through the merge" \
    "SELECT attribute, error_code, latency, quality, value FROM it.tm_tag_point
      WHERE tag_id='TAG-001' AND timestamp = $(( WIN_MS + 120000 ));" \
    '\{\} \| +0 \| +431 \| +192 \| +23\.6'
check "columns that were never written stay null after re-encoding" \
    "SELECT value_boolean, value_numeric FROM it.tm_tag_point
      WHERE tag_id='TAG-001' AND timestamp = $(( WIN_MS + 120000 ));" \
    'null \| +null'
check "static columns survive the re-encoder's range delete" \
    "SELECT site_id, tag_name, type FROM it.tm_tag_point WHERE tag_id='TAG-001' LIMIT 1;" \
    'S1 \| +boiler\.temp \| +analog'

# DESC clustering end to end. A bound-arithmetic bug here previously returned ZERO cold rows with no
# error at all, so both bound directions and both orderings are asserted, not just the row count.
# `check` greps line by line, so ordering is asserted with LIMIT 1 (which row comes FIRST) rather
# than with a multi-line regex -- and LIMIT 1 through the merge is itself worth pinning.
check "unqualified SELECT returns the whole merged window (5 rows)" \
    "SELECT timestamp, value FROM it.tm_tag_point WHERE tag_id='TAG-001';" \
    '\(5 rows\)'
check "DESC table serves the merged window newest-first (LIMIT 1 = 24.1)" \
    "SELECT value FROM it.tm_tag_point WHERE tag_id='TAG-001' LIMIT 1;" \
    '^ *24\.1'
check "WHERE timestamp < X on a DESC table (3 of 5 rows)" \
    "SELECT count(*) FROM it.tm_tag_point WHERE tag_id='TAG-001' AND timestamp < $(( WIN_MS + 240000 ));" \
    '^ *3'
check "WHERE timestamp > X on a DESC table (2 of 5 rows)" \
    "SELECT count(*) FROM it.tm_tag_point WHERE tag_id='TAG-001' AND timestamp > $(( WIN_MS + 180000 ));" \
    '^ *2'
check "ORDER BY timestamp ASC re-reverses the merged window (LIMIT 1 = 23.4)" \
    "SELECT value FROM it.tm_tag_point WHERE tag_id='TAG-001' ORDER BY timestamp ASC LIMIT 1;" \
    '^ *23\.4'
check "ORDER BY timestamp ASC still returns every merged row (5 rows)" \
    "SELECT timestamp, value FROM it.tm_tag_point WHERE tag_id='TAG-001' ORDER BY timestamp ASC;" \
    '\(5 rows\)'

# Cold immutability (Task 3 rule C): once a clustering is older than hot_window it is immutable,
# because a tombstone against it would only mask the chunk's copy until gc_grace_seconds purged it.
check "a cold row DELETE is rejected and points at cold_window" \
    "DELETE FROM it.tm_tag_point WHERE tag_id='TAG-001' AND timestamp = $(( WIN_MS + 120000 ));" \
    'cold_window'
check "UPDATE ... SET col = null on a cold row is rejected (it writes a cell tombstone)" \
    "UPDATE it.tm_tag_point SET latency = null WHERE tag_id='TAG-001' AND timestamp = $(( WIN_MS + 120000 ));" \
    'cold_window'
check "a whole-partition DELETE is rejected (it has no clustering bound at all)" \
    "DELETE FROM it.tm_tag_point WHERE tag_id='TAG-001';" \
    'cold_window'
check "the rejected deletes changed nothing (all 5 rows still there)" \
    "SELECT count(*) FROM it.tm_tag_point WHERE tag_id='TAG-001';" \
    '^ *5'

# ...and a write inside the hot window is untouched by the rule.
cql "INSERT INTO it.tm_tag_point (tag_id, timestamp, latency, quality, value)
          VALUES ('TAG-001', $NOW_MS, 9, 192, '25.0');" > /dev/null
check "a DELETE inside the hot window still works" \
    "DELETE FROM it.tm_tag_point WHERE tag_id='TAG-001' AND timestamp = $NOW_MS;
     SELECT count(*) FROM it.tm_tag_point WHERE tag_id='TAG-001' AND timestamp = $NOW_MS;" \
    '^ *0$'

# Same deletion gate as above, on the shape that matters: with the chunk removed, a base table that
# still answers for this window is one the re-encoder never deleted.
# The clustering bound is not decoration: an unrestricted count on a partition whose clustered rows
# have all been deleted still returns 1, for the static-only row (the statics are meant to survive).
cql "DELETE FROM it.tm_tag_point__chunks WHERE tag_id='TAG-001' AND window_start=$WIN_MS;" > /dev/null
check "production-shaped rows are physically gone once their chunk is removed" \
    "SELECT count(*) FROM it.tm_tag_point
      WHERE tag_id='TAG-001' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *0$'
check "...and the statics are still there, because the range delete never reached them" \
    "SELECT site_id, tag_name FROM it.tm_tag_point WHERE tag_id='TAG-001';" \
    'S1 \| +boiler\.temp'

section "TimeSeriesCompactionStrategy: window split and freeze"
# TSCS is otherwise absent from this gate -- every other table here is on UCS or the default -- so a
# regression in window classification, the flush-time split (T3) or the freeze (T2) that only shows
# up through the packaged image would reach a release unseen. docker/cluster-test.sh covers TSCS on
# three replicas, but it is manual in CI and explicitly not a release gate.
#
# Windows are one minute wide and freeze a minute after closing. Every write carries an explicit
# USING TIMESTAMP (microseconds, TSCS's configured resolution) minutes in the past, so which window
# a row lands in is fixed by this script rather than by the wall clock when it runs.
TSCS_W0=$(( (NOW_MS / 60000 - 20) * 60000 ))     # first window start, 20 minutes ago
TSCS_WINDOWS=3
TSCS_FLUSHES=3

cql "
CREATE TABLE IF NOT EXISTS it.tscs (
    tag text, ts timestamp, value double,
    PRIMARY KEY (tag, ts)
) WITH compaction = {'class':'TimeSeriesCompactionStrategy',
                     'timestamp_resolution':'MICROSECONDS',
                     'window_size':'1m','freeze_after':'1m'};
" > /dev/null
sleep 3

check "TSCS is the table's compaction strategy" \
    "SELECT compaction FROM system_schema.tables WHERE keyspace_name='it' AND table_name='tscs';" \
    'TimeSeriesCompactionStrategy'

# Several flushes, each writing into every window, so each closed window really starts out as
# several sstables and the freeze has something to do.
for f in $(seq 0 $((TSCS_FLUSHES - 1))); do
    STMTS=""
    for w in $(seq 0 $((TSCS_WINDOWS - 1))); do
        WS=$(( TSCS_W0 + w * 60000 ))
        TS_MS=$(( WS + f * 1000 ))
        STMTS="$STMTS INSERT INTO it.tscs (tag, ts, value) VALUES ('tag-$f', $TS_MS, $w) USING TIMESTAMP $(( TS_MS * 1000 ));"
    done
    cql "$STMTS" > /dev/null
    ntool flush it tscs > /dev/null
done

sstable_count() { ntool tablestats "it.$1" | grep -E '^[[:space:]]+SSTable count:' | head -1 | tr -dc '0-9'; }

# T3: the flush split at window boundaries, so no sstable spans two windows and there is at least
# one per window. A flush that ignored the boundaries would produce fewer.
TSCS_AFTER_FLUSH=$(sstable_count tscs)
assert "flush split at window boundaries (>= one sstable per window)" \
    "$([ "${TSCS_AFTER_FLUSH:-0}" -ge "$TSCS_WINDOWS" ] && echo ">= $TSCS_WINDOWS" || echo "${TSCS_AFTER_FLUSH:-<none>}")" \
    ">= $TSCS_WINDOWS"

# T2: each closed window compacts down to exactly one sstable. This is background work, so it is
# waited for rather than asserted immediately -- but the wait is bounded, and a node that never
# converges answers every CQL read correctly and simply keeps its disk, which is precisely why it
# needs an assertion of its own.
printf '   waiting for freeze convergence'
TSCS_CONVERGED=0
for _ in $(seq 1 24); do
    TSCS_N=$(sstable_count tscs)
    [ "${TSCS_N:-0}" = "$TSCS_WINDOWS" ] && { TSCS_CONVERGED=1; break; }
    printf '.'; sleep 5
done
echo
assert "every closed window freezes to exactly one sstable" "${TSCS_N:-<none>}" "$TSCS_WINDOWS"

# ...and then stops. A freeze/split alternation rewrites a converged window forever; it is invisible
# to every read, and the only observable is that the sstable set keeps changing.
TSCS_BEFORE=$(ntool tablestats it.tscs | grep -E '^[[:space:]]+(SSTable count|Space used \(live\))')
sleep 20
TSCS_AFTER=$(ntool tablestats it.tscs | grep -E '^[[:space:]]+(SSTable count|Space used \(live\))')
assert "a converged window is not rewritten again (no freeze/split livelock)" \
    "$([ "$TSCS_BEFORE" = "$TSCS_AFTER" ] && echo stable || echo changed)" stable

check "every row survives the freeze" \
    "SELECT count(*) FROM it.tscs;" "^ *$((TSCS_WINDOWS * TSCS_FLUSHES))$"

section "restart: everything above survives a real process boot"
# Every assertion so far ran against a node that has been up the whole time, with schema in memory
# and data possibly still in memtables. The failure class this covers is the one that only appears
# when the process starts again and replays what it wrote -- exactly the shape of the chunk-table
# TCM bug (a committed metadata log entry that could not be parsed back, so the node that wrote it
# could not replay its own log). The jvm-dtests restart an in-JVM instance; only this restarts a
# real process through the image's entrypoint, off a real commit log on disk.
#
# The tiered tables above cannot be reused: both tiering sections end by DELETING their chunk row,
# which is the only way a shell script can prove the re-encoder issued its range delete. So this
# builds its own tiered table and leaves it intact across the restart.
cql "
CREATE TABLE IF NOT EXISTS it.restarted (
    tag_id text, timestamp timestamp, value double,
    PRIMARY KEY (tag_id, timestamp)
);
" > /dev/null
cql "ALTER TABLE it.restarted WITH extensions = {'timeseries_tiering': '$TIERING_JSON'};" > /dev/null
cql "
INSERT INTO it.restarted (tag_id, timestamp, value) VALUES ('pump-09', $(( WIN_MS +  300000 )), 11);
INSERT INTO it.restarted (tag_id, timestamp, value) VALUES ('pump-09', $(( WIN_MS +  900000 )), 22);
INSERT INTO it.restarted (tag_id, timestamp, value) VALUES ('pump-09', $(( WIN_MS + 1500000 )), 33);
" > /dev/null
check_nodetool "a fresh window is re-encoded before the restart" '' retier it restarted
check "its chunk row exists before the restart (samples = 3)" \
    "SELECT samples FROM it.restarted__chunks WHERE tag_id='pump-09' AND window_start=$WIN_MS;" \
    '^ *3'

$RUNTIME restart "$CONTAINER" > /dev/null 2>&1
wait_for_cql

check "the node comes back up and still reports 6.0.0" \
    "SELECT release_version FROM system.local;" '^ *6\.0\.0 *$'
check "the tiering policy survived the restart" \
    "SELECT extensions FROM system_schema.tables WHERE keyspace_name='it' AND table_name='restarted';" \
    'timeseries_tiering'
check "the chunk table survived the restart" \
    "SELECT table_name FROM system_schema.tables WHERE keyspace_name='it' AND table_name='restarted__chunks';" \
    'restarted__chunks'
check "the chunk row survived the restart (samples = 3)" \
    "SELECT samples FROM it.restarted__chunks WHERE tag_id='pump-09' AND window_start=$WIN_MS;" \
    '^ *3'
check "re-encoded rows still read back through the merge after the restart" \
    "SELECT count(*) FROM it.restarted WHERE tag_id='pump-09' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *3$'
check "an aggregate still spans the chunk transparently after the restart" \
    "SELECT avg(value) FROM it.restarted WHERE tag_id='pump-09' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *22'
check "the TSCS table still returns every row after the restart" \
    "SELECT count(*) FROM it.tscs;" "^ *$((TSCS_WINDOWS * TSCS_FLUSHES))$"
# The frozen windows are on disk, not in a memtable: a restart that re-split or re-merged them would
# show up here as a different count. Replaying already-flushed data must add nothing.
assert "frozen sstables come back unchanged (still one per window)" "$(sstable_count tscs)" "$TSCS_WINDOWS"
check "the SAI index still answers LIKE after the restart" \
    "SELECT count(*) FROM it.logs WHERE device='pump-01' AND msg LIKE '%timeout%' ALLOW FILTERING;" \
    '^ *1$'

echo
echo "== $PASS passed, $FAIL failed =="
write_report
[ "$FAIL" -eq 0 ] || exit 1
