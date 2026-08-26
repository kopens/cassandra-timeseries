#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Runs the fork's targeted test classes for CI, one line of output each.
#
# Why this exists rather than a list of `ant` lines in .gitlab-ci.yml:
#
#   1. jvm-dtests log at DEBUG to stdout. A single one can emit megabytes, and GitLab
#      stops collecting a job's trace at 4 MiB -- in the 6.0.0 release run the limit was
#      hit during the SECOND class, so had anything failed afterwards the job would have
#      reported failure with no visible cause. Full output now goes to a per-class file
#      under build/ci-logs/ (kept as a job artifact); only a summary line reaches the trace.
#
#   2. `ant testsome` exits 0 when the class name does not resolve -- it reports
#      BUILD SUCCESSFUL having run nothing. An exit code therefore does not mean the tests
#      ran. This reads failures/errors/tests out of the JUnit XML and treats tests=0 as a
#      failure, so a typo or a silently-skipped class cannot pass CI.
set -o errexit -o nounset -o pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOGS=build/ci-logs
mkdir -p "$LOGS"

# target|class
TESTS=(
  "testsome|org.apache.cassandra.db.aggregation.TimeBucketGapFillerTest"
  "testsome|org.apache.cassandra.cql3.functions.TimeSeriesFctsTest"
  "testsome|org.apache.cassandra.index.sai.analyzer.LuceneTokenizingAnalyzerTest"
  "testsome|org.apache.cassandra.index.sai.cql.AnalyzedLikeQueryTest"
  "testsome|org.apache.cassandra.index.sai.cql.AllowFilteringTest"
  "test-jvm-dtest-some|org.apache.cassandra.distributed.test.sai.AnalyzedLikeDistributedTest"
  "testsome|org.apache.cassandra.db.timeseries.ChunkCodecsTest"
  "testsome|org.apache.cassandra.db.timeseries.ColumnarChunkCodecTest"
  "testsome|org.apache.cassandra.db.timeseries.tiering.TieringPolicyTest"
  "testsome|org.apache.cassandra.db.timeseries.tiering.TieredStorageServiceTest"
  "testsome|org.apache.cassandra.db.timeseries.tiering.TieringSchemaSupportTest"
  "testsome|org.apache.cassandra.db.timeseries.tiering.TieredStorageColumnsTest"
  "testsome|org.apache.cassandra.db.timeseries.tiering.ChunkReadSupportTest"
  "testsome|org.apache.cassandra.db.timeseries.tiering.ChunkMergeIteratorTest"
  "testsome|org.apache.cassandra.db.timeseries.tiering.TransparentReadTest"
  "testsome|org.apache.cassandra.tools.nodetool.mock.TieredStorageMockTest"
  "testsome|org.apache.cassandra.cql3.validation.operations.AlterTableExtensionsTest"
  "testsome|org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategyOptionsTest"
  "testsome|org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategyTest"
  "testsome|org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategyE2ETest"
  "testsome|org.apache.cassandra.db.compaction.timeseries.WindowFrozenListenersTest"
  "testsome|org.apache.cassandra.db.compaction.timeseries.WindowRoutingIteratorTest"
  "testsome|org.apache.cassandra.db.compaction.timeseries.TimeWindowSplittingMultiWriterTest"
  "test-jvm-dtest-some|org.apache.cassandra.distributed.test.timeseries.ChunkTableSchemaPropagationTest"
  "test-jvm-dtest-some|org.apache.cassandra.distributed.test.timeseries.TieredStorageDistributedTest"
  "test-jvm-dtest-some|org.apache.cassandra.distributed.test.timeseries.TimeSeriesCompactionDistributedTest"
  "testsome|org.apache.cassandra.db.memtable.TimeSeriesMemtableSchemaSupportTest"
  "testsome|org.apache.cassandra.db.memtable.TimeSeriesMemtableDifferentialTest"
  "testsome|org.apache.cassandra.db.memtable.TimeSeriesMemtableFlushTest"
  "testsome|org.apache.cassandra.db.memtable.TimeSeriesMemtableColumnarTest"
  "testsome|org.apache.cassandra.db.memtable.TimeSeriesMemtableHeapTest"
  "testsome|org.apache.cassandra.db.memtable.TimeSeriesMemtableReadPathTest"
  "testsome|org.apache.cassandra.db.memtable.TimeSeriesMemtableOffheapReadPathTest"
  "testsome|org.apache.cassandra.db.memtable.TimeSeriesMemtableStreamingReadTest"
  "testsome|org.apache.cassandra.db.timeseries.tiering.ColdWindowChunkFlushTest"
)

# Guard: a class deleted from the tree stays in TESTS above and then fails every run as a
# ClassNotFoundException -- which reads as a product failure, so the real signal ("this list is
# stale") is buried and the suite silently stays red. That happened: the chunk-format-v3 tests were
# deleted in 74e542db95 and their entries were not. Fail here instead, in the list's own terms,
# before spending a build on it. Same guard `.build/sh/ai-ci-test` applies to its argument.
missing=()
for entry in "${TESTS[@]}"; do
    class="${entry#*|}"
    path="${class//.//}.java"
    compgen -G "test/*/$path" > /dev/null || missing+=("$class")
done
if [ "${#missing[@]}" -gt 0 ]; then
    echo "ERROR: ${#missing[@]} entry(ies) in TESTS name a class that does not exist under test/*/:" >&2
    printf '  %s\n' "${missing[@]}" >&2
    echo "Remove them from this script, or restore the class." >&2
    exit 2
fi

# Reads the JUnit XML rather than trusting ant's exit code. Prints "tests failures errors"
# or "MISSING" when no report was produced at all.
verdict() {
  python3 - "$1" <<'PY'
import glob, re, sys
hits = glob.glob(f"build/test/output/TEST-{sys.argv[1]}.xml") \
    or glob.glob(f"build/test/output/TEST-{sys.argv[1]}-*.xml")
if not hits:
    print("MISSING"); raise SystemExit
t = f = e = 0
for h in hits:
    with open(h, encoding="utf-8", errors="replace") as fh:
        head = fh.read(4000)
    m = re.search(r'<testsuite\b[^>]*', head)
    if not m:
        print("MISSING"); raise SystemExit
    for key, add in (("tests", "t"), ("failures", "f"), ("errors", "e")):
        v = re.search(rf'{key}="(\d+)"', m.group(0))
        n = int(v.group(1)) if v else 0
        if add == "t": t += n
        elif add == "f": f += n
        else: e += n
print(t, f, e)
PY
}

failed=0
printf '%-62s %7s %8s %8s  %s\n' "CLASS" "TESTS" "FAIL" "ERROR" "RESULT"
for entry in "${TESTS[@]}"; do
    target="${entry%%|*}"; class="${entry#*|}"
    short="${class#org.apache.cassandra.}"
    log="$LOGS/${class##*.}.log"
    set +o errexit
    ant "$target" -Dtest.name="$class" > "$log" 2>&1
    set -o errexit
    read -r t f e <<< "$(verdict "$class")" || true
    if [ "$t" = "MISSING" ]; then
        printf '%-62s %7s %8s %8s  %s\n' "$short" "-" "-" "-" "NO REPORT"
        failed=1
    elif [ "$f" != "0" ] || [ "$e" != "0" ] || [ "$t" = "0" ]; then
        # tests=0 counts as failure: ant reports BUILD SUCCESSFUL for a class it never ran.
        printf '%-62s %7s %8s %8s  %s\n' "$short" "$t" "$f" "$e" "FAIL"
        failed=1
    else
        printf '%-62s %7s %8s %8s  %s\n' "$short" "$t" "$f" "$e" "ok"
    fi
done

if [ "$failed" != 0 ]; then
    echo
    echo "Some classes failed. Full output per class is in $LOGS/ (job artifact)."
    exit 1
fi
echo
echo "All ${#TESTS[@]} classes passed. Full logs in $LOGS/ (job artifact)."
