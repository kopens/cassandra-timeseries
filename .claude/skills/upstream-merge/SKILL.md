---
name: upstream-merge
description: Merge the latest apache/cassandra `cassandra-6.0` upstream into this fork's `main`, resolve the recurring conflicts, build, and run the fork's test suite. Use this whenever the user asks to sync, update, catch up, rebase, or merge with upstream Cassandra — including phrasings like "업스트림 반영하자", "apache/cassandra 최신거 가져와", "cassandra-6.0 머지", or when they paste a github.com/apache/cassandra link and ask to pull it in. Also use it when a merge from upstream is already half-done and conflicts need resolving.
---

# Merging upstream apache/cassandra into this fork

This fork tracks `apache/cassandra`'s `cassandra-6.0` branch on `main`. The merge itself is
routine; what makes it error-prone is that a handful of files conflict *every single time* in
the same way, and that the repo's own build wrapper reports success on this host without
building anything. Both are covered below.

Work directly on `main` unless the user asks otherwise — that is where the tracking rule in
`CLAUDE.md` says upstream must land.

## 1. Fetch upstream

The `upstream` remote is not always configured (a fresh clone won't have it):

```bash
git remote get-url upstream 2>/dev/null || git remote add upstream https://github.com/apache/cassandra.git
git fetch upstream cassandra-6.0
```

Then show what is actually new before merging, so you and the user know the size of the change:

```bash
git log --oneline --no-merges main..upstream/cassandra-6.0
git rev-list --count main..upstream/cassandra-6.0
```

If the count is 0, say so and stop — there is nothing to do.

## 2. Merge

```bash
git merge upstream/cassandra-6.0 --no-edit
```

Expect conflicts. Resolve them with the playbook below, then `git add` each file. Do **not**
commit until the build in step 4 passes — a broken merge commit on `main` is much more annoying
to undo than an uncommitted one.

## 3. Conflict playbook

`CLAUDE.md` lists the recurring conflict spots; this is how each one is actually resolved.

**`README.asc`** — deleted in this fork so GitHub renders `README.md` (the English README;
`README.ko.md` is the Korean one, and a change to either belongs in both). Upstream keeps editing
`README.asc`, producing a modify/delete conflict. Delete it again: `git rm README.asc`.

**`build.xml`** — upstream resets `base.version` to an alpha/beta (e.g. `6.0-alpha3`). The fork's
versioning rule requires `6.0.0`, because the build must produce `build/apache-cassandra-6.0.0.jar`
and the release pipeline renames from that exact path. Keep `<property name="base.version" value="6.0.0"/>`.

**`CHANGES.txt`** — conflicts every time. The fork replaced upstream's top version header with
`6.0.0` and lists its own entries under it. There are two cases, and they resolve differently — check
which one you are in *before* editing, by comparing the top version header of the merge base with
upstream's:

```bash
git show $(git merge-base HEAD MERGE_HEAD):CHANGES.txt | head -1
git show MERGE_HEAD:CHANGES.txt | grep -n '^6\.0'
```

*Same top header on both sides* (upstream appended bullets to the section the fork renamed — the
common case, e.g. the 2026-09-05 merge): there is no displaced header. Just drop the `6.0-alpha3`
line from upstream's side and let its new bullets follow the fork's under the `6.0.0` header.

*Upstream opened a new version section*: keep the `6.0.0` header and the fork's entries, append
upstream's new entries (with their `Merged from 5.0:` / `Merged from 4.0:` sub-headers intact) below
them, and **restore the version header of the section that upstream's new one displaced** —
otherwise the older release's entries end up looking like they belong to a `Merged from 4.0:` block.
Concretely, the shape you want is:

```
6.0.0
 * <fork entries, newest first>
 * <upstream's newest-section entries>
Merged from 5.0:
 ...
Merged from 4.0:
 ...


6.0-alpha2          <- header restored, was the top of upstream's previous section
 * <older entries, untouched>
```

**`conf/cassandra.yaml` + `conf/cassandra_latest.yaml`** — conflicts most times, and the two files
conflict identically, so resolve them the same way. The fork's commented-out
`prepared_statements_require_parameters_*` guardrail block sits at the very end of the guardrails
section — exactly where upstream appends each new guardrail. Keep both blocks, fork's first, with a
bare `#` line between them (the separator upstream uses between guardrail entries); do not take a
side.

**`ModificationStatement.java`** — the fork's `validatePrepare(ClientState)` override (the
`preparedStatementsRequireParameters` guardrail) sits immediately before the disk-usage code, which
upstream keeps refactoring. Keep both. Watch the brace: the conflict cuts through the fork's method,
so `<<<<<<<` holds its body without a closing `}` and the `}` after `>>>>>>>` belongs to upstream's
method — the resolved text needs a `}` added to close `validatePrepare`.

**`.build/sh/ant-log-summary.py`** — add/add conflict. The fork carried this script before upstream
shipped its own (`c574262b10`, "Add --summary and --clean to the build scripts"); the files are otherwise identical, and upstream's
only delta is exiting 2 rather than 1 when the log file cannot be read, which is the better
behaviour (1 means "the log says BUILD FAILED"). Take theirs. Note that this does **not** fix the
false-green trap in step 4: upstream's version also prints `BUILD SUCCESSFUL` and exits 0 on empty
input.

**`debian/changelog`** — keep the fork's `cassandra (6.0.0) unstable; urgency=medium` stanza and
drop upstream's alpha stanza. One gotcha: this file contains a *pre-existing* stray
`>>>>>>> cassandra-5.0` line (around line 72) that upstream committed years ago. It is not your
conflict — leave it. Verify with `git show upstream/cassandra-6.0:debian/changelog | grep -n '>>>>>>>'`
before touching anything that looks like a leftover marker.

**`modules/accord`** — a submodule pointer. Compare the three sides explicitly rather than guessing:

```bash
git rev-parse HEAD:modules/accord :modules/accord upstream/cassandra-6.0:modules/accord
```

The fork does not carry its own Accord changes, so take upstream's pointer when they differ, then
`git submodule update --init --recursive`.

**Fork feature touchpoints** — `SelectStatement.java` (gap-fill wiring), `db/compaction/TimeSeries*.java`,
`FreezeCompactionTask.java`, `db/compaction/timeseries/`, `TableAttributes.java`, `CassandraDaemon.java`,
`SystemViewsKeyspace.java`, `NodetoolCommand.java`, `NodeProbe.java`, `DataResolver.java`/`DigestResolver.java`,
`ColumnFamilyStore.java`/`ColumnFamilyStoreMBean.java`. These usually auto-merge. When they don't, the
rule is *keep both*: upstream's fix and the fork's feature. Never resolve one of these by taking a
side wholesale without reading the hunk.

**Upstream-only files with a stale fork delta** — occasionally the fork carries a one-line tweak to a
pure-upstream file (a test, usually) that upstream has since restructured out of existence. Check
whether the fork's delta is something *we* authored or an upstream commit that never made it onto
`cassandra-6.0`:

```bash
git log --oneline -3 HEAD -- <path>          # who changed it, and why
git branch -r --contains <that-sha>          # only fork refs => upstream doesn't have it
```

If the file holds no fork feature and upstream has rewritten it, take theirs
(`git checkout --theirs <path>`) and say so in the summary. Don't preserve a delta whose reason no
longer exists.

Before moving on, confirm nothing is left unresolved:

```bash
git diff --name-only --diff-filter=U
git grep -n '^<<<<<<< \|^>>>>>>> upstream'
```

## 4. Build — and why you cannot use `.build/sh/ai-build` here

**This host has no `ant` installed.** `.build/sh/ai-build` pipes ant's output through
`.build/sh/ant-log-summary.py`, and that script prints `BUILD SUCCESSFUL` and exits 0 when its input
is empty. So a missing `ant` produces a completely convincing green build that compiled nothing.
The same trap applies to `ai-ci-test`, which pipes through the same summarizer.

Never treat `BUILD SUCCESSFUL` from those wrappers as evidence on its own. Build in a container
instead, using the same image CI uses:

```bash
.build/sh/ai-build-image          # idempotent; ~1 min the first time
.build/sh/ai-in-container 'ant -Dant.gen-doc.skip=true -Drat.skip=true clean jar checkstyle checkstyle-test'
```

`--network host` is required for both the image build and the runs — Docker's default bridge has no
working DNS on this host, so `apt-get` inside a plain `docker build` fails to resolve
`archive.ubuntu.com`. Both scripts already pass it.

To run the whole release gate rather than just the build, use `.build/sh/ci-local`, which walks
`.gitlab-ci.yml`'s stages in order (jar → tests → image → integration test) on this machine. That is
worth doing after an upstream merge, and right now it is the *only* thing that verifies one: the
project's GitLab runners have been offline since at least 2026-08-07, so every pipeline fails with
`stuck_pending_no_matching_runners` before a job starts and a red pipeline says nothing about the
code (`glab ci get -p <id>` shows it).

Confirm the build really happened, by name *and* by timestamp:

```bash
ls -la --time-style=full-iso build/apache-cassandra-6.0.0.jar
```

A jar dated before today means it did not rebuild, whatever the log said.

**A dependency bump needs `realclean`, not `clean`.** `lib/` is `${build.lib}` — a gitignored
download cache, not checked-in jars — and a plain `clean` leaves it alone. So when upstream bumps a
dependency, the *old* jar stays in `lib/` and shadows the newly resolved one in `build/lib/jars`,
and the build fails on an API that plainly exists in the version the pom asks for. The 2026-09-05
merge hit this: CASSANDRA-21474 moved `NoSpamLogger` to Caffeine and bumped 3.1.8 → 3.2.4, and the
stale `lib/caffeine-3.1.8.jar` produced `error: cannot find symbol ... method writing(...) location:
interface Expiry`. Diagnose by comparing the pom with what is on disk, and fix by clearing the
cache — never by editing fork code to the old API:

```bash
grep -A2 '<artifactId>caffeine</artifactId>' .build/parent-maven-pom.xml   # version the pom wants
ls lib/ | grep -i caffeine                                                 # version actually there
.build/sh/ai-in-container 'ant -Dant.gen-doc.skip=true -Drat.skip=true realclean && ant -Dant.gen-doc.skip=true -Drat.skip=true jar checkstyle checkstyle-test'
```

To spot it before the build, diff the dependency versions the merge brought in:

```bash
git diff $(git merge-base HEAD MERGE_HEAD)...MERGE_HEAD -- .build/parent-maven-pom.xml | grep -E '^[-+].*<version>'
```

Compile failures after an upstream merge are usually upstream tightening a signature that fork code
calls. Read the actual error rather than the summary:

```bash
.build/sh/ai-in-container 'ant -Dant.gen-doc.skip=true -Drat.skip=true build-test 2>&1 | grep -E "error:|error\]"'
```

Fix fork-side callers to match upstream's new contract, and leave a short comment naming the
upstream ticket so the next reader knows why the shim exists.

## 5. Test

Run the fork's own CI suite — it is the set of classes that actually cover the fork delta:

```bash
.build/sh/ai-in-container '.build/sh/ci-timeseries-tests.sh'
```

It writes one summary line per class and treats "0 tests ran" as a failure, so unlike `ant testsome`
it cannot pass by silently running nothing. It takes a while; start it in the background and do
other work while it runs.

Run these **one at a time**. `ci-test` starts with a `realclean`, so a second run started
concurrently deletes `build/lib/jars` out from under the first, and the victim fails with hundreds
of "package org.slf4j does not exist" errors that look like a broken change and are not. Put the
loop *inside* one container invocation rather than starting a container per class.

For a single class, bypass the summarizer and read the JUnit line yourself:

```bash
.build/sh/ai-in-container '.build/sh/ci-test <FQCN> 2>&1 | grep -E "Tests run:|BUILD "'
```

Report results with the actual `Tests run: N, Failures: 0, Errors: 0` lines. If something fails,
say which class and show the output — do not summarize a failure into a pass.

## 6. Commit

Only after the build and tests are green:

```bash
git commit --no-edit    # keep git's generated merge message, then amend if the user wants notes
```

If any conflict was resolved in a way that changed behaviour (taking upstream's version of a file
the fork had touched, adding a compile shim), mention it in the commit body and in your reply — that
is the part the user cannot see from the diff stat.

## Reporting back

Tell the user: how many upstream commits landed, which files conflicted and how each was resolved,
anything that needed a code fix to compile, and the test results. Keep it to a short list — they are
syncing, not reading a report.
