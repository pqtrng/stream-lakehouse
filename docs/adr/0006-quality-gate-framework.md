# ADR 0006 — Quality-gate framework: Great Expectations

**Status:** accepted · **Date:** 2026-08-13 · **Sprint:** 0

## Context

Sprint 3.3 gates Bronze → Silver promotion on validation suites, and four places in
`README.md` referenced a framework that was still undecided ("ADR 0.6"). The candidates
named in the README are Great Expectations and Pandera, and the axis they actually differ
on is not validation — both stop a bad batch — but what remains after a run finishes.
Pandera validates in-process and leaves nothing behind but an exit code. Great
Expectations writes a structured validation result per run — which batch, which
expectation passed or failed, and the observed value even on passing runs — into a result
store, and renders those results as Data Docs, a static HTML site readable by someone who
was not there when the run happened.

The README's honesty rules apply: the gate's pass claim must be reproducible from a clean
clone, and inference must be labelled as inference.

## Decision

Great Expectations gates Silver promotion.

## Why

Both candidates stop a bad batch; the decision rests on what survives the run. Great
Expectations persists a structured validation result per run — batch identity,
per-expectation outcome, and the observed value even when the check passes — and renders
that history as Data Docs. Two consequences drive the choice:

1. **Drift is visible before the first failure.** A sequence of passing observed values
   (null rate 0.2% → 0.4% → 0.8%) shows quality degrading while every run is still green.
   Pandera computes the same values and discards them with the Spark job that produced
   them; Airflow task state records one bit per run and cannot answer which expectation
   moved, or by how much.
2. **The pass claim carries evidence, not trust.** The README promises measured,
   reproducible claims. A validation-result store is the quality gate's equivalent of a
   Grafana panel: proof a reader can open, rather than an exit code they must believe.

## What this trades away

- **Pandera's in-process simplicity.** A Pandera schema is ~10 lines of Python inside the
  Spark job: no data context, no datasource, no checkpoint, no store backend to configure,
  learn, and operate. That entire config surface is the price of the artefact, not of
  validation.
- **A free CI story.** GE's results land in `gx/uncommitted/` by default, which is empty
  on a clean clone — the artefact needs a home (MinIO result store, or CI-published
  GitHub Pages) and a lifecycle. Where Data Docs get published is deliberately **not**
  decided here; it is a Sprint 3/7 deliverable decision.
- **A documentation-thin path.** GE 1.x validating Spark DataFrames is far less travelled
  than the pandas path (inference from GE docs and changelog, not yet verified against a
  running job). This risk is accepted and mitigated by the spike named below.

## What it unblocks

- Sprint 3.3 — quality-gate suites gating Silver promotion.
- The four README placeholders that previously read "ADR 0.6": the `QG` node in the architecture
  diagram, the quality-gate row in the tech-stack table, the Sprint 3.3 bullet, and the
  `quality/` line in the repository layout.
- The Measured-results row *Data quality gate (Silver) — 100% suites passing in CI*.

## How it gets verified

1. **Spike before the first suite in Sprint 3.3:** GE 1.x validates a Spark DataFrame end
   to end — define an expectation, run a checkpoint, produce a validation result — against
   a Bronze fixture. If the spike fails, this ADR is reopened *before* any suite is
   written, not after.
2. **CI:** suites run on a committed HAI fixture (a few thousand rows). The
   Measured-results row is satisfied by suite pass in CI, with the scope stated: fixture
   size and commit.
3. **Any published Data Docs page states its own scope** — dataset, row count, commit. A
   green page without that caption overstates what was verified and violates the README's
   honesty rules.

## Alternatives considered

- **Pandera** — wins on every operational axis: in-process, no extra service, schemas are
  plain Python reviewable in a PR. Lost because it leaves no run-level artefact: no
  history of observed values, so no drift visibility, and no evidence of a pass beyond an
  exit code.
- **Hybrid (Pandera validates, GE renders docs)** — rejected without scoring: it violates
  the README non-goal "No both-of-everything."
