# Claude Code Project Memory

This file is the shared project memory for Claude Code sessions working in this repository.

Use it to understand the project quickly before reviewing or changing code.

## Project Goal

This repository contains a Polymarket research pipeline for:

- fetching active/open Polymarket events and markets
- filtering the raw universe to business-relevant topics
- building historical price and trade-derived volume datasets
- ranking markets using deterministic signal logic
- using staged AI passes to pick the most relevant markets and events
- exporting analyst-friendly Excel outputs

The intended audience is an internal research / strategy workflow rather than a generic open-source package.

## Current Primary Files

- `polymarket_pipeline.py`
  - main production script
  - default export behavior
  - deterministic filtering, history, ranking, AI, and export flow
- `polymarket_pipeline_no_volume.py`
  - preferred runner when trade-volume outputs are empty or unreliable
  - skips the trade-history API entirely
- `ai_topic_config.json`
  - per-topic AI batch and shortlist settings
- `event_filter_config.json`
  - deterministic event-filter configuration
- `README.md`
  - public-facing project overview and backlog

## Important Project History

This code started as a Jupyter notebook and was progressively converted into a script.

The current repository reflects multiple refinement passes with the user. The code has notebook lineage, but the main execution path is now structured around `run_pipeline(...)`.

Key changes already made:

- converted the notebook workflow into a runnable Python pipeline
- enforced active/open event filtering
- removed the old legacy AI event-filter stage and replaced it with deterministic multi-topic filtering
- added historical trade-volume support
- added market ranking before AI selection
- changed the AI flow to a two-stage process:
  - pick markets/events first
  - generate commentary only for shortlisted items
- added retry logic for incomplete AI commentary rows
- added a no-volume pipeline variant because the trade-volume API has been unreliable / empty in some runs

## User Preferences and Non-Negotiables

These are important because they came directly from the user over multiple iterations.

### Preserve original structure where possible

- keep original columns when adjusting or extending the code
- avoid removing prompt content from the original notebook prompt
- if behavior changes are needed, make them additive or explicit

### Hallucination reduction matters a lot

The user specifically wanted fewer hallucinations in executive summaries.

Preferred design:

- deterministic filtering first
- deterministic ranking second
- AI shortlist pass third
- commentary only on shortlisted items

Avoid reintroducing a broad one-shot “pick + summarize everything” pattern.

### Business relevance is more important than broad coverage

The event universe should prioritize materiality and topic relevance rather than processing all possible events.

The deterministic prefilter should remain an important control layer.

### Active/open only

The user explicitly wanted active and open events only.

### Current practical preference

Because volume outputs have often been empty or limited by the public trades API, the user asked for a version that runs without volume.

When in doubt:

- prefer the no-volume path for operational stability
- treat the volume path as optional / best-effort

## Review Guidance

If asked to review this repo, focus on these areas first:

### 1. Data correctness

- event / market normalization
- preservation of `conditionId`
- token / market mapping correctness
- daily timestamp handling
- source tag behavior and payload grouping

### 2. Volume-history realism

Known limitations:

- the public trades API can return empty coverage
- some markets hit a pagination cap around deep historical offsets
- trade-derived volume should not be assumed to perfectly reconcile to Polymarket’s displayed snapshot totals

If reviewing volume logic, treat it as an approximation layer with explicit known limitations.

### 3. AI pipeline integrity

Focus on:

- whether stage 1 and stage 2 stay properly separated
- whether dynamic per-topic caps are honored
- whether deterministic signal ordering is preserved after AI selection
- whether partial AI rows can leak into exports

### 4. Export reliability

Known past issues:

- timezone-aware datetimes caused Excel export failures
- empty volume outputs created noisy workbooks

Review export behavior with these failure modes in mind.

## Known Limitations

### Public trades API limitations

- deep pagination can fail for very active markets
- some runs return empty volume history
- volume outputs may be partial or absent even when price history is populated

### Source-tag partitioning

Per-topic AI payloads are grouped by source tag context, not full thematic overlap.

This means:

- some economically relevant markets may show up under Finance or Politics instead of Economy
- candidate counts can look artificially small for a topic even if the broader universe contains related markets

### Script organization

`polymarket_pipeline.py` is still monolithic.

If proposing refactors, prefer incremental modularization instead of a risky rewrite.

## Operational Recommendations

### Recommended stable run

If the goal is dependable output rather than volume diagnostics:

```bash
python polymarket_pipeline_no_volume.py
```

### Full run

If the goal is to attempt price + volume + reconciliation:

```bash
python polymarket_pipeline.py
```

### Export behavior

- exports are on by default
- default output directory is `polymarket_output/`
- the no-volume script writes to `polymarket_output/no_volume/`

## Files a reviewer should inspect first

If doing a serious review, start here:

1. `README.md`
2. `polymarket_pipeline.py`
3. `ai_topic_config.json`
4. `event_filter_config.json`
5. `polymarket_pipeline_no_volume.py`

## Good Next Steps for Future Work

The most useful next improvements are:

- modularize the large script into logical files
- add tests for filtering, ranking, and export behavior
- add explicit flags in outputs for truncated or partial trade histories
- support thematic inheritance across topic tags
- add a proper `.env.example`
- add CI checks

## Instructions for Future Claude Code Sessions

When helping on this repo:

- preserve the staged AI design unless the user explicitly wants a redesign
- avoid removing user prompt content
- avoid deleting columns from existing outputs without a clear reason
- be careful with volume-history claims and describe them precisely
- prefer additive changes that improve reliability and transparency
- if recommending the volume path, clearly state its current limitations
