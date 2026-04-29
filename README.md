# Polymarket Signal Pipeline

Production-oriented research pipeline for sourcing, filtering, ranking, and summarizing Polymarket markets and events.

This repository turns a notebook-driven workflow into a runnable Python pipeline that:

- fetches active/open events and markets from Polymarket
- applies deterministic business-relevance filtering by topic
- builds historical price and traded-volume datasets
- computes daily market signals and market-level ranking scores
- uses a staged LLM workflow to pick the most relevant markets and events
- exports analyst-friendly Excel outputs

## Why This Exists

Polymarket contains a large and noisy event universe. This project narrows that universe into a business-relevant monitoring system with explicit controls for:

- topic-specific event relevance
- market signal ranking
- AI batch sizing and shortlist caps
- executive-summary hallucination reduction
- historical price and trade-volume export

The design is intentionally hybrid:

- Python handles normalization, filtering, ranking, and export
- the LLM handles selection refinement and commentary generation

## Core Workflow

```mermaid
flowchart TD
    A["Fetch Gamma events by topic tag"] --> B["Normalize events, markets, and outcome tokens"]
    B --> C["Filter to active/open universe"]
    C --> D["Apply multi-topic deterministic event filter"]
    D --> E["Build selected market universe"]
    E --> F["Fetch token price history from CLOB"]
    E --> G["Fetch trade history from Data API"]
    F --> H["Build daily price features"]
    G --> I["Build daily volume history and reconciliation"]
    H --> J["Rank markets by deterministic signal score"]
    I --> J
    J --> K["Build per-topic AI payloads"]
    K --> L["Stage 1: AI picks markets/events"]
    L --> M["Stage 2: AI writes commentary only for shortlisted items"]
    M --> N["Export Excel outputs"]
```

## Repository Layout

```text
.
├── README.md
├── requirements.txt
├── polymarket_pipeline.py
├── polymarket_pipeline.txt
├── polymarket_refined.ipynb
├── polymarket_llm_pipeline.py
├── ai_topic_config.json
├── event_filter_config.json
├── event_filter_config.txt
├── export_notebook_to_script.py
└── update_polymarket_notebook.py
```

## Main Entry Point

The primary runnable file is:

- [polymarket_pipeline.py](./polymarket_pipeline.py)

The main orchestration function is:

- `run_pipeline(...)`

The script entry point is:

- `main()`

By default, the pipeline exports outputs to:

- `polymarket_output/`

unless `POLYMARKET_OUTPUT_DIR` is set.

## Features

### 1. Multi-topic event universe construction

The pipeline fetches from default topic tags:

- Politics
- Finance
- Crypto
- Tech
- Geopolitics
- Economy

These are defined in `DEFAULT_TAGS_DF`.

### 2. Deterministic event filtering

Before any AI call, the code filters events using:

- keyword relevance
- event label/tag relevance
- liquidity thresholds
- volume thresholds
- topic-specific configs

Configuration file:

- [event_filter_config.json](./event_filter_config.json)

### 3. Historical price pipeline

The pipeline fetches token history from Polymarket CLOB and constructs:

- raw price history
- daily normalized price features
- rolling z-scores
- momentum and regime-shift indicators

### 4. Historical trade-volume pipeline

The pipeline fetches market trade history using `conditionId` and builds:

- daily trade counts
- token volume
- notional volume
- average and last trade prices
- reconciliation vs snapshot `volume` and `volume24hr`

Important note:

- extremely active markets may be truncated because the public trades API enforces deep pagination limits
- truncated trade histories are explicitly flagged in the pipeline logs and aggregated output

### 5. Deterministic market ranking

Markets are ranked before AI using a composite score built from:

- z-score strength
- z-score shift count
- momentum
- liquidity
- recent volume
- total volume
- time-to-expiry weighting

The final market ranking score is `moving_market_score`.

### 6. Two-stage AI workflow

To reduce hallucination, the LLM does not select and summarize in one pass.

Instead it runs in two stages:

1. shortlist markets and events from structured payloads
2. generate commentary only for the shortlisted items

This staged design is one of the main controls for executive-summary quality.

### 7. Rich Excel exports

The pipeline exports analyst-facing workbooks for:

- AI market picks
- AI event picks
- price history
- daily price + volume history
- volume history
- volume reconciliation
- filtered markets
- selected events
- event tags
- market coverage summaries
- missing-date diagnostics

## Installation

### Python version

Recommended:

- Python 3.9+

### Install dependencies

```bash
pip install -r requirements.txt
```

## Configuration

### Optional proxy settings

If running inside an environment that requires the UBS proxy:

```bash
export UBS_TNUMBER="..."
export UBS_INET_PASSWORD="..."
```

If proxy connectivity fails, the pipeline falls back to direct internet automatically.

### Azure OpenAI settings

AI stages require:

```bash
export AZURE_OPENAI_ENDPOINT="..."
export OPENAI_API_VERSION="..."
export AZURE_OPENAI_DEPLOYMENT="gpt-5.2"
```

Without these values, the deterministic pipeline still runs, but AI selection/commentary will not.

### Topic-level AI controls

Per-topic AI batching and shortlist caps live in:

- [ai_topic_config.json](./ai_topic_config.json)

Examples of controls:

- `market_top_n`
- `event_top_n`
- `top_k_markets`
- `event_batch_size`
- `max_markets_per_batch`
- `final_market_cap`
- `final_event_cap`

### Event filter controls

Per-topic deterministic event filtering lives in:

- [event_filter_config.json](./event_filter_config.json)

Examples of controls:

- `keywords`
- `min_volume`
- `min_liquidity`
- `min_keyword_hits`
- `top_n`

## Usage

### Run from CLI

```bash
python polymarket_pipeline.py
```

### Run with a custom output folder

```bash
POLYMARKET_OUTPUT_DIR=/path/to/output python polymarket_pipeline.py
```

### Run as a module

```python
from pathlib import Path
from polymarket_pipeline import run_pipeline

results = run_pipeline(
    export=True,
    output_dir=Path("./polymarket_output"),
)
```

## Outputs

Typical exported files include:

- `AI Market Pick 10.xlsx`
- `AI Event Pick 10.xlsx`
- `Price History.xlsx`
- `Daily Price Volume History.xlsx`
- `Volume History.xlsx`
- `Volume Reconciliation.xlsx`
- `Market Coverage Summary.xlsx`
- `Market Missing Dates.xlsx`
- `Filtered Markets.xlsx`
- `Events.xlsx`
- `Events Tags.xlsx`

## Logging and Progress

The pipeline prints progress markers for major stages, including:

- `[pipeline]`
- `[fetch]`
- `[normalize]`
- `[universe-topic]`
- `[universe]`
- `[history]`
- `[history-volume]`
- `[ranking]`
- `[ai-payload]`
- `[ai-start]`
- `[ai-shortlist]`
- `[ai-stage2-market]`
- `[ai-stage2-event]`
- `[export]`

These are useful for debugging candidate counts, topic shrinkage, and API/export failures.

## Known Limitations

### Public trades API pagination

The public Polymarket trades endpoint can cap historical activity pagination for very active markets.

Current behavior:

- the pipeline continues running
- affected markets are flagged as truncated
- trade-derived historical volume for those markets should be treated as partial

### Timezone handling

Polymarket APIs return UTC timestamps.

Current behavior:

- timestamps are converted to `US/Eastern` for daily analytics
- Excel exports strip timezone awareness because Excel does not support timezone-aware datetimes

### Notebook lineage

This project started as a notebook workflow and was progressively refactored into a script.

That means:

- the main runtime path is productionized
- some helper organization still reflects notebook evolution

## Development Notes

If you are extending the repo, the best places to start are:

- `run_pipeline(...)` for top-level orchestration
- `build_history_and_feature_data(...)` for historical data logic
- `build_ranked_markets(...)` for signal engineering
- `build_ai_payloads(...)` for topic payload construction
- `run_ai_for_tag_batched(...)` for staged AI selection/commentary
- `export_pipeline_outputs(...)` for final artifacts

## Recommended Next Improvements

- split the monolithic script into modules such as `fetch.py`, `signals.py`, `ai.py`, and `export.py`
- add unit tests around filtering, ranking, and export formatting
- add a `Makefile` or task runner for repeatable local runs
- add CI checks for linting and script compilation
- add a lightweight schema contract for AI outputs
- add a sample `.env.example`

## Status

Current status:

- actively evolving research/analytics pipeline
- suitable for internal research and analyst workflows
- not yet packaged as a library

## License

No license file has been added yet. If you want this repository to be shareable beyond personal/internal use, add an explicit license.
