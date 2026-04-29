from __future__ import annotations

import json
import math
import time
from textwrap import dedent
from typing import Any, Iterable

import numpy as np
import pandas as pd


def _to_iso_or_none(value: Any) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, pd.Timestamp):
        ts = value
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.tz_convert("US/Eastern").strftime("%Y-%m-%dT%H:%M:%SZ")
    return value


def _sanitize_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_json(v) for v in value]
    if isinstance(value, tuple):
        return [_sanitize_for_json(v) for v in value]
    return _to_iso_or_none(value)


def _json_ready_df(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    sort_by: str | None = None,
    ascending: bool = False,
    n: int | None = None,
) -> list[dict[str, Any]]:
    x = df.copy()
    if columns is not None:
        keep = [c for c in columns if c in x.columns]
        x = x[keep]
    if sort_by and sort_by in x.columns:
        x = x.sort_values(sort_by, ascending=ascending)
    if n is not None:
        x = x.head(n)
    for c in x.columns:
        if pd.api.types.is_datetime64_any_dtype(x[c]):
            x[c] = x[c].apply(_to_iso_or_none)
    x = x.replace({np.nan: None})
    records = x.to_dict("records")
    return [_sanitize_for_json(record) for record in records]


def _chunk_records(records: list[dict[str, Any]], chunk_size: int) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(records), chunk_size):
        yield records[start : start + chunk_size]


def _extract_json_object(raw_text: str) -> dict[str, Any]:
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object found in model response")
    return json.loads(raw_text[start : end + 1])


def call_llm_json(
    *,
    client: Any,
    model: str,
    prompt: str,
    temperature: float = 0.0,
    max_retries: int = 3,
    sleep_sec: float = 1.0,
    use_json_mode: bool = True,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            kwargs = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": temperature,
            }
            if use_json_mode:
                kwargs["response_format"] = {"type": "json_object"}
            response = client.chat.completions.create(**kwargs)
            raw_text = response.choices[0].message.content
            return _extract_json_object(raw_text)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == max_retries:
                break
            time.sleep(sleep_sec)
    raise RuntimeError(f"LLM JSON call failed after {max_retries} attempts: {last_error}") from last_error


def add_yes_no_prices(markets_df: pd.DataFrame) -> pd.DataFrame:
    x = markets_df.copy()

    def _extract(row: pd.Series) -> pd.Series:
        outcomes = row.get("outcomes")
        prices = row.get("outcomePrices")
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except Exception:  # noqa: BLE001
                outcomes = []
        if isinstance(prices, str):
            try:
                prices = json.loads(prices)
            except Exception:  # noqa: BLE001
                prices = []
        outcomes = outcomes or []
        prices = prices or []
        price_map = dict(zip(outcomes, prices))
        yes_price = price_map.get("Yes")
        no_price = price_map.get("No")
        return pd.Series(
            {
                "Yes Price": pd.to_numeric(yes_price, errors="coerce"),
                "No Price": pd.to_numeric(no_price, errors="coerce"),
            }
        )

    x[["Yes Price", "No Price"]] = x.apply(_extract, axis=1)
    return x


def prepare_market_candidates(ranked_markets: pd.DataFrame) -> pd.DataFrame:
    x = add_yes_no_prices(ranked_markets).copy()
    for col in ["market_startDate", "market_endDate", "event_startDate", "event_endDate"]:
        if col in x.columns:
            x[col] = pd.to_datetime(x[col], utc=True, errors="coerce")
    now = pd.Timestamp.utcnow()
    if "market_startDate" in x.columns:
        x["market_age_days"] = ((now - x["market_startDate"]).dt.total_seconds() / 86400.0).clip(lower=0)
    else:
        x["market_age_days"] = np.nan
    x["volume_per_day"] = x["volume"].fillna(0) / x["market_age_days"].replace(0, np.nan)
    x["liquidity_per_day"] = x["liquidity"].fillna(0) / x["market_age_days"].replace(0, np.nan)
    x["freshness_score"] = (
        np.log1p(x["volume24hr"].fillna(0))
        + np.log1p(x["liquidity"].fillna(0))
        + np.log1p(x["volume_per_day"].fillna(0).clip(lower=0))
    )
    x = x.sort_values(["moving_market_score", "freshness_score"], ascending=False).reset_index(drop=True)
    return x


def build_event_candidates(ranked_markets: pd.DataFrame, top_k_markets: int = 4) -> pd.DataFrame:
    x = prepare_market_candidates(ranked_markets)
    agg = (
        x.groupby("event_slug", as_index=False)
        .agg(
            event_title=("event_title", "first"),
            source_tag_id=("source_tag_id", "first"),
            topic_name=("topic_name", "first"),
            event_description=("event_description", "first"),
            event_startDate=("event_startDate", "first"),
            event_endDate=("event_endDate", "first"),
            event_volume24hr=("volume24hr", "sum"),
            event_volume_total=("volume", "sum"),
            event_liquidity=("liquidity", "sum"),
            market_count=("market_id", "nunique"),
            best_market_score=("moving_market_score", "max"),
            mean_market_score=("moving_market_score", "mean"),
            best_freshness_score=("freshness_score", "max"),
            max_abs_z=("max_abs_z", "max"),
            max_abs_momentum=("max_abs_momentum", "max"),
        )
        .sort_values(["best_market_score", "event_volume24hr"], ascending=False)
    )

    top_market_cols = [
        "event_slug",
        "market_id",
        "question",
        "Yes Price",
        "No Price",
        "moving_market_score",
        "latest_momentum_return",
        "max_abs_z",
        "max_abs_momentum",
        "volume24hr",
        "volume",
        "liquidity",
        "days_to_end",
    ]
    top_market_cols = [c for c in top_market_cols if c in x.columns]
    top_markets = (
        x.sort_values("moving_market_score", ascending=False)
        .groupby("event_slug", as_index=False)
        .head(top_k_markets)[top_market_cols]
    )

    drivers = (
        top_markets.groupby("event_slug")
        .apply(lambda g: g.to_dict("records"))
        .reset_index(name="top_markets_in_event")
    )

    out = agg.merge(drivers, on="event_slug", how="left")
    out["event_candidate_score"] = (
        out["best_market_score"].fillna(0) * 0.55
        + np.log1p(out["event_volume24hr"].fillna(0)) * 0.20
        + np.log1p(out["event_liquidity"].fillna(0)) * 0.15
        + out["market_count"].fillna(0) * 0.10
    )
    return out.sort_values("event_candidate_score", ascending=False).reset_index(drop=True)


def _event_selection_prompt(tag_name: str, batch: list[dict[str, Any]], top_k: int) -> str:
    return dedent(
        f"""
        You are screening Polymarket event candidates for UBS risk monitoring.

        Topic bucket: {tag_name}

        Your task:
        1. Select at most {top_k} events from the provided candidates.
        2. Favor events with strong market evidence, clean UBS transmission, and credible liquidity.
        3. Prefer new or reframed events only when the provided fields show strong early trading or fresh repricing.

        Hard guardrails:
        - Use ONLY the events and market IDs present in the payload.
        - Do NOT invent titles, metrics, or related markets.
        - If evidence is weak, return fewer than {top_k}.
        - Output strict JSON only.

        Return:
        {{
          "selected_events": [
            {{
              "event_slug": "...",
              "event_title": "...",
              "selection_score": 0.0,
              "market_status": "new" | "mature",
              "why_now": "...",
              "ubs_channels": ["Trading"],
              "shock_type": ["macro"],
              "driving_market_ids": ["123", "456"]
            }}
          ]
        }}

        Candidate events JSON:
        {json.dumps({"events": batch}, ensure_ascii=False)}
        """
    ).strip()


def _market_selection_prompt(tag_name: str, batch: list[dict[str, Any]], top_k: int) -> str:
    return dedent(
        f"""
        You are screening Polymarket markets for UBS early-warning monitoring.

        Topic bucket: {tag_name}

        Select at most {top_k} markets that are:
        - strong enough to matter now,
        - liquid enough to be credible,
        - clearly tied to a UBS transmission path.

        Hard guardrails:
        - Use ONLY the market_id, question, and metrics in the payload.
        - Do NOT infer catalysts or percentages not shown.
        - Favor freshness only when the early trading evidence in the payload supports it.
        - Output strict JSON only.

        Return:
        {{
          "selected_markets": [
            {{
              "market_id": "...",
              "question": "...",
              "selection_score": 0.0,
              "event_status": "new" | "existing",
              "event_emergence_type": "new_risk" | "reframed_risk" | "escalation" | "existing",
              "why_now": "...",
              "ubs_channels": ["Trading"],
              "shock_type": ["macro"]
            }}
          ]
        }}

        Candidate markets JSON:
        {json.dumps({"markets": batch}, ensure_ascii=False)}
        """
    ).strip()


def _event_summary_prompt(tag_name: str, batch: list[dict[str, Any]]) -> str:
    return dedent(
        f"""
        You are writing concise executive event summaries for UBS senior risk readers.

        Topic bucket: {tag_name}

        Hard guardrails:
        - Use ONLY the provided event payload and the explicitly listed top markets.
        - Never mention a market name, percentage, or change that is not in the payload.
        - If a metric is missing, omit it instead of guessing.
        - Keep each executive summary to one sentence.
        - Output strict JSON only.

        Return:
        {{
          "events": [
            {{
              "event_slug": "...",
              "executive_summary_sentence": "...",
              "top_markets_driving_signal": ["..."],
              "signal_summary": "...",
              "transmission_path": "...",
              "ubs_channels": ["..."],
              "confidence_score": 0.0,
              "risk_of_false_signal": "low" | "medium" | "high",
              "confirmation_strength": "single" | "partial" | "strong",
              "time_horizon": "immediate" | "near-term" | "medium-term",
              "shock_type": ["..."],
              "recommended_actions": ["...", "..."],
              "follow_ups": ["...", "..."]
            }}
          ]
        }}

        Event payload JSON:
        {json.dumps({"events": batch}, ensure_ascii=False)}
        """
    ).strip()


def _market_summary_prompt(tag_name: str, batch: list[dict[str, Any]]) -> str:
    return dedent(
        f"""
        You are writing concise executive market summaries for UBS senior risk readers.

        Topic bucket: {tag_name}

        Hard guardrails:
        - Use ONLY the provided fields for each market.
        - Never invent price levels, market moves, or external catalysts.
        - If data is missing, write around it instead of filling gaps.
        - Keep each executive summary to one sentence.
        - Output strict JSON only.

        Return:
        {{
          "markets": [
            {{
              "market_id": "...",
              "executive_summary_sentence": "...",
              "signal_summary": "...",
              "transmission_path": "...",
              "ubs_channels": ["..."],
              "confidence_score": 0.0,
              "risk_of_false_signal": "low" | "medium" | "high",
              "confirmation_strength": "single" | "partial" | "strong",
              "time_horizon": "immediate" | "near-term" | "medium-term",
              "shock_type": ["..."],
              "recommended_actions": ["...", "..."],
              "follow_ups": ["...", "..."]
            }}
          ]
        }}

        Market payload JSON:
        {json.dumps({"markets": batch}, ensure_ascii=False)}
        """
    ).strip()


def _merge_selection_results(
    candidates_df: pd.DataFrame,
    selected_records: list[dict[str, Any]],
    key_col: str,
) -> pd.DataFrame:
    if not selected_records:
        return candidates_df.head(0).copy()
    selected_df = pd.DataFrame(selected_records).drop_duplicates(subset=[key_col])
    out = candidates_df.merge(selected_df, on=key_col, how="inner", suffixes=("", "_llm"))
    sort_col = "selection_score" if "selection_score" in out.columns else key_col
    return out.sort_values(sort_col, ascending=False).reset_index(drop=True)


def run_batched_event_selection(
    event_candidates: pd.DataFrame,
    *,
    tag_name: str,
    client: Any,
    model: str,
    batch_size: int = 18,
    per_batch_top_k: int = 5,
    final_top_k: int = 10,
) -> pd.DataFrame:
    candidate_cols = [
        "event_slug",
        "event_title",
        "source_tag_id",
        "topic_name",
        "event_description",
        "event_startDate",
        "event_endDate",
        "event_volume24hr",
        "event_volume_total",
        "event_liquidity",
        "market_count",
        "best_market_score",
        "mean_market_score",
        "best_freshness_score",
        "max_abs_z",
        "max_abs_momentum",
        "event_candidate_score",
        "top_markets_in_event",
    ]
    records = _json_ready_df(
        event_candidates,
        columns=candidate_cols,
        sort_by="event_candidate_score",
        ascending=False,
    )
    first_pass: list[dict[str, Any]] = []
    for batch in _chunk_records(records, batch_size):
        prompt = _event_selection_prompt(tag_name, batch, per_batch_top_k)
        parsed = call_llm_json(client=client, model=model, prompt=prompt)
        first_pass.extend(parsed.get("selected_events", []))

    shortlist = _merge_selection_results(event_candidates, first_pass, "event_slug")
    final_pool = shortlist.head(max(final_top_k * 3, final_top_k)).copy()
    if final_pool.empty:
        return final_pool

    final_prompt = _event_selection_prompt(
        tag_name,
        _json_ready_df(final_pool, columns=candidate_cols, sort_by="event_candidate_score"),
        final_top_k,
    )
    final_parsed = call_llm_json(client=client, model=model, prompt=final_prompt)
    final_records = final_parsed.get("selected_events", [])
    return _merge_selection_results(final_pool, final_records, "event_slug").head(final_top_k)


def run_batched_market_selection(
    market_candidates: pd.DataFrame,
    *,
    tag_name: str,
    client: Any,
    model: str,
    batch_size: int = 24,
    per_batch_top_k: int = 6,
    final_top_k: int = 10,
) -> pd.DataFrame:
    candidate_cols = [
        "market_id",
        "question",
        "event_slug",
        "event_title",
        "source_tag_id",
        "topic_name",
        "market_startDate",
        "market_endDate",
        "days_to_end",
        "market_age_days",
        "moving_market_score",
        "freshness_score",
        "Yes Price",
        "No Price",
        "latest_price",
        "latest_zscore_7d",
        "latest_momentum_return",
        "max_abs_z",
        "z_shift_count",
        "max_abs_momentum",
        "volume24hr",
        "volume",
        "liquidity",
        "volume_per_day",
        "liquidity_per_day",
        "description",
        "event_description",
    ]
    records = _json_ready_df(
        market_candidates,
        columns=candidate_cols,
        sort_by="moving_market_score",
        ascending=False,
    )
    first_pass: list[dict[str, Any]] = []
    for batch in _chunk_records(records, batch_size):
        prompt = _market_selection_prompt(tag_name, batch, per_batch_top_k)
        parsed = call_llm_json(client=client, model=model, prompt=prompt)
        first_pass.extend(parsed.get("selected_markets", []))

    shortlist = _merge_selection_results(market_candidates, first_pass, "market_id")
    final_pool = shortlist.head(max(final_top_k * 3, final_top_k)).copy()
    if final_pool.empty:
        return final_pool

    final_prompt = _market_selection_prompt(
        tag_name,
        _json_ready_df(final_pool, columns=candidate_cols, sort_by="moving_market_score"),
        final_top_k,
    )
    final_parsed = call_llm_json(client=client, model=model, prompt=final_prompt)
    final_records = final_parsed.get("selected_markets", [])
    return _merge_selection_results(final_pool, final_records, "market_id").head(final_top_k)


def run_event_summary_pass(
    selected_events: pd.DataFrame,
    *,
    tag_name: str,
    client: Any,
    model: str,
    batch_size: int = 5,
) -> pd.DataFrame:
    if selected_events.empty:
        return selected_events.copy()

    cols = [
        "event_slug",
        "event_title",
        "topic_name",
        "market_status",
        "why_now",
        "event_startDate",
        "event_endDate",
        "event_volume24hr",
        "event_volume_total",
        "event_liquidity",
        "market_count",
        "best_market_score",
        "top_markets_in_event",
    ]
    records = _json_ready_df(selected_events, columns=cols, sort_by="selection_score")
    summaries: list[dict[str, Any]] = []
    for batch in _chunk_records(records, batch_size):
        prompt = _event_summary_prompt(tag_name, batch)
        parsed = call_llm_json(client=client, model=model, prompt=prompt)
        summaries.extend(parsed.get("events", []))

    summary_df = pd.DataFrame(summaries).drop_duplicates(subset=["event_slug"])
    return selected_events.merge(summary_df, on="event_slug", how="left")


def run_market_summary_pass(
    selected_markets: pd.DataFrame,
    *,
    tag_name: str,
    client: Any,
    model: str,
    batch_size: int = 8,
) -> pd.DataFrame:
    if selected_markets.empty:
        return selected_markets.copy()

    cols = [
        "market_id",
        "question",
        "event_slug",
        "event_title",
        "topic_name",
        "event_status",
        "event_emergence_type",
        "why_now",
        "market_startDate",
        "market_endDate",
        "days_to_end",
        "market_age_days",
        "moving_market_score",
        "Yes Price",
        "No Price",
        "latest_price",
        "latest_zscore_7d",
        "latest_momentum_return",
        "max_abs_z",
        "z_shift_count",
        "max_abs_momentum",
        "volume24hr",
        "volume",
        "liquidity",
        "volume_per_day",
        "liquidity_per_day",
        "description",
        "event_description",
    ]
    records = _json_ready_df(selected_markets, columns=cols, sort_by="selection_score")
    summaries: list[dict[str, Any]] = []
    for batch in _chunk_records(records, batch_size):
        prompt = _market_summary_prompt(tag_name, batch)
        parsed = call_llm_json(client=client, model=model, prompt=prompt)
        summaries.extend(parsed.get("markets", []))

    summary_df = pd.DataFrame(summaries).drop_duplicates(subset=["market_id"])
    return selected_markets.merge(summary_df, on="market_id", how="left")


def bulletize_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    x = df.copy()

    def _bulletize(value: Any) -> Any:
        if isinstance(value, list):
            return "\n".join(f"• {item}" for item in value)
        return value

    for column in columns:
        if column in x.columns:
            x[column] = x[column].apply(_bulletize)
    return x
