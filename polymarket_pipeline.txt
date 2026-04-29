# Auto-generated from polymarket.ipynb and then refined for script use.

from __future__ import annotations

import os, sys, json
import numpy as np
import pandas as pd
import re
import datetime
import dateutil.parser as dparser
import requests
import getpass
import time
from pathlib import Path

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
# import pytest
# from full_fred.fred import Fred ## need to install - pip install full-fred
# import fredapi as fa ## need to install - pip install fredapi
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',250)

pd.options.display.float_format = '{:.4f}'.format # Format float numbers to 4 decimals

SCRIPT_DIR = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
EVENT_FILTER_CONFIG_PATH = SCRIPT_DIR / "event_filter_config.json"
AI_TOPIC_CONFIG_PATH = SCRIPT_DIR / "ai_topic_config.json"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "polymarket_output"


# ===== Notebook cell 1 =====

user = os.getenv("UBS_TNUMBER", "")
pwd = os.getenv("UBS_INET_PASSWORD", "")


def build_session(user: str = "", pwd: str = "", test_connectivity: bool = True) -> requests.Session:
    proxy_session = requests.Session()
    if user and pwd:
        proxy_url = f"http://{user}:{pwd}@inet-proxy-b.adns.ubs.net:8080"
        proxy_session.proxies = {"http": proxy_url, "https": proxy_url}
        if test_connectivity:
            try:
                probe = proxy_session.get(
                    "https://gamma-api.polymarket.com/events",
                    params={"limit": 1, "closed": "false", "related_tags": "true"},
                    timeout=10,
                )
                probe.raise_for_status()
                print("Using UBS proxy-backed session.")
                return proxy_session
            except Exception as exc:
                print(f"Proxy session unavailable, falling back to direct internet: {exc}")
        else:
            return proxy_session

    print("Using direct internet session.")
    return requests.Session()


SESSION = build_session(user, pwd)

from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider

AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-5.2")

client = None
if AZURE_OPENAI_ENDPOINT and OPENAI_API_VERSION:
    credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    token_provider = get_bearer_token_provider(
        credential,
        "https://cognitiveservices.azure.com/.default",
    )
    client = AzureOpenAI(
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=OPENAI_API_VERSION,
        azure_ad_token_provider=token_provider,
        max_retries=12,
    )
    print(f"Azure OpenAI client ready for deployment: {deployment}")
else:
    print("Azure OpenAI env vars missing: set AZURE_OPENAI_ENDPOINT and OPENAI_API_VERSION before AI calls.")


import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import ast
import json
import numpy as np
import pandas as pd
import math

# =========================
# Config
# =========================
GAMMA = "https://gamma-api.polymarket.com"  # Polymarket Gamma base URL
CLOB = "https://clob.polymarket.com"
DATA_API = "https://data-api.polymarket.com"


# =========================
# HTTP helper
# =========================
def _get(url: str, params: Optional[dict] = None, timeout: int = 30, retries: int = 3, backoff: float = 0.8):
    """
    Simple GET with retries. Returns parsed JSON.
    """
    last_err = None
    for i in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(backoff * (2 ** i))
    raise RuntimeError(f"GET failed after {retries} retries: {url} params={params} err={last_err}")



# =========================
# Fetchers (pagination)
# =========================
def fetch_events_by_tag(
    tag_id: str,
    closed: bool = False,
    active: Optional[bool] = None,
    limit: int = 200,
    related_tags: bool = True,
) -> List[dict]:
    """
    Fetch events for a tag from Gamma with limit/offset pagination.
    Events often contain a nested `markets` list.

    NOTE: Gamma supports pagination via limit/offset.
    """
    all_rows: List[dict] = []
    offset = 0

    while True:
        params = {
            "tag_id": tag_id,
            "related_tags": str(related_tags).lower(),  # MUST be lower()
            "closed": closed,
            "limit": limit,
            "offset": offset,
        }
        if active is not None:
            params["active"] = str(active).lower()

        rows = _get(f"{GAMMA}/events", params=params)
        if not rows:
            break

        # Gamma typically returns a list of event dicts
        all_rows.extend(rows)
        offset += limit

    return all_rows


def fetch_markets_by_tag(
    tag_id: str,
    closed: bool = False,
    limit: int = 200,
    related_tags: bool = True,
) -> List[dict]:
    """
    Your original market fetcher (kept here in case you want it).
    """
    all_rows: List[dict] = []
    offset = 0
    while True:
        params = {
            "tag_id": tag_id,
            "related_tags": str(related_tags).lower(),  # FIXED
            "order": "volume24hr",
            "ascending": "false",
            "closed": str(closed).lower(),
            "limit": limit,
            "offset": offset,
        }
        rows = _get(f"{GAMMA}/markets", params=params)
        if not rows:
            break
        all_rows.extend(rows)
        offset += limit
    return all_rows


# =========================
# Normalization helpers
# =========================
EVENT_COLS = [
    "title", "slug", "endDate", "volume24hr", "volume", "liquidity", "startDate",
    "active", "closed", "markets", "tags", "source_tag_id",'source_tag_name','description','image','icon'
]

MARKET_COLS = [
    "id", "question", "slug", "active", "closed",'endDate','startDate',
    "liquidity", "volume", "volume24hr","oneDayPriceChange","lastTradePrice","tags",
    "outcomes", "outcomePrices", "clobTokenIds", "conditionId", 'description','image','icon', 'groupItemTitle'
]


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _norm_outcome(x: Any) -> Optional[str]:
    if x is None:
        return None
    return str(x).strip().upper()

def safe_list(x):
    # missing values
    if x is None or x is pd.NA:
        return []
    if isinstance(x, float) and math.isnan(x):
        return []

    # already list
    if isinstance(x, list):
        return x

    # numpy array / tuples / sets
    if isinstance(x, np.ndarray):
        return x.tolist()
    if isinstance(x, (tuple, set)):
        return list(x)

    # string that may encode a list
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return []
        # try JSON first
        if (s.startswith("[") and s.endswith("]")):
            try:
                y = json.loads(s)
                if isinstance(y, list):
                    return y
            except Exception:
                pass
            # then try python literal
            try:
                y = ast.literal_eval(s)
                if isinstance(y, list):
                    return y
            except Exception:
                pass
        return []

    return []

def concat_tag_labels(tags, sep=", "):
    if not isinstance(tags, list):
        return None
    return sep.join(
        t.get("label") for t in tags
        if isinstance(t, dict) and t.get("label")
    )

def normalize_events_markets_prices(
    events: List[Dict[str, Any]],
    keep_event_tags: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Normalize raw events (with nested markets) into:
      - events_df
      - markets_df
      - prices_long_df
      - prices_wide_df (YES/NO, UP/DOWN etc columns)

    Assumes:
      outcomes = ["Yes","No"] or ["Up","Down"] etc.
      outcomePrices = ["0.43","0.57"] aligned by index with outcomes.
    """
    
    # -------------------------
    # 1) events_df
    # -------------------------
    event_rows: List[Dict[str, Any]] = []
    for e in events:
        row = {c: e.get(c) for c in EVENT_COLS if c in e}
        if not keep_event_tags:
            row.pop("tags", None)
        event_rows.append(row)
    events_df = pd.DataFrame(event_rows)

    # Coerce numeric
    for c in ["volume24hr", "volume", "liquidity"]:
        if c in events_df.columns:
            events_df[c] = pd.to_numeric(events_df[c], errors="coerce")

    events_df["tag_labels"] = events_df["tags"].apply(concat_tag_labels)
    # -------------------------
    # 2) markets_df (explode event.markets)
    # -------------------------
    market_rows: List[Dict[str, Any]] = []

    for e in events:
        event_meta = {
            "event_title": e.get("title"),
            "event_slug": e.get("slug"),
            "event_endDate": e.get("endDate"),
            "event_volume24hr": _to_float(e.get("volume24hr")),
            "event_volume": _to_float(e.get("volume")),
            "event_liquidity": _to_float(e.get("liquidity")),
            "event_active": e.get("active"),
            "event_closed": e.get("closed"),
            "source_tag_id": e.get("source_tag_id"),
            # keep raw tags if useful for downstream
            "event_tags": e.get("tags"),
            "event_description":e.get("description"),
            "event_startDate": e.get("StartDate"),
            "event_endDate": e.get("endDate")
        }

        for m in safe_list(e.get("markets")):
            row = {c: m.get(c) for c in MARKET_COLS if c in m}
            row.update(event_meta)
            market_rows.append(row)

    markets_df = pd.DataFrame(market_rows)
    markets_df = markets_df[markets_df['closed'] == False]
    markets_df = markets_df.drop_duplicates('id')
    markets_df.rename(columns= {'endDate':'market_endDate','startDate':'market_startDate'},inplace = True)
    # numeric coercion
    for c in ["volume24hr", "volume", "liquidity", "event_volume24hr", "event_volume", "event_liquidity"]:
        if c in markets_df.columns:
            markets_df[c] = pd.to_numeric(markets_df[c], errors="coerce")
    if "id" in markets_df.columns:
        markets_df["id"] = markets_df["id"].astype("string")

    # -------------------------
    # 3) prices_long_df (align outcomes/outcomePrices/clobTokenIds by index)
    # -------------------------
    price_rows: List[Dict[str, Any]] = []

    for _, r in markets_df.iterrows():
        outcomes = safe_list(r.get("outcomes"))
        prices = safe_list(r.get("outcomePrices"))
        token_ids = safe_list(r.get("clobTokenIds"))

        n = max(len(outcomes), len(prices), len(token_ids))
        for i in range(n):
            outcome = outcomes[i] if i < len(outcomes) else None
            price = prices[i] if i < len(prices) else None
            token_id = token_ids[i] if i < len(token_ids) else None

            # skip empty
            if outcome is None and price is None:
                continue

            price_rows.append({
                "market_id": r.get("id"),
                "question": r.get("question"),
                "market_slug": r.get("slug"),
                "source_tag_id": r.get("source_tag_id"),
                "market_endDate": r.get("market_endDate"),
                "event_title": r.get("event_title"),
                "event_slug": r.get("event_slug"),
                "event_endDate": r.get("event_endDate"),
                "outcome": outcome,
                "outcome_norm": _norm_outcome(outcome),
                "last_price": _to_float(price),  # outcomePrices treated as last price
                "clob_token_id": token_id,
                "outcome_index": i,
            })

    prices_long_df = pd.DataFrame(price_rows)
    if not prices_long_df.empty:
        prices_long_df["last_price"] = pd.to_numeric(prices_long_df["last_price"], errors="coerce")
    return events_df, markets_df, prices_long_df

# =========================
# High-level function:
# from tags_df -> fetch -> normalize -> return tables
# =========================
def build_tables_from_tags_df(
    tags_df: pd.DataFrame,
    tag_id_col: str = "tag_id",
    tag_name_col: Optional[str] = "tag_name",
    closed: bool = False,
    active: Optional[bool] = True,
    limit: int = 200,
    related_tags: bool = True,
    keep_event_tags: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    For each row in tags_df, fetch events and normalize into tables.

    Returns:
      events_df
      markets_df
      prices_long_df
      prices_wide_df
      markets_enriched_df  (markets_df + wide prices merged)
    """
    all_events: List[Dict[str, Any]] = []

    tag_records = tags_df.to_dict("records")

    for t in tag_records:
        tag_id = str(t[tag_id_col])
        tag_name = str(t[tag_name_col])
        print(f"[fetch] tag={tag_name} tag_id={tag_id} fetching events...")

        events = fetch_events_by_tag(
            tag_id=tag_id,
            closed=closed,
            active=active,
            limit=limit,
            related_tags=related_tags,
        )
        print(f"[fetch] tag={tag_name} fetched_events={len(events)}")

        # Attach tag context onto each event so it flows into the normalized tables
        for e in events:
            e["source_tag_id"] = tag_id  # user-specified: event header includes source_tag_id
            e["source_tag_name"] = tag_name  # optional helpful context
        all_events.extend(events)

    events_df, markets_df, prices_long_df = normalize_events_markets_prices(
        all_events,
        keep_event_tags=keep_event_tags,
    )

    # Safety filter: keep only open and active events, even if upstream API behavior changes.
    if "closed" in events_df.columns:
        events_df = events_df[events_df["closed"] == False].copy()
    if "active" in events_df.columns:
        events_df = events_df[events_df["active"] == True].copy()

    if "event_closed" in markets_df.columns:
        markets_df = markets_df[markets_df["event_closed"] == False].copy()
    if "event_active" in markets_df.columns:
        markets_df = markets_df[markets_df["event_active"] == True].copy()

    valid_event_slugs = set(events_df["slug"].dropna().astype(str)) if "slug" in events_df.columns else set()
    if valid_event_slugs and "event_slug" in markets_df.columns:
        markets_df = markets_df[markets_df["event_slug"].astype(str).isin(valid_event_slugs)].copy()
    if valid_event_slugs and "event_slug" in prices_long_df.columns:
        prices_long_df = prices_long_df[prices_long_df["event_slug"].astype(str).isin(valid_event_slugs)].copy()

    print(
        "[normalize] "
        f"events={len(events_df)} "
        f"markets={len(markets_df)} "
        f"price_rows={len(prices_long_df)}"
    )

    # Add tag name onto events_df / markets_df if we fetched it
    if "source_tag_name" in events_df.columns:
        # keep both source_tag_id + name
        pass

    # Merge wide prices onto markets_df for a single market-level table
    # markets_df uses "id", prices_wide_df uses "market_id"

    return events_df, markets_df, prices_long_df


DEFAULT_TAGS_DF = pd.DataFrame([
    {"id": "politics", "tag_name": "Politics", "tag_id": 2},
    {"id": "finance", "tag_name": "Finance", "tag_id": 120},
    {"id": "crypto", "tag_name": "Crypto", "tag_id": 21},
    {"id": "tech", "tag_name": "Tech", "tag_id": 1401},
    {"id": "geopolitics", "tag_name": "Geopolitics", "tag_id": 100265},
    {"id": "economy", "tag_name": "Economy", "tag_id": 100328},
])


# ===== Notebook cell 3 =====

import json


def df_to_records_json(df, columns):
    # Assuming this function converts a DataFrame to a list of dictionaries.
    return df[columns].to_dict(orient='records')

DEFAULT_EVENT_FILTER_CONFIG = {
    "default": {
        "keywords": [
            "iran", "oil", "wti", "energy", "fed", "rate", "rates", "cut", "cuts",
            "gold", "silver", "election", "senate", "house", "taiwan", "china",
            "russia", "ukraine", "ceasefire", "ai", "artificial intelligence",
            "data center", "datacenter", "semiconductor", "nvidia", "macro",
            "inflation", "employment", "recession", "yield", "treasury", "credit", "spread",
            "volatility", "tariff", "geopolit", "middle east", "crude", "trump"
        ],
        "min_volume": 25000,
        "min_liquidity": 25000,
        "min_keyword_hits": 1,
        "top_n": 500
    }
}


def load_event_filter_config(config_path: Path = EVENT_FILTER_CONFIG_PATH) -> dict:
    if config_path.exists():
        try:
            return json.loads(config_path.read_text())
        except Exception as exc:
            print(f"Failed to read event filter config from {config_path}: {exc}")
    return DEFAULT_EVENT_FILTER_CONFIG


EVENT_FILTER_CONFIG = load_event_filter_config()
BUSINESS_RELEVANCE_KEYWORDS = EVENT_FILTER_CONFIG.get("default", {}).get("keywords", DEFAULT_EVENT_FILTER_CONFIG["default"]["keywords"])

DEFAULT_AI_TOPIC_CONFIG = {
    "default": {
        "market_top_n": 150,
        "event_top_n": 100,
        "top_k_markets": 3,
        "event_batch_size": 12,
        "max_markets_per_batch": 60,
        "final_market_cap": 10,
        "final_event_cap": 10,
    }
}


def load_ai_topic_config(config_path: Path = AI_TOPIC_CONFIG_PATH) -> dict:
    if config_path.exists():
        try:
            return json.loads(config_path.read_text())
        except Exception as exc:
            print(f"Failed to read AI topic config from {config_path}: {exc}")
    return DEFAULT_AI_TOPIC_CONFIG


AI_TOPIC_CONFIG = load_ai_topic_config()


def get_ai_topic_config(topic_name: Optional[str] = None, overrides: Optional[dict] = None) -> dict:
    config = dict(AI_TOPIC_CONFIG["default"])
    if topic_name:
        themed = AI_TOPIC_CONFIG.get(str(topic_name).strip().lower())
        if themed:
            config.update(themed)
    if overrides:
        config.update(overrides)
    return config


def get_event_filter_config(theme_name: Optional[str] = None, overrides: Optional[dict] = None) -> dict:
    config = dict(EVENT_FILTER_CONFIG["default"])
    if theme_name:
        themed = EVENT_FILTER_CONFIG.get(str(theme_name).strip().lower())
        if themed:
            config.update(themed)
    if overrides:
        config.update(overrides)
    return config


def _event_text_blob(row: pd.Series) -> str:
    parts = [
        str(row.get("title", "") or ""),
        str(row.get("description", "") or ""),
        str(row.get("tag_labels", "") or ""),
        str(row.get("source_tag_name", "") or ""),
    ]
    return " | ".join(parts).lower()


def _event_label_blob(row: pd.Series) -> str:
    parts = [
        str(row.get("tag_labels", "") or ""),
        str(row.get("source_tag_name", "") or ""),
        str(row.get("title", "") or ""),
    ]
    return " | ".join(parts).lower()


def score_event_relevance(events_df: pd.DataFrame, keywords: Optional[list[str]] = None) -> pd.DataFrame:
    x = events_df.copy()
    x["event_text_blob"] = x.apply(_event_text_blob, axis=1)
    x["event_label_blob"] = x.apply(_event_label_blob, axis=1)
    keywords = keywords or BUSINESS_RELEVANCE_KEYWORDS
    x["keyword_hit_count"] = x["event_text_blob"].apply(
        lambda text: sum(1 for keyword in keywords if keyword in text)
    )
    x["event_label_hit_count"] = x["event_label_blob"].apply(
        lambda text: sum(1 for keyword in keywords if keyword in text)
    )
    x["combined_hit_count"] = x[["keyword_hit_count", "event_label_hit_count"]].max(axis=1)

    x["volume"] = pd.to_numeric(x["volume"], errors="coerce").fillna(0)
    x["liquidity"] = pd.to_numeric(x["liquidity"], errors="coerce").fillna(0)
    x["volume24hr"] = pd.to_numeric(x.get("volume24hr"), errors="coerce").fillna(0)

    x["log_volume"] = np.log1p(x["volume"])
    x["log_liquidity"] = np.log1p(x["liquidity"])
    x["log_volume24hr"] = np.log1p(x["volume24hr"])

    max_keyword = max(x["combined_hit_count"].max(), 1)
    x["relevance_score"] = (
        0.50 * (x["combined_hit_count"] / max_keyword)
        + 0.20 * (x["log_volume"] / max(x["log_volume"].max(), 1))
        + 0.20 * (x["log_liquidity"] / max(x["log_liquidity"].max(), 1))
        + 0.10 * (x["log_volume24hr"] / max(x["log_volume24hr"].max(), 1))
    )
    return x


def filter_events(
    events_df,
    min_volume,
    min_liquidity,
    min_keyword_hits: int = 1,
    top_n: int = 600,
    keywords: Optional[list[str]] = None,
):
    """
    Legacy event prefilter optimized to send only business-relevant, material events to AI.
    """
    events_df_clean = events_df.drop_duplicates("slug").copy()
    scored = score_event_relevance(events_df_clean, keywords=keywords)
    filtered = scored[
        (scored["volume"] >= min_volume)
        & (scored["liquidity"] >= min_liquidity)
        & (scored["combined_hit_count"] >= min_keyword_hits)
    ].copy()
    filtered = filtered.sort_values(
        ["relevance_score", "volume24hr", "volume", "liquidity"],
        ascending=False,
    )
    return filtered.head(top_n)

def process_events_in_chunks(
    events_df_full,
    event_columns,
    theme_name: Optional[str] = None,
    theme_overrides: Optional[dict] = None,
    min_volume=25000,
    min_liquidity=25000,
    min_keyword_hits: int = 1,
    top_n: int = 500,
):
    """
    Legacy name retained for compatibility, but this function no longer calls AI.
    It now returns a deterministic keyword/materiality-filtered event table.
    """
    config = get_event_filter_config(theme_name=theme_name, overrides=theme_overrides)
    min_volume = config.get("min_volume", min_volume)
    min_liquidity = config.get("min_liquidity", min_liquidity)
    min_keyword_hits = config.get("min_keyword_hits", min_keyword_hits)
    top_n = config.get("top_n", top_n)
    keywords = config.get("keywords", BUSINESS_RELEVANCE_KEYWORDS)

    filtered_events_df = filter_events(
        events_df_full,
        min_volume,
        min_liquidity,
        min_keyword_hits=min_keyword_hits,
        top_n=top_n,
        keywords=keywords,
    )
    print(
        "[event-filter] "
        f"theme={theme_name or 'default'} "
        f"input_events={len(events_df_full.drop_duplicates('slug'))} "
        f"filtered_events={len(filtered_events_df)} "
        f"min_volume={min_volume} "
        f"min_liquidity={min_liquidity} "
        f"min_keyword_hits={min_keyword_hits} "
        f"top_n={top_n}"
    )
    keep_cols = [c for c in event_columns if c in filtered_events_df.columns]
    return filtered_events_df[keep_cols].copy()

# Example usage
EVENT_SELECTION_COLUMNS = [
    "title", "slug", "volume", "volume24hr", "liquidity",
    "tag_labels", "source_tag_name", "description", "relevance_score",
    "keyword_hit_count", "event_label_hit_count", "combined_hit_count"
]

def select_relevant_event_universe_multi_topic(
    events_df_full: pd.DataFrame,
    markets_df: pd.DataFrame,
    prices_long_df: pd.DataFrame,
    tags_df: pd.DataFrame,
    *,
    theme_overrides_by_topic: Optional[dict[str, dict]] = None,
    event_columns: Optional[list[str]] = None,
) -> dict[str, pd.DataFrame]:
    event_columns = event_columns or EVENT_SELECTION_COLUMNS
    theme_overrides_by_topic = theme_overrides_by_topic or {}

    filtered_event_frames: list[pd.DataFrame] = []
    selected_slugs: set[str] = set()

    for row in tags_df.itertuples(index=False):
        theme_name = str(getattr(row, "tag_name"))
        overrides = theme_overrides_by_topic.get(theme_name) or theme_overrides_by_topic.get(theme_name.lower())
        filtered_df = process_events_in_chunks(
            events_df_full,
            event_columns,
            theme_name=theme_name,
            theme_overrides=overrides,
        )
        filtered_df = filtered_df.copy()
        filtered_df["filter_theme"] = theme_name
        filtered_event_frames.append(filtered_df)
        if "slug" in filtered_df.columns:
            selected_slugs.update(filtered_df["slug"].dropna().astype(str).tolist())
        print(
            "[universe-topic] "
            f"theme={theme_name} "
            f"filtered_events={len(filtered_df)} "
            f"union_selected_slugs={len(selected_slugs)}"
        )

    filtered_events_legacy_df = (
        pd.concat(filtered_event_frames, ignore_index=True)
        .drop_duplicates(subset=["slug"])
        if filtered_event_frames else pd.DataFrame(columns=event_columns)
    )

    events_df_selected = events_df_full[
        events_df_full["slug"].astype(str).isin(selected_slugs)
    ].drop_duplicates("slug").copy()

    if "tags" in events_df_selected.columns and not events_df_selected.empty:
        s = events_df_selected.set_index("slug")["tags"].explode()
        df_event_tags = pd.DataFrame(s.tolist(), index=s.index)[["id", "label"]].reset_index()
    else:
        df_event_tags = pd.DataFrame(columns=["slug", "id", "label"])

    markets_filtered = markets_df[
        markets_df["event_slug"].astype(str).isin(selected_slugs)
    ].copy()
    if "groupItemTitle" in markets_filtered.columns and "question" in markets_filtered.columns:
        markets_filtered["groupItemTitle"] = markets_filtered["groupItemTitle"].fillna(markets_filtered["question"])

    prices_long_df_filtered = prices_long_df[
        prices_long_df["event_slug"].astype(str).isin(selected_slugs)
    ].copy()

    print(
        "[universe] "
        f"topics={len(tags_df)} "
        f"selected_events={len(events_df_selected)} "
        f"selected_markets={len(markets_filtered)} "
        f"selected_price_rows={len(prices_long_df_filtered)} "
        f"event_tags_rows={len(df_event_tags)}"
    )

    return {
        "filtered_events_legacy_df": filtered_events_legacy_df,
        "events_df_selected": events_df_selected,
        "df_event_tags": df_event_tags,
        "markets_filtered": markets_filtered,
        "prices_long_df_filtered": prices_long_df_filtered,
    }


# ===== Notebook cell 8 =====

#historical price 
import time
from typing import Optional, Any
import pandas as pd
import requests

CLOB = "https://clob.polymarket.com"

def _get_json(url: str, params: dict, timeout: int = 30,
              retries: int = 6, backoff: float = 0.8) -> Any:
    """
    GET JSON with retry/backoff. Handles 429 and transient errors.
    """
    last_err = None
    for i in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)

            # Handle rate limiting
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else backoff * (2 ** i)
                time.sleep(sleep_s)
                continue

            r.raise_for_status()
            return r.json()

        except Exception as e:
            last_err = e
            time.sleep(backoff * (2 ** i))

    raise RuntimeError(f"GET failed after {retries} retries: {url} params={params} err={last_err}")

def fetch_token_price_history(
    token_id: str,
    interval: str = "max",
    fidelity: int = 1440,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
) -> pd.DataFrame:
    """
    Fetch token price history from CLOB: GET /prices-history

    Query params per docs:
      market (asset id) required
      startTs/endTs optional unix seconds
      interval: max, all, 1m, 1w, 1d, 6h, 1h
      fidelity: minutes (default 1)
    Returns: {"history":[{"t":..., "p":...}, ...]}
    [1](https://docs.polymarket.com/api-reference/markets/get-prices-history)
    """
    params = {"market": str(token_id), "interval": interval, "fidelity": int(fidelity)}
    if start_ts is not None:
        params["startTs"] = int(start_ts)
    if end_ts is not None:
        params["endTs"] = int(end_ts)

    data = _get_json(f"{CLOB}/prices-history", params=params)

    history = data.get("history", []) if isinstance(data, dict) else []
    df = pd.DataFrame(history)

    if df.empty:
        return pd.DataFrame(columns=["token_id", "t", "p", "ts", "price"])
    us_eastern = pytz.timezone("US/Eastern")
    df["token_id"] = str(token_id)
    df["ts"] = pd.to_datetime(df["t"], unit="s", utc=True).dt.tz_convert(us_eastern)
    df["price"] = pd.to_numeric(df["p"], errors="coerce")
    return df[["token_id", "t", "p", "ts", "price"]].sort_values("ts")


from typing import List
import pytz

def build_price_history_from_prices_long(
    prices_long_df: pd.DataFrame,
    token_col: str = "clob_token_id",
    interval: str = "max",
    fidelity: int = 1440,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    sleep_between: float = 0.05,
) -> pd.DataFrame:
    """
    Returns a combined price history DataFrame for all unique tokens in prices_long_df.

    Each token uses CLOB GET /prices-history with:
      market=<token_id>&interval=<interval>&fidelity=<fidelity>
    [1](https://docs.polymarket.com/api-reference/markets/get-prices-history)
    """
    df = prices_long_df.copy()

    # Keep only rows with token ids
    df = df.dropna(subset=[token_col])
    df[token_col] = df[token_col].astype(str)

    token_ids: List[str] = df[token_col].drop_duplicates().tolist()

    all_hist = []
    for token_id in token_ids:
        h = fetch_token_price_history(
            token_id=token_id,
            interval=interval,
            fidelity=fidelity,
        )
        all_hist.append(h)

        if sleep_between:
            time.sleep(sleep_between)

    history_df = pd.concat(all_hist, ignore_index=True) if all_hist else pd.DataFrame()
    if history_df.empty:
        return history_df

    # Attach all original source columns back onto each token's history.
    context_cols = [c for c in df.columns if c != token_col]
    context = df[[token_col] + context_cols].drop_duplicates(subset=[token_col]).rename(columns={token_col: "token_id"})

    history_with_context = history_df.merge(context, on="token_id", how="left")
    return history_with_context.sort_values(["token_id", "ts"])


def fetch_market_trade_history(
    condition_id: str,
    limit: int = 500,
    max_pages: Optional[int] = None,
    sleep_between: float = 0.05,
    max_offset: int = 3000,
) -> pd.DataFrame:
    """
    Fetch raw trade history for a market condition from Polymarket's public Data API.

    Official docs:
    https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets

    Notes:
    - Query parameter `market` expects the market condition ID.
    - The response is a public list of trades with price, size, timestamp, outcome, and event metadata.
    """
    all_rows: list[dict[str, Any]] = []
    offset = 0
    page = 0
    hit_offset_cap = False

    while True:
        if offset > max_offset:
            hit_offset_cap = True
            break

        params = {"market": str(condition_id), "limit": int(limit), "offset": int(offset)}
        try:
            rows = _get_json(f"{DATA_API}/trades", params=params)
        except Exception as exc:
            msg = str(exc).lower()
            if "offset" in msg and "3000" in msg:
                hit_offset_cap = True
                break
            raise
        if not rows:
            break

        if not isinstance(rows, list):
            raise RuntimeError(f"Unexpected trade payload for {condition_id}: {type(rows).__name__}")

        all_rows.extend(rows)
        page += 1
        if max_pages is not None and page >= max_pages:
            break
        if len(rows) < limit:
            break

        offset += limit
        if offset > max_offset:
            hit_offset_cap = True
            break
        if sleep_between:
            time.sleep(sleep_between)

    trades_df = pd.DataFrame(all_rows)
    if trades_df.empty:
        return pd.DataFrame(
            columns=[
                "condition_id",
                "asset",
                "timestamp",
                "ts",
                "price",
                "size",
                "notional",
                "side",
                "outcome",
                "eventSlug",
                "slug",
                "title",
            ]
        )

    us_eastern = pytz.timezone("US/Eastern")
    trades_df["condition_id"] = str(condition_id)
    trades_df["timestamp"] = pd.to_numeric(trades_df["timestamp"], errors="coerce")
    trades_df["ts"] = pd.to_datetime(trades_df["timestamp"], unit="s", utc=True).dt.tz_convert(us_eastern)
    trades_df["price"] = pd.to_numeric(trades_df["price"], errors="coerce")
    trades_df["size"] = pd.to_numeric(trades_df["size"], errors="coerce")
    trades_df["notional"] = trades_df["price"] * trades_df["size"]
    trades_df["trade_history_truncated"] = bool(hit_offset_cap)
    if hit_offset_cap:
        print(
            "[history-volume] "
            f"trade_history_truncated condition_id={condition_id} "
            f"fetched_trades={len(trades_df)} max_offset={max_offset}"
        )
    return trades_df.sort_values("ts")


def build_trade_volume_history_from_markets(
    markets_df: pd.DataFrame,
    market_col: str = "conditionId",
    bucket_freq: str = "1D",
    limit: int = 500,
    max_pages: Optional[int] = None,
    sleep_between: float = 0.05,
    max_offset: int = 3000,
) -> pd.DataFrame:
    """
    Build a historical traded-volume time series similar to `build_price_history_from_prices_long`.

    Output preserves the original market columns by merging them back after trade aggregation.
    Added columns include:
    - condition_id
    - bucket_ts
    - trade_count
    - volume_tokens
    - volume_notional
    - avg_trade_price
    - last_trade_price
    - first_trade_ts
    - last_trade_ts
    """
    df = markets_df.copy()
    if market_col not in df.columns:
        print(f"[history-volume] skipped: missing market column `{market_col}` on markets_df")
        return pd.DataFrame()

    raw_market_count = len(df)
    raw_condition_non_null = int(df[market_col].notna().sum())
    df = df.dropna(subset=[market_col]).copy()
    df[market_col] = df[market_col].astype(str)
    df = df[df[market_col].str.fullmatch(r"0x[a-fA-F0-9]{64}", na=False)].copy()

    print(
        "[history-volume] "
        f"markets_in={raw_market_count} "
        f"condition_non_null={raw_condition_non_null} "
        f"valid_condition_ids={df[market_col].nunique() if not df.empty else 0}"
    )

    condition_ids = df[market_col].drop_duplicates().tolist()
    all_volume_histories: list[pd.DataFrame] = []

    for condition_id in condition_ids:
        raw_trades = fetch_market_trade_history(
            condition_id=condition_id,
            limit=limit,
            max_pages=max_pages,
            sleep_between=sleep_between,
            max_offset=max_offset,
        )
        if raw_trades.empty:
            continue

        bucketed = (
            raw_trades.assign(bucket_ts=raw_trades["ts"].dt.floor(bucket_freq))
            .groupby(["condition_id", "bucket_ts"], as_index=False)
            .agg(
                trade_count=("timestamp", "count"),
                volume_tokens=("size", "sum"),
                volume_notional=("notional", "sum"),
                avg_trade_price=("price", "mean"),
                last_trade_price=("price", "last"),
                first_trade_ts=("ts", "min"),
                last_trade_ts=("ts", "max"),
                trade_history_truncated=("trade_history_truncated", "max"),
            )
        )
        all_volume_histories.append(bucketed)

    volume_history_df = pd.concat(all_volume_histories, ignore_index=True) if all_volume_histories else pd.DataFrame()
    if volume_history_df.empty:
        return volume_history_df

    context_cols = [c for c in df.columns if c != market_col]
    context = df[[market_col] + context_cols].drop_duplicates(subset=[market_col]).rename(columns={market_col: "condition_id"})
    return volume_history_df.merge(context, on="condition_id", how="left").sort_values(["condition_id", "bucket_ts"])


def reconcile_trade_volume_to_market_snapshots(
    volume_history_df: pd.DataFrame,
    markets_df: pd.DataFrame,
    market_col: str = "conditionId",
    snapshot_total_volume_col: str = "volume",
    snapshot_24hr_volume_col: str = "volume24hr",
    bucket_col: str = "bucket_ts",
    notional_col: str = "volume_notional",
) -> pd.DataFrame:
    """
    Reconcile trade-derived volume history against current Polymarket market snapshot columns.

    Important interpretation:
    - `trade_volume_total_notional` is the cumulative sum of trade notional observed in the Data API feed.
    - `snapshot_volume` and `snapshot_volume24hr` are the current market snapshot values from Gamma.
    - Differences can arise from timing, rounding, API aggregation conventions, or incomplete trade history.
    """
    if volume_history_df.empty:
        return pd.DataFrame()

    vh = volume_history_df.copy()
    vh[bucket_col] = pd.to_datetime(vh[bucket_col], errors="coerce")
    vh[notional_col] = pd.to_numeric(vh[notional_col], errors="coerce").fillna(0)
    vh["trade_count"] = pd.to_numeric(vh["trade_count"], errors="coerce").fillna(0)

    latest_bucket = vh[bucket_col].max()
    if pd.isna(latest_bucket):
        last_24h_cutoff = None
    else:
        last_24h_cutoff = latest_bucket - pd.Timedelta(days=1)

    total_trade_volume = (
        vh.groupby("condition_id", as_index=False)
        .agg(
            trade_volume_total_notional=(notional_col, "sum"),
            trade_volume_total_tokens=("volume_tokens", "sum"),
            trade_count_total=("trade_count", "sum"),
            trade_history_start=("first_trade_ts", "min"),
            trade_history_end=("last_trade_ts", "max"),
            bucket_count=(bucket_col, "nunique"),
        )
    )

    if last_24h_cutoff is not None:
        trailing_24h = (
            vh.loc[vh[bucket_col] >= last_24h_cutoff]
            .groupby("condition_id", as_index=False)
            .agg(
                trade_volume_24h_notional=(notional_col, "sum"),
                trade_volume_24h_tokens=("volume_tokens", "sum"),
                trade_count_24h=("trade_count", "sum"),
            )
        )
    else:
        trailing_24h = pd.DataFrame(columns=["condition_id", "trade_volume_24h_notional", "trade_volume_24h_tokens", "trade_count_24h"])

    market_keep_cols = [c for c in markets_df.columns if c != market_col]
    snapshots = (
        markets_df[[market_col] + market_keep_cols]
        .dropna(subset=[market_col])
        .copy()
        .drop_duplicates(subset=[market_col])
        .rename(columns={market_col: "condition_id"})
    )

    if snapshot_total_volume_col in snapshots.columns:
        snapshots["snapshot_volume"] = pd.to_numeric(snapshots[snapshot_total_volume_col], errors="coerce")
    else:
        snapshots["snapshot_volume"] = np.nan

    if snapshot_24hr_volume_col in snapshots.columns:
        snapshots["snapshot_volume24hr"] = pd.to_numeric(snapshots[snapshot_24hr_volume_col], errors="coerce")
    else:
        snapshots["snapshot_volume24hr"] = np.nan

    reconciliation = (
        snapshots
        .merge(total_trade_volume, on="condition_id", how="left")
        .merge(trailing_24h, on="condition_id", how="left")
    )

    reconciliation["trade_volume_total_notional"] = pd.to_numeric(
        reconciliation["trade_volume_total_notional"], errors="coerce"
    ).fillna(0)
    reconciliation["trade_volume_24h_notional"] = pd.to_numeric(
        reconciliation["trade_volume_24h_notional"], errors="coerce"
    ).fillna(0)

    reconciliation["total_volume_gap"] = (
        reconciliation["trade_volume_total_notional"] - reconciliation["snapshot_volume"]
    )
    reconciliation["volume24hr_gap"] = (
        reconciliation["trade_volume_24h_notional"] - reconciliation["snapshot_volume24hr"]
    )

    reconciliation["total_volume_gap_pct_vs_snapshot"] = np.where(
        reconciliation["snapshot_volume"].fillna(0).abs() > 0,
        reconciliation["total_volume_gap"] / reconciliation["snapshot_volume"],
        np.nan,
    )
    reconciliation["volume24hr_gap_pct_vs_snapshot"] = np.where(
        reconciliation["snapshot_volume24hr"].fillna(0).abs() > 0,
        reconciliation["volume24hr_gap"] / reconciliation["snapshot_volume24hr"],
        np.nan,
    )

    def _reconciliation_flag(row: pd.Series) -> str:
        total_gap_pct = row.get("total_volume_gap_pct_vs_snapshot")
        vol24_gap_pct = row.get("volume24hr_gap_pct_vs_snapshot")
        if pd.isna(total_gap_pct) and pd.isna(vol24_gap_pct):
            return "no_snapshot_reference"
        if (pd.notna(total_gap_pct) and abs(total_gap_pct) <= 0.05) and (
            pd.isna(vol24_gap_pct) or abs(vol24_gap_pct) <= 0.10
        ):
            return "close_match"
        if (pd.notna(total_gap_pct) and abs(total_gap_pct) <= 0.20) and (
            pd.isna(vol24_gap_pct) or abs(vol24_gap_pct) <= 0.25
        ):
            return "moderate_gap"
        return "large_gap"

    reconciliation["reconciliation_flag"] = reconciliation.apply(_reconciliation_flag, axis=1)
    return reconciliation.sort_values(
        ["reconciliation_flag", "trade_volume_total_notional"],
        ascending=[True, False],
    )

import numpy as np
import pandas as pd

def build_daily_features(
    price_history_df: pd.DataFrame,
    token_col: str = "token_id",   # or "clob_token_id"
    ts_col: str = "ts",
    price_col: str = "price",
    z_window: int = 7,
    min_periods: int | None = None,
) -> pd.DataFrame:
    """
    Steps:
      1) Ensure types (datetime, numeric)
      2) For each token & day: keep the latest observation (max timestamp)
      3) Reindex to a full daily calendar per token in US/Eastern
      4) Forward-fill missing price with prior day's price
      5) Preserve the synthetic daily timestamps created by reindexing
      6) Compute momentum: P_t / P_{t-1} and return
      7) Compute regime shift z-score using prior 7 days (exclude today)

    Returns: daily_df with features per token per day.
    """
    df = price_history_df.copy()
    us_eastern = pytz.timezone("US/Eastern")
    # --- 1) types ---
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.dropna(subset=[token_col, ts_col, price_col])

    # --- 2) daily bucket + keep latest per token/day ---
    df["date"] = df[ts_col].dt.tz_convert(us_eastern).dt.floor("D")
    # sort so "last" is truly the latest observation within the day
    df = df.sort_values([token_col, "date", ts_col])

    # if you have multiple rows per day, keep the latest one
    daily = (
        df.groupby([token_col, "date"], as_index=False)
          .last()
    )

    # choose min_periods default (must be z_window if you want full-window zscore)
    if min_periods is None:
        min_periods = z_window
    # --- 3) reindex each token to full daily range + 4) forward fill ---
    out_parts = []
    for token, part in daily.groupby(token_col, sort=False):
        part = part.set_index("date").sort_index()

        full_idx = pd.date_range(
            start=part.index.min(),
            end=part.index.max(),
            freq="D",
            tz=us_eastern
        )

        part = part.reindex(full_idx)
        part.index.name = "date"

        # restore token column
        part[token_col] = token

        # Preserve the reindexed calendar day itself as the canonical daily timestamp.
        # This fixes missing daily rows when the source history skips a day or when the
        # API returns sparse points at coarse fidelity.
        part[ts_col] = part.index

        # forward-fill missing price from prior day
        part[price_col] = part[price_col].ffill()

        out_parts.append(part.reset_index())

    daily_df = pd.concat(out_parts, ignore_index=True)

    # --- 5) momentum ---
    daily_df[ts_col] = pd.to_datetime(daily_df[ts_col], errors="coerce")
    daily_df = daily_df.sort_values([token_col, ts_col])
    g = daily_df.groupby(token_col, group_keys=False)

    daily_df["p_prev_day"] = g[price_col].shift(1)
    daily_df["momentum_ratio"] = daily_df[price_col] / daily_df["p_prev_day"]
    daily_df["momentum_return"] = daily_df["momentum_ratio"] - 1

    # --- 6) z-score (regime shift) based on prior z_window days (exclude today) ---
    shifted = g[price_col].shift(1)

    roll_mean = (
        shifted.groupby(daily_df[token_col])
               .rolling(z_window, min_periods=min_periods)
               .mean()
               .reset_index(level=0, drop=True)
    )
    roll_std = (
        shifted.groupby(daily_df[token_col])
               .rolling(z_window, min_periods=min_periods)
               .std(ddof=0)
               .reset_index(level=0, drop=True)
    )

    daily_df[f"mean_{z_window}d"] = roll_mean
    daily_df[f"std_{z_window}d"] = roll_std.replace(0, np.nan)

    daily_df[f"zscore_{z_window}d"] = (
        (daily_df[price_col] - daily_df[f"mean_{z_window}d"]) / daily_df[f"std_{z_window}d"]
    )

    # Optional: a regime shift flag
    daily_df[f"regime_shift_{z_window}d_flag"] = daily_df[f"zscore_{z_window}d"].abs() >= 2

    # clean up helper column if you don't need it
    # daily_df = daily_df.drop(columns=["date"], errors="ignore")

    return daily_df


def build_daily_price_volume_history(
    daily_price_df: pd.DataFrame,
    volume_history_df: pd.DataFrame,
    markets_df: pd.DataFrame,
    market_id_col: str = "market_id",
    condition_col_markets: str = "conditionId",
    condition_col_volume: str = "condition_id",
    price_ts_col: str = "ts",
    volume_bucket_col: str = "bucket_ts",
) -> pd.DataFrame:
    """
    Merge daily price history with daily trade-volume history.

    Why this is done at daily grain:
    - `daily_price_df` is one row per token/day after normalization.
    - `volume_history_df` is one row per market/day after bucket aggregation.
    - Joining at daily grain avoids duplicating rows and keeps the price features stable.

    The merge path is:
    token/day price row -> market_id -> conditionId -> condition_id/day volume row
    """
    if daily_price_df.empty:
        return daily_price_df.copy()

    price_df = daily_price_df.copy()
    price_df[price_ts_col] = pd.to_datetime(price_df[price_ts_col], errors="coerce")
    price_df["price_day"] = price_df[price_ts_col].dt.floor("D")

    market_map_cols = [c for c in [market_id_col, condition_col_markets] if c in markets_df.columns]
    if len(market_map_cols) < 2:
        out = price_df.copy()
        out["condition_id"] = np.nan
        out["trade_count"] = np.nan
        out["volume_tokens"] = np.nan
        out["volume_notional"] = np.nan
        out["avg_trade_price"] = np.nan
        out["last_trade_price"] = np.nan
        out["first_trade_ts"] = pd.NaT
        out["last_trade_ts"] = pd.NaT
        return out

    market_map = (
        markets_df[[market_id_col, condition_col_markets]]
        .dropna(subset=[market_id_col, condition_col_markets])
        .drop_duplicates(subset=[market_id_col])
        .rename(columns={condition_col_markets: "condition_id"})
    )

    out = price_df.merge(market_map, on=market_id_col, how="left")

    if volume_history_df.empty or condition_col_volume not in volume_history_df.columns or volume_bucket_col not in volume_history_df.columns:
        out["trade_count"] = np.nan
        out["volume_tokens"] = np.nan
        out["volume_notional"] = np.nan
        out["avg_trade_price"] = np.nan
        out["last_trade_price"] = np.nan
        out["first_trade_ts"] = pd.NaT
        out["last_trade_ts"] = pd.NaT
        return out

    volume_df = volume_history_df.copy()
    volume_df[volume_bucket_col] = pd.to_datetime(volume_df[volume_bucket_col], errors="coerce")
    volume_df["price_day"] = volume_df[volume_bucket_col].dt.floor("D")

    volume_keep_cols = [
        condition_col_volume,
        "price_day",
        "trade_count",
        "volume_tokens",
        "volume_notional",
        "avg_trade_price",
        "last_trade_price",
        "first_trade_ts",
        "last_trade_ts",
    ]
    volume_keep_cols = [c for c in volume_keep_cols if c in volume_df.columns]
    volume_merge = volume_df[volume_keep_cols].rename(columns={condition_col_volume: "condition_id"})

    out = out.merge(volume_merge, on=["condition_id", "price_day"], how="left")
    return out


def audit_market_daily_coverage(
    daily_price_df: pd.DataFrame,
    volume_history_df: pd.DataFrame,
    markets_df: pd.DataFrame,
    market_id_col: str = "market_id",
    condition_col_markets: str = "conditionId",
    condition_col_volume: str = "condition_id",
    price_ts_col: str = "ts",
    volume_bucket_col: str = "bucket_ts",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Audit whether each market has a complete daily calendar in both price and trade-volume history.

    Returns:
    - market_coverage_summary_df: one row per market with missing-day counts and percentages
    - market_missing_dates_df: one row per missing market/day with flags for missing price and/or volume

    Interpretation:
    - Missing price day: no row exists in the normalized daily price history for that market/day.
    - Missing volume day: no trade-volume bucket exists for that market/day.
    - A market can have daily price coverage but no volume because price is forward-filled while trades may not occur.
    """
    if daily_price_df.empty:
        empty_summary = pd.DataFrame(
            columns=[
                market_id_col,
                "condition_id",
                "calendar_start",
                "calendar_end",
                "expected_days",
                "price_days_present",
                "volume_days_present",
                "missing_price_days",
                "missing_volume_days",
                "missing_any_days",
                "missing_price_pct",
                "missing_volume_pct",
                "missing_any_pct",
            ]
        )
        empty_details = pd.DataFrame(
            columns=[market_id_col, "condition_id", "calendar_day", "missing_price", "missing_volume"]
        )
        return empty_summary, empty_details

    market_map = pd.DataFrame(columns=[market_id_col, "condition_id"])
    if market_id_col in markets_df.columns and condition_col_markets in markets_df.columns:
        market_map = (
            markets_df[[market_id_col, condition_col_markets]]
            .dropna(subset=[market_id_col, condition_col_markets])
            .drop_duplicates(subset=[market_id_col])
            .rename(columns={condition_col_markets: "condition_id"})
        )

    price_df = daily_price_df.copy()
    price_df[price_ts_col] = pd.to_datetime(price_df[price_ts_col], errors="coerce")
    price_df["calendar_day"] = price_df[price_ts_col].dt.floor("D")
    if "condition_id" not in price_df.columns and not market_map.empty:
        price_df = price_df.merge(market_map, on=market_id_col, how="left")

    volume_df = volume_history_df.copy()
    if not volume_df.empty:
        volume_df[volume_bucket_col] = pd.to_datetime(volume_df[volume_bucket_col], errors="coerce")
        volume_df["calendar_day"] = volume_df[volume_bucket_col].dt.floor("D")
        if condition_col_volume != "condition_id" and condition_col_volume in volume_df.columns:
            volume_df = volume_df.rename(columns={condition_col_volume: "condition_id"})

    summaries: list[dict] = []
    missing_rows: list[dict] = []

    grouped = price_df.dropna(subset=[market_id_col, "calendar_day"]).groupby(market_id_col, sort=False)
    for market_id, price_part in grouped:
        condition_id = price_part["condition_id"].dropna().astype(str).iloc[0] if "condition_id" in price_part.columns and not price_part["condition_id"].dropna().empty else None
        calendar_start = price_part["calendar_day"].min()
        calendar_end = price_part["calendar_day"].max()
        expected_idx = pd.date_range(start=calendar_start, end=calendar_end, freq="D")
        expected_days = pd.Series(expected_idx)

        price_days_present = set(price_part["calendar_day"].dropna().tolist())
        volume_days_present = set()
        if not volume_df.empty and condition_id is not None and "condition_id" in volume_df.columns:
            volume_part = volume_df[volume_df["condition_id"].astype(str) == str(condition_id)]
            volume_days_present = set(volume_part["calendar_day"].dropna().tolist())

        missing_price_days = []
        missing_volume_days = []
        for day in expected_days:
            has_price = day in price_days_present
            has_volume = day in volume_days_present
            if not has_price:
                missing_price_days.append(day)
            if not has_volume:
                missing_volume_days.append(day)
            if (not has_price) or (not has_volume):
                missing_rows.append(
                    {
                        market_id_col: market_id,
                        "condition_id": condition_id,
                        "calendar_day": day,
                        "missing_price": not has_price,
                        "missing_volume": not has_volume,
                    }
                )

        expected_count = len(expected_days)
        missing_any_days = len({*missing_price_days, *missing_volume_days})
        summaries.append(
            {
                market_id_col: market_id,
                "condition_id": condition_id,
                "calendar_start": calendar_start,
                "calendar_end": calendar_end,
                "expected_days": expected_count,
                "price_days_present": len(price_days_present),
                "volume_days_present": len(volume_days_present),
                "missing_price_days": len(missing_price_days),
                "missing_volume_days": len(missing_volume_days),
                "missing_any_days": missing_any_days,
                "missing_price_pct": len(missing_price_days) / expected_count if expected_count else np.nan,
                "missing_volume_pct": len(missing_volume_days) / expected_count if expected_count else np.nan,
                "missing_any_pct": missing_any_days / expected_count if expected_count else np.nan,
            }
        )

    market_coverage_summary_df = pd.DataFrame(summaries).sort_values(
        ["missing_any_days", "missing_volume_days", "missing_price_days"],
        ascending=False,
    ) if summaries else pd.DataFrame()
    market_missing_dates_df = pd.DataFrame(missing_rows).sort_values(
        [market_id_col, "calendar_day"],
        ascending=True,
    ) if missing_rows else pd.DataFrame()
    return market_coverage_summary_df, market_missing_dates_df


# ===== Notebook cell 11 =====



def build_history_and_feature_data(
    prices_long_df_filtered: pd.DataFrame,
    markets_filtered: pd.DataFrame,
    *,
    price_interval: str = "max",
    price_fidelity: int = 60,
    volume_bucket_freq: str = "1D",
    volume_limit: int = 500,
    z_window: int = 7,
    include_volume_history: bool = True,
) -> dict[str, pd.DataFrame]:
    price_history_df = build_price_history_from_prices_long(
        prices_long_df_filtered,
        interval=price_interval,
        fidelity=price_fidelity,
    )
    print(
        "[history] "
        f"price_history_rows={len(price_history_df)} "
        f"unique_tokens={price_history_df['token_id'].nunique() if 'token_id' in price_history_df.columns and not price_history_df.empty else 0}"
    )

    if include_volume_history:
        volume_history_df = (
            build_trade_volume_history_from_markets(
                markets_filtered,
                market_col="conditionId",
                bucket_freq=volume_bucket_freq,
                limit=volume_limit,
            )
            if "conditionId" in markets_filtered.columns
            else pd.DataFrame()
        )

        volume_reconciliation_df = (
            reconcile_trade_volume_to_market_snapshots(
                volume_history_df,
                markets_filtered,
                market_col="conditionId",
                snapshot_total_volume_col="volume",
                snapshot_24hr_volume_col="volume24hr",
            )
            if not volume_history_df.empty and "conditionId" in markets_filtered.columns
            else pd.DataFrame()
        )
    else:
        print("[history-volume] skipped: include_volume_history=False")
        volume_history_df = pd.DataFrame()
        volume_reconciliation_df = pd.DataFrame()

    daily_features_df = build_daily_features(
        price_history_df,
        token_col="token_id",
        ts_col="ts",
        price_col="price",
        z_window=z_window,
    )

    daily_price_volume_features_df = build_daily_price_volume_history(
        daily_features_df,
        volume_history_df,
        markets_filtered,
        market_id_col="market_id",
        condition_col_markets="conditionId",
        condition_col_volume="condition_id",
        price_ts_col="ts",
        volume_bucket_col="bucket_ts",
    )

    market_coverage_summary_df, market_missing_dates_df = audit_market_daily_coverage(
        daily_features_df,
        volume_history_df,
        markets_filtered,
        market_id_col="market_id",
        condition_col_markets="conditionId",
        condition_col_volume="condition_id",
        price_ts_col="ts",
        volume_bucket_col="bucket_ts",
    )

    print(
        "[history] "
        f"volume_history_rows={len(volume_history_df)} "
        f"volume_reconciliation_rows={len(volume_reconciliation_df)} "
        f"daily_price_rows={len(daily_features_df)} "
        f"daily_price_volume_rows={len(daily_price_volume_features_df)} "
        f"coverage_summary_rows={len(market_coverage_summary_df)} "
        f"missing_date_rows={len(market_missing_dates_df)}"
    )

    return {
        "price_history_df": price_history_df,
        "volume_history_df": volume_history_df,
        "volume_reconciliation_df": volume_reconciliation_df,
        "daily_features_df": daily_features_df,
        "daily_price_volume_features_df": daily_price_volume_features_df,
        "market_coverage_summary_df": market_coverage_summary_df,
        "market_missing_dates_df": market_missing_dates_df,
        "include_volume_history": include_volume_history,
    }


# ===== Notebook cell 13 =====

price_columns = ['token_id', 'ts', 'price', 'market_id', 
       'outcome_norm', 'question', 'event_slug', 'event_title',
       'source_tag_id', 'p_prev_day', 'momentum_ratio', 'momentum_return',
       'mean_7d', 'std_7d', 'zscore_7d', 'regime_shift_7d_flag','market_endDate',
       'market_startDate', 'volume',"volume24hr" ,'liquidity',
       'condition_id', 'price_day', 'trade_count', 'volume_tokens',
       'volume_notional', 'avg_trade_price', 'last_trade_price',
       'first_trade_ts', 'last_trade_ts']


# ===== Notebook cell 14 =====

# ----------------------------
# Helpers
# ----------------------------
def _norm01(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn, mx = s.min(skipna=True), s.max(skipna=True)
    return (s - mn) / (mx - mn + 1e-9)

def enddate_weight_piecewise(days_to_end: pd.Series,
                             soon_days: int = 2,
                             sweet_start: int = 7,
                             sweet_end: int = 45,
                             far_days: int = 360) -> pd.Series:
    """
    Weight in [0,1], highest in sweet window, tapers outside.
    """
    d = pd.to_numeric(days_to_end, errors="coerce")
    w = np.zeros(len(d), dtype=float)

    sweet = (d >= sweet_start) & (d <= sweet_end)
    w[sweet] = 1.0

    ramp_up = (d >= soon_days) & (d < sweet_start)
    w[ramp_up] = (d[ramp_up] - soon_days) / max(1, (sweet_start - soon_days))

    ramp_down = (d > sweet_end) & (d <= far_days)
    w[ramp_down] = 1.0 - (d[ramp_down] - sweet_end) / max(1, (far_days - sweet_end))

    w = np.clip(w, 0, 1)
    # Past end date -> 0
    w[(d < 0).fillna(False)] = 0.0
    return pd.Series(w, index=days_to_end.index)

# ----------------------------
# 2) Choose a "canonical" outcome per market (YES/UP)
# ----------------------------
def filter_canonical_outcome(df: pd.DataFrame, outcome_norm_col: str = "outcome_norm") -> pd.DataFrame:
    """
    Keep canonical side per market: YES if present, else UP if present.
    Falls back to the first outcome per market if neither exists.
    """
    x = df.copy()
    x[outcome_norm_col] = x[outcome_norm_col].astype(str).str.upper().str.strip()

    # Preference order: YES, UP
    pref = pd.Series(2, index=x.index)  # default low priority
    pref[x[outcome_norm_col].eq("YES")] = 0
    pref[x[outcome_norm_col].eq("UP")] = 1
    x["_pref"] = pref

    # For each market_id+date choose best pref outcome (lowest number)
    # If your df has multiple outcomes per token/day, this collapses to canonical.
    keep = (
        x.sort_values(["market_id", "ts", "_pref"])
         .groupby(["market_id", "ts"], as_index=False)
         .head(1)
    )
    return keep.drop(columns=["_pref"], errors="ignore")

# ----------------------------
# 3) Build market-level features (latest day + lookback)
# ----------------------------
def build_market_signals(
    token_daily_df: pd.DataFrame,
    date_col: str = "ts",
    lookback_days: int = 30,
) -> pd.DataFrame:
    """
    Aggregate token daily features into market-level signals.
    """
    df = token_daily_df.copy()
    df[date_col] = pd.to_datetime(df[date_col], utc=True, errors="coerce")

    max_date = df[date_col].max()
    cutoff = max_date - pd.Timedelta(days=lookback_days)
    w = df[df[date_col] >= cutoff].copy()

    w["abs_z"] = w["zscore_7d"].abs()
    w["abs_mom"] = w["momentum_return"].abs()
    w["z_ge_2"] = w["abs_z"] >= 2

    # latest row per market
    latest = (
        w.sort_values(["market_id", date_col])
         .groupby("market_id", as_index=False)
         .tail(1)
         [["market_id", date_col, "zscore_7d", "momentum_return", "price"]]
         .rename(columns={date_col: "latest_date",
                          "price": "latest_price",
                          "zscore_7d": "latest_zscore_7d",
                          "momentum_return": "latest_momentum_return"})
    )

    agg = (
        w.groupby("market_id", as_index=False)
         .agg(
             max_abs_z=("abs_z", "max"),
             z_shift_count=("z_ge_2", "sum"),
             max_abs_momentum=("abs_mom", "max"),
             avg_abs_z=("abs_z", "mean"),
             avg_abs_momentum=("abs_mom", "mean"),
         )
    )

    out = agg.merge(latest, on="market_id", how="left")
    return out

def rank_moving_markets(
    market_signals_df: pd.DataFrame,
    markets_df: pd.DataFrame,
    market_id_col_markets: str = "id",
    market_end_col: str = "market_endDate",
    vol24_col: str = "volume24hr",
    vol_total_col: str = "volume",
    liq_col: str = "liquidity",
    # time preference
    soon_days: int = 2,
    sweet_start: int = 7,
    sweet_end: int = 45,
    far_days: int = 180,
    # weights
    w_z: float = 1.5,
    w_freq: float = 0.8,
    w_mom: float = 0.6,
    w_liq: float = 0.3,
    vol24_mix: float = 0.7,     # weight on 24h volume vs total volume
) -> pd.DataFrame:
    """
    Produces a ranked table of markets likely to be "moving" early indicators.
    """
    s = market_signals_df.copy()

    m = markets_df.copy()
    m[market_end_col] = pd.to_datetime(m[market_end_col], utc=True, errors="coerce")
    m[vol24_col] = pd.to_numeric(m[vol24_col], errors="coerce").fillna(0)
    m[vol_total_col] = pd.to_numeric(m[vol_total_col], errors="coerce").fillna(0)
    m[liq_col] = pd.to_numeric(m[liq_col], errors="coerce").fillna(0)

    # Keep one row per market id (markets_df may have duplicates depending on how built)
    keep_cols = [market_id_col_markets, "question", "slug", "event_title", "event_slug",'outcomes','outcomePrices','market_startDate',"event_endDate","event_startDate",
                 market_end_col, vol24_col, vol_total_col, liq_col, "active", "closed", "source_tag_id","description",'event_description']
    keep_cols = [c for c in keep_cols if c in m.columns]
    m = m[keep_cols].drop_duplicates(subset=[market_id_col_markets])

    # Merge signals -> meta
    s = s.merge(m, left_on="market_id", right_on=market_id_col_markets, how="left")

    # Days-to-end and weight
    now = pd.Timestamp.utcnow()
    s["days_to_end"] = (s[market_end_col] - now).dt.total_seconds() / 86400.0
    s["enddate_weight"] = enddate_weight_piecewise(
        s["days_to_end"], soon_days=soon_days, sweet_start=sweet_start, sweet_end=sweet_end, far_days=far_days
    )

    # Volume weights (log + normalize)
    s["log_vol24"] = np.log1p(s[vol24_col].fillna(0))
    s["log_vol_total"] = np.log1p(s[vol_total_col].fillna(0))
    s["vol24_norm"] = _norm01(s["log_vol24"])
    s["vol_total_norm"] = _norm01(s["log_vol_total"])

    s["volume_score"] = vol24_mix * s["vol24_norm"] + (1 - vol24_mix) * s["vol_total_norm"]
    s["liq_norm"] = _norm01(np.log1p(s[liq_col].fillna(0)))

    # Base signal score from price dynamics
    s["signal_score"] = (
        w_z * s["max_abs_z"].fillna(0)
        + w_freq * s["z_shift_count"].fillna(0)
        + w_mom * s["max_abs_momentum"].fillna(0)
        + w_liq * s["liq_norm"].fillna(0)
    )

    # Final score with interest weighting
    s["moving_market_score"] = (
        s["signal_score"]
        * (1 + s["volume_score"].fillna(0))
        * s["enddate_weight"].fillna(0)
    )

    return s.sort_values("moving_market_score", ascending=False)


# ===== Notebook cell 15 =====

# Assuming 'outcomes' and 'outcomePrices' columns contain JSON-like strings/lists, e.g.:
# outcomes = '["Yes", "No"]'
# outcomePrices = '["0.9", "0.1"]'

import ast
import pandas as pd

def extract_yes_no_prices(row):
    # Parse the string representation to Python list if necessary
    # If already lists, skip parsing
    outcomes = row['outcomes']
    outcomePrices = row['outcomePrices']

    if isinstance(outcomes, str):
        try:
            outcomes = ast.literal_eval(outcomes)
        except Exception:
            outcomes = []
    if isinstance(outcomePrices, str):
        try:
            outcomePrices = ast.literal_eval(outcomePrices)
        except Exception:
            outcomePrices = []

    # Create a mapping of outcome -> price
    price_map = dict(zip(outcomes, outcomePrices))

    yes_price = price_map.get("Yes", None)
    no_price = price_map.get("No", None)

    # Convert to float if possible
    try:
        yes_price = float(yes_price) if yes_price is not None else None
    except Exception:
        yes_price = None
    try:
        no_price = float(no_price) if no_price is not None else None
    except Exception:
        no_price = None

    return pd.Series({
        "Yes Price": yes_price,
        "No Price": no_price
    })


def build_ranked_markets(
    daily_features_df: pd.DataFrame,
    markets_filtered: pd.DataFrame,
    *,
    lookback_days: int = 30,
) -> dict[str, pd.DataFrame]:
    canonical = filter_canonical_outcome(daily_features_df, outcome_norm_col="outcome_norm")
    market_signals = build_market_signals(canonical, lookback_days=lookback_days)
    ranked_markets = rank_moving_markets(
        market_signals,
        markets_filtered,
        market_id_col_markets="id",
        market_end_col="market_endDate",
        vol24_col="volume24hr",
        vol_total_col="volume",
        liq_col="liquidity",
    )
    ranked_markets = ranked_markets[ranked_markets["days_to_end"] > 0].copy()
    ranked_markets[["Yes Price", "No Price"]] = ranked_markets.apply(extract_yes_no_prices, axis=1)
    print(
        "[ranking] "
        f"canonical_rows={len(canonical)} "
        f"market_signal_rows={len(market_signals)} "
        f"ranked_markets={len(ranked_markets)}"
    )
    return {
        "canonical": canonical,
        "market_signals": market_signals,
        "ranked_markets": ranked_markets,
    }


# ===== Notebook cell 19 =====

def df_to_records_json(df: pd.DataFrame, cols: list[str], n: int, sort_by: Optional[str] = None, ascending: bool = False):
    cols = [c for c in cols if c in df.columns]
    if sort_by and sort_by in df.columns:
        x = df.sort_values(sort_by, ascending=ascending).head(n)[cols].copy()
    else:
        x = df.sort_values(cols[0] if cols else df.columns[0], ascending=False).head(n)[cols].copy()

    # Convert datetime columns to ISO strings
    for c in x.columns:
        if pd.api.types.is_datetime64_any_dtype(x[c]):
            x[c] = x[c].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Replace NaN with None for clean JSON
    x = x.where(pd.notnull(x), None)

    return x.to_dict("records")

MARKET_PAYLOAD_COLUMNS = [
    "market_id", "question", "event_slug", "event_title", "market_endDate", "days_to_end","market_startDate",
    "moving_market_score","event_startDate","event_endDate",
    "latest_price", "latest_zscore_7d", "latest_momentum_return",
    "max_abs_z", "z_shift_count", "max_abs_momentum",
    "volume24hr", "volume", "liquidity",
    "topic_name","description","event_description"  # optional
]

def convert_series_to_est(series: pd.Series) -> pd.Series:
    est = pytz.timezone('US/Eastern')

    def convert_timestamp(ts):
        if pd.isna(ts):
            return None
        if ts.tzinfo is None:
            ts = ts.tz_localize(pytz.UTC)
        ts_est = ts.tz_convert(est)
        return ts_est.strftime("%Y-%m-%dT%H:%M:%SZ")

    if not pd.api.types.is_datetime64_any_dtype(series):
        raise ValueError("Input series must have datetime64 dtype")

    return series.apply(convert_timestamp)

def build_event_payload_from_markets(ranked_markets: pd.DataFrame, n_events: int = 50, top_k_markets: int = 3):
    df = ranked_markets.copy()
    est = pytz.timezone('US/Eastern')
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] =convert_series_to_est(df[c])
    # Aggregate event metrics
    agg = (df.groupby("event_slug", as_index=False)
             .agg(
                 event_title=("event_title", "first"),
                 event_description=("event_description", "first"),
                 event_startDate=("event_startDate", "first"),
                 event_endDate=("event_endDate", "first"),
                 event_volume24hr=("volume24hr", "sum"),
                 event_volume_total=("volume", "sum"),
                 event_liquidity=("liquidity", "sum"),
                 best_market_score=("moving_market_score", "max"),
                 topic_name=("topic_name", "first") if "topic_name" in df.columns else ("event_title", "first"),
             )
          )
    
    # Find top markets driving each event
    top_markets = (df.sort_values("moving_market_score", ascending=False)
                     .groupby("event_slug")
                     .head(top_k_markets)[["event_slug", "market_id", "question", "Yes Price",'No Price',"moving_market_score","outcomes","outcomePrices","market_endDate","market_startDate","description", "latest_momentum_return",
    "max_abs_z", "z_shift_count", "max_abs_momentum",
    "volume24hr", "volume", "liquidity"]]
                  )

    drivers = (top_markets.groupby("event_slug")
                         .apply(lambda g: g.to_dict("records"))
                         .reset_index(name="top_markets_in_event"))

    out = agg.merge(drivers, on="event_slug", how="left")
    out = out.sort_values("best_market_score", ascending=False).head(n_events)


    out = out.where(pd.notnull(out), None)
    return out.to_dict("records")

def build_ai_payloads(
    ranked_markets: pd.DataFrame,
    tags_df: pd.DataFrame,
    *,
    market_cols: Optional[list[str]] = None,
    market_top_n: int = 150,
    event_top_n: int = 100,
    top_k_markets: int = 3,
) -> dict[str, dict]:
    market_cols = market_cols or MARKET_PAYLOAD_COLUMNS
    markets_by_tag = {}
    events_by_tag = {}
    tag_name_lookup = {
        str(row["tag_id"]): str(row["tag_name"])
        for _, row in tags_df[["tag_id", "tag_name"]].iterrows()
    }
    for tag_id in ranked_markets["source_tag_id"].unique():
        topic_name = tag_name_lookup.get(str(tag_id), str(tag_id))
        topic_cfg = get_ai_topic_config(topic_name)
        subset = ranked_markets[ranked_markets["source_tag_id"] == tag_id]
        market_top_n_for_topic = int(topic_cfg.get("market_top_n", market_top_n))
        event_top_n_for_topic = int(topic_cfg.get("event_top_n", event_top_n))
        top_k_markets_for_topic = int(topic_cfg.get("top_k_markets", top_k_markets))

        markets_by_tag[tag_id] = df_to_records_json(
            subset,
            market_cols,
            n=market_top_n_for_topic,
            sort_by="moving_market_score",
            ascending=False,
        )
        events_by_tag[tag_id] = build_event_payload_from_markets(
            subset,
            n_events=event_top_n_for_topic,
            top_k_markets=top_k_markets_for_topic,
        )
        print(
            "[ai-payload] "
            f"tag_id={tag_id} "
            f"topic={topic_name} "
            f"market_candidates={len(markets_by_tag[tag_id])} "
            f"event_candidates={len(events_by_tag[tag_id])}"
        )
    return {"markets_by_tag": markets_by_tag, "events_by_tag": events_by_tag}


# ===== Notebook cell 20 =====

from typing import Optional
import json



def _base_tag_prompt_text(tag_name, top_event_cap: int = 10, top_market_cap: int = 10):
    BASE_PROMPT = """
    You are an early‑warning risk analyst supporting UBS US Wealth Management and Investment Banking,
including Private Markets.

────────────────────────────────────────
CONTEXT
────────────────────────────────────────
All candidate markets and events relate ONLY to the topic: "{tag_name}".

You will receive candidate prediction markets and events from Polymarket.
Each market resolves to:
• YES = $1 if the event occurs
• NO = $0 otherwise
Per‑market data includes:
• price (0–1) = YES probability
• implied NO probability = 1 − price
• momentum and max momentum
• z‑scores
• volume24hr, total volume
• liquidity
• days_to_end
• moving_market_score
• topic and event metadata

Interpretation guardrails:
• High momentum ≠ high probability
• Low liquidity = noisy signals
• Near‑expiry markets move faster. Ignore days to end <0, it means the market has expired.
• Signals NEVER override UBS relevance
• Newly launched markets with strong early volume or liquidity are
  treated as expectation‑formation signals and may be prioritized
  even if historical data is limited
• For new markets, assess volume‑per‑day and liquidity‑per‑day
  rather than absolute totals
────────────────────────────────────────
OBJECTIVE
────────────────────────────────────────
Identify high‑quality prediction markets and event themes that act as
EARLY‑WARNING INDICATORS of US‑related risks impacting UBS exposures.
Focus on transmission into:
1) Collateral / Margin
2) Private Credit Clients
3) Trading

────────────────────────────────────────
TASKS
────────────────────────────────────────
A) Select the TOP {top_event_cap} EVENTS that are the strongest early‑warning indicators RIGHT NOW.
B) Select the TOP {top_market_cap} MARKETS that most clearly express those risks RIGHT NOW.
C) Identify up to 5 NEW or RECENTLY LAUNCHED  markets/events that:
• Started trading recently
• Show disproportionately high volume, liquidity, or price movement
  relative to time since launch
• Provide a meaningful update on market expectations

These markets/events may overlap with Tasks A or B, but must be explicitly flagged.

Explicitly identify NEW EVENTS, defined as:
• Events that did not previously exist or were not previously tradeable
• Events representing a new framing or escalation of an existing risk
• Events driven by newly launched, heavily traded markets

────────────────────────────────────────
EXECUTIVE SUMMARY REQUIREMENT (CRITICAL)
────────────────────────────────────────
You MUST generate a ONE‑SENTENCE EXECUTIVE SUMMARY for:

• EACH EVENT
• EACH MARKET

This sentence is written as if briefing senior UBS risk management.

The sentence MUST include, where applicable:
• Current YES % ("Yes Price") and NO % ("No Price"). 
• Direction of recent change (up/down) - latest_momentum_return suggest the direction of "Yes" outcome
• Approximate magnitude of change (week‑on‑week or recent)
• Commentary on volatility / stability of pricing
• Why this market stand out 

EVENT‑LEVEL EXECUTIVE SUMMARY (MANDATORY):
• Must explicitly NAME the key markets driving the signal.
• Must describe overall market expectation and skew (base vs tail).

Example:
“Iran escalation risk has repriced higher, led by ‘WTI $100 by March’ (YES 70%, +44pp WoW)
and ‘US‑Iran ceasefire by April’ (YES 30%, −8pp), signaling elevated oil and volatility risk
despite unstable, headline‑driven pricing.”

MARKET‑LEVEL EXECUTIVE SUMMARY (MANDATORY):
• Must describe YES % vs NO %, recent direction, and volatility.

Example:
“‘WTI $100 by March’ pricing jumped to YES 70% from 26% last week, with sharp intraday swings,
indicating rising near‑term energy upside risk and elevated inflation sensitivity.”

────────────────────────────────────────
SELECTION RULES
────────────────────────────────────────
1) Signal strength: high |z|, repeated spikes, strong momentum
2) Credibility: high liquidity and volume
3) Timing: prefer 7–45 days to end
4) De‑duplication: one clean market per idea
5) Coverage: diversify themes
6) UBS relevance (CRITICAL): must map clearly to UBS risk channels
7. Market freshness (NEW):
   • Recently launched markets showing heavy early trading,
     strong liquidity, or rapid expectation convergence
   • Favor markets where activity is high relative to age,
     signaling rapid information assimilation
────────────────────────────────────────
REQUIRED ANALYSIS (PER EVENT / MARKET)
────────────────────────────────────────
For EACH selected event or market, provide:

1) Executive summary sentence (per rules above)
2) Signal story (what changed and why) - summarize the most important change in probability, volume or trend
3) Transmission path to UBS risk
4) UBS channels impacted
5) Confidence score (0–1)
6) Risk of false signal: ["low", "medium", "high"]
7) Confirmation strength: ["single", "partial", "strong"]
8) Time horizon: ["immediate", "near-term", "medium-term"]
9) Shock type
10) Alert rules
11) Recommended UBS actions - highlight 2-3 forward-looking indicators or events to monitor


Write in a tone suitable for senior management. Prioritize insight over description
────────────────────────────────────────
OUTPUT FORMAT (STRICT JSON ONLY)
────────────────────────────────────────
{{
  "events": [
    {{
      "event_slug": "...",
      "event_title": "...",
      "topic_name": "...",
      "market_status": "new" | "mature",
      "executive_summary_sentence": "...",
      "top_markets_driving_signal": ["..."],
      "signal_summary": "...",
      "transmission_path": "...",
      "ubs_channels": ["..."],
      "confidence_score": 0.9,
      "risk_of_false_signal": "medium",
      "confirmation_strength": "strong",
      "time_horizon": "near-term",
      "shock_type": ["..."],
      "alert_rules": {{
        "trigger_if_any_market_alerts": true,
        "guardrails": {{ "min_event_volume24hr": 20000 }}
      }},
      "recommended_actions": ["...", "..."],
      "follow_ups": ["...", "..."]
    }}
  ],
  "markets": [
    {{
      "market_id": "...",
      "question": "...",
      "topic_name": "...",
      "event_status": "new" | "existing",
      "event_emergence_type": "new_risk" | "reframed_risk" | "escalation",
      "executive_summary_sentence": "...",
      "signal_summary": "...",
      "transmission_path": "...",
      "ubs_channels": ["..."],
      "confidence_score": 0.85,
      "risk_of_false_signal": "low",
      "confirmation_strength": "strong",
      "time_horizon": "near-term",
      "shock_type": ["..."],
      "alert_rules": {{
        "zscore": {{ "threshold_abs": 2.5 }},
        "momentum": {{ "threshold_abs_return": 0.08 }},
        "guardrails": {{
          "min_volume24hr": 10000,
          "min_liquidity": 25000
        }}
      }},
      "recommended_actions": ["...", "..."],
      "follow_ups": ["...", "..."]
    }}
  ]
}}
"""
    return BASE_PROMPT.format(tag_name=tag_name, top_event_cap=top_event_cap, top_market_cap=top_market_cap)


def build_tag_prompt(tag_name, markets_payload, events_payload, top_event_cap: int = 10, top_market_cap: int = 10):
    STRICTNESS_ADDENDUM = """
────────────────────────────────────────
ADDITIONAL EXECUTION RULES (DO NOT RELAX ANY REQUIREMENT ABOVE)
────────────────────────────────────────
1) Use ONLY event titles, event slugs, market IDs, market questions, and numeric values that appear in the provided JSON payload.
2) If a metric is missing, do NOT infer or fabricate it. Omit the metric or lower confidence instead.
3) Copy `event_slug`, `event_title`, `market_id`, and `question` exactly from the payload.
4) Do NOT mention any market, catalyst, or related contract unless it is explicitly present in the payload.
5) If an item is low-quality, thin, duplicated, or not clearly tied to UBS transmission, exclude it rather than forcing a pick.
6) When summarizing direction or magnitude of change, rely only on fields present in the payload such as `latest_momentum_return`, `Yes Price`, `No Price`, z-scores, volume, and liquidity.
7) Return ONE valid JSON object only. No markdown fences, no prose before JSON, no prose after JSON.
8) If you are uncertain, return fewer picks. Precision is more important than coverage.
"""
    payload_json = json.dumps(
        {"markets": markets_payload, "events": events_payload},
        ensure_ascii=False
    )
    print(tag_name)
    return _base_tag_prompt_text(tag_name, top_event_cap=top_event_cap, top_market_cap=top_market_cap) + STRICTNESS_ADDENDUM + "\nINPUT JSON:\n" + payload_json


def build_market_pick_prompt(tag_name, markets_payload, events_payload, top_market_cap: int = 10, top_event_cap: int = 10):
    PICK_STAGE_ADDENDUM = """
────────────────────────────────────────
SELECTION STAGE OVERRIDE
────────────────────────────────────────
This is STAGE 1 of a two-stage workflow.

Your job in this stage is ONLY to pick the best markets.
Do NOT generate executive commentary yet.
Do NOT generate event writeups yet.

Return STRICT JSON ONLY with this exact structure:
{
  "markets": [
    {
      "market_id": "...",
      "question": "...",
      "topic_name": "...",
      "event_status": "new" | "existing",
      "event_emergence_type": "new_risk" | "reframed_risk" | "escalation",
      "selection_rationale": "...",
      "ubs_channels": ["..."],
      "confidence_score": 0.85,
      "risk_of_false_signal": "low" | "medium" | "high",
      "confirmation_strength": "single" | "partial" | "strong",
      "time_horizon": "immediate" | "near-term" | "medium-term",
      "shock_type": ["..."]
    }
  ],
  "events": []
}

Hard constraints:
1) Select only the strongest markets.
2) If uncertain, return fewer markets.
3) Copy market_id and question exactly from the payload.
4) Do not invent percentages, catalysts, or commentary in this stage.
"""
    payload_json = json.dumps(
        {"markets": markets_payload, "events": events_payload},
        ensure_ascii=False
    )
    print(f"{tag_name} [market-pick]")
    return _base_tag_prompt_text(tag_name, top_event_cap=top_event_cap, top_market_cap=top_market_cap) + PICK_STAGE_ADDENDUM + "\nINPUT JSON:\n" + payload_json


def build_market_commentary_prompt(tag_name, selected_markets_payload, selected_events_payload, top_market_cap: int = 10, top_event_cap: int = 10):
    COMMENTARY_STAGE_ADDENDUM = """
────────────────────────────────────────
COMMENTARY STAGE OVERRIDE
────────────────────────────────────────
This is STAGE 2 of a two-stage workflow.

The markets in the input JSON were already shortlisted in STAGE 1.
Your job now is to generate the commentary and action fields for ONLY those shortlisted markets.

Return STRICT JSON ONLY with this exact structure:
{
  "markets": [
    {
      "market_id": "...",
      "question": "...",
      "topic_name": "...",
      "event_status": "new" | "existing",
      "event_emergence_type": "new_risk" | "reframed_risk" | "escalation",
      "executive_summary_sentence": "...",
      "signal_summary": "...",
      "transmission_path": "...",
      "ubs_channels": ["..."],
      "confidence_score": 0.85,
      "risk_of_false_signal": "low",
      "confirmation_strength": "strong",
      "time_horizon": "near-term",
      "shock_type": ["..."],
      "alert_rules": {
        "zscore": { "threshold_abs": 2.5 },
        "momentum": { "threshold_abs_return": 0.08 },
        "guardrails": {
          "min_volume24hr": 10000,
          "min_liquidity": 25000
        }
      },
      "recommended_actions": ["...", "..."],
      "follow_ups": ["...", "..."]
    }
  ],
  "events": []
}

Hard constraints:
1) Use ONLY the shortlisted markets in the payload.
2) Do not introduce new markets.
3) If a metric is missing, omit it rather than infer it.
"""
    payload_json = json.dumps(
        {"markets": selected_markets_payload, "events": selected_events_payload},
        ensure_ascii=False
    )
    print(f"{tag_name} [market-commentary]")
    return _base_tag_prompt_text(tag_name, top_event_cap=top_event_cap, top_market_cap=top_market_cap) + COMMENTARY_STAGE_ADDENDUM + "\nINPUT JSON:\n" + payload_json


def build_event_pick_prompt(tag_name, markets_payload, events_payload, top_event_cap: int = 10, top_market_cap: int = 10):
    PICK_STAGE_ADDENDUM = """
────────────────────────────────────────
EVENT SELECTION STAGE OVERRIDE
────────────────────────────────────────
This is STAGE 1 of a two-stage workflow for events.

Your job in this stage is ONLY to pick the best events.
Do NOT generate executive commentary yet.

Return STRICT JSON ONLY with this exact structure:
{
  "events": [
    {
      "event_slug": "...",
      "event_title": "...",
      "topic_name": "...",
      "market_status": "new" | "mature",
      "selection_rationale": "...",
      "top_markets_driving_signal": ["..."],
      "ubs_channels": ["..."],
      "confidence_score": 0.9,
      "risk_of_false_signal": "low" | "medium" | "high",
      "confirmation_strength": "single" | "partial" | "strong",
      "time_horizon": "immediate" | "near-term" | "medium-term",
      "shock_type": ["..."]
    }
  ],
  "markets": []
}

Hard constraints:
1) Select only the strongest events.
2) If uncertain, return fewer events.
3) Copy event_slug and event_title exactly from the payload.
4) Do not invent commentary in this stage.
"""
    payload_json = json.dumps(
        {"markets": markets_payload, "events": events_payload},
        ensure_ascii=False
    )
    print(f"{tag_name} [event-pick]")
    return _base_tag_prompt_text(tag_name, top_event_cap=top_event_cap, top_market_cap=top_market_cap) + PICK_STAGE_ADDENDUM + "\nINPUT JSON:\n" + payload_json


def build_event_commentary_prompt(tag_name, selected_markets_payload, selected_events_payload, top_event_cap: int = 10, top_market_cap: int = 10):
    COMMENTARY_STAGE_ADDENDUM = """
────────────────────────────────────────
EVENT COMMENTARY STAGE OVERRIDE
────────────────────────────────────────
This is STAGE 2 of a two-stage workflow for events.

The events in the input JSON were already shortlisted in STAGE 1.
Your job now is to generate the commentary and action fields for ONLY those shortlisted events.
Use the shortlisted markets only as supporting evidence.

Return STRICT JSON ONLY with this exact structure:
{
  "events": [
    {
      "event_slug": "...",
      "event_title": "...",
      "topic_name": "...",
      "market_status": "new" | "mature",
      "executive_summary_sentence": "...",
      "top_markets_driving_signal": ["..."],
      "signal_summary": "...",
      "transmission_path": "...",
      "ubs_channels": ["..."],
      "confidence_score": 0.9,
      "risk_of_false_signal": "medium",
      "confirmation_strength": "strong",
      "time_horizon": "near-term",
      "shock_type": ["..."],
      "alert_rules": {
        "trigger_if_any_market_alerts": true,
        "guardrails": { "min_event_volume24hr": 20000 }
      },
      "recommended_actions": ["...", "..."],
      "follow_ups": ["...", "..."]
    }
  ],
  "markets": []
}

Hard constraints:
1) Use ONLY the shortlisted events and supporting shortlisted markets in the payload.
2) Do not introduce new events or new markets.
3) If a metric is missing, omit it rather than infer it.
"""
    payload_json = json.dumps(
        {"markets": selected_markets_payload, "events": selected_events_payload},
        ensure_ascii=False
    )
    print(f"{tag_name} [event-commentary]")
    return _base_tag_prompt_text(tag_name, top_event_cap=top_event_cap, top_market_cap=top_market_cap) + COMMENTARY_STAGE_ADDENDUM + "\nINPUT JSON:\n" + payload_json

import json
import time
from typing import Optional


def _extract_json_object(raw_text: str) -> dict:
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("No JSON object found")
    return json.loads(raw_text[start:end + 1])


def _chat_completion_json(
    *,
    client,
    model: str,
    prompt: str,
    temperature: float = 0.01,
):
    if client is None:
        raise RuntimeError("Azure OpenAI client is not configured. Set AZURE_OPENAI_ENDPOINT and OPENAI_API_VERSION.")

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
            response_format={"type": "json_object"},
        )
    except TypeError:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
        )

    raw_text = response.choices[0].message.content
    return _extract_json_object(raw_text)


def _chunk_tag_payload(
    markets_payload: list[dict],
    events_payload: list[dict],
    event_batch_size: int = 12,
    max_markets_per_batch: int = 60,
) -> list[tuple[list[dict], list[dict]]]:
    if not events_payload:
        return [
            (markets_payload[i : i + max_markets_per_batch], [])
            for i in range(0, len(markets_payload), max_markets_per_batch)
        ]

    batches: list[tuple[list[dict], list[dict]]] = []
    covered_market_ids: set[str] = set()

    for start in range(0, len(events_payload), event_batch_size):
        event_batch = events_payload[start : start + event_batch_size]
        event_slugs = {event.get("event_slug") for event in event_batch if event.get("event_slug")}
        related_markets = [market for market in markets_payload if market.get("event_slug") in event_slugs]
        related_markets = related_markets[:max_markets_per_batch]
        covered_market_ids.update(str(market.get("market_id")) for market in related_markets if market.get("market_id") is not None)
        batches.append((related_markets, event_batch))

    orphan_markets = [
        market for market in markets_payload
        if str(market.get("market_id")) not in covered_market_ids
    ]
    for start in range(0, len(orphan_markets), max_markets_per_batch):
        batches.append((orphan_markets[start : start + max_markets_per_batch], []))

    return batches


def _dedupe_records(records: list[dict], key: str) -> list[dict]:
    deduped: dict[str, dict] = {}
    for record in records:
        record_key = record.get(key)
        if record_key is None:
            continue
        deduped[str(record_key)] = record
    return list(deduped.values())


def _merge_batch_results(batch_results: list[dict]) -> dict:
    merged_events: list[dict] = []
    merged_markets: list[dict] = []
    for result in batch_results:
        merged_events.extend(result.get("events", []))
        merged_markets.extend(result.get("markets", []))
    return {
        "events": _dedupe_records(merged_events, "event_slug"),
        "markets": _dedupe_records(merged_markets, "market_id"),
    }


def _merge_market_pick_results(batch_results: list[dict]) -> list[dict]:
    merged_markets: list[dict] = []
    for result in batch_results:
        merged_markets.extend(result.get("markets", []))
    return _dedupe_records(merged_markets, "market_id")


def _merge_event_pick_results(batch_results: list[dict]) -> list[dict]:
    merged_events: list[dict] = []
    for result in batch_results:
        merged_events.extend(result.get("events", []))
    return _dedupe_records(merged_events, "event_slug")


def _subset_payload(records: list[dict], key: str, keep_values: set[str]) -> list[dict]:
    out = []
    for record in records:
        value = record.get(key)
        if value is not None and str(value) in keep_values:
            out.append(record)
    return out


def _missing_record_ids(
    shortlisted_records: list[dict],
    commentary_records: list[dict],
    key: str,
) -> set[str]:
    shortlisted_ids = {str(item.get(key)) for item in shortlisted_records if item.get(key) is not None}
    commentary_ids = {str(item.get(key)) for item in commentary_records if item.get(key) is not None}
    return shortlisted_ids - commentary_ids

tags_df = pd.DataFrame([
    {"id": "politics", "tag_name": "Politics", "tag_id": 2},
    {"id": "finance", "tag_name": "Finance", "tag_id": 120},
    {"id": "crypto", "tag_name": "Crypto", "tag_id": 21},
    {"id": "tech", "tag_name": "Tech", "tag_id": 1401},
    {"id": "geopolitics", "tag_name": "Geopolitics", "tag_id": 100265},
    {"id": "economy", "tag_name": "Economy", "tag_id": 100328}
])
def run_ai_for_tag(
    *,
    tag_name,
    tag_id: str,
    markets_by_tag: dict,
    events_by_tag: dict,
    client,
    model: str,
    temperature: float = 0.01,
    max_retries: int = 2,
    sleep_sec: float = 1.0,
) -> Optional[dict]:

    markets = markets_by_tag.get(str(tag_id), [])
    events = events_by_tag.get(str(tag_id), [])
    if not markets and not events:
        return None
    prompt = build_tag_prompt(tag_name, markets, events)
    for attempt in range(1, max_retries + 1):
        try:
            parsed = _chat_completion_json(
                client=client,
                model=model,
                prompt=prompt,
                temperature=temperature,
            )
            return parsed

        except Exception as e:
            print(f"[{tag_name}] Attempt {attempt} failed: {e}")
            if attempt < max_retries:
                time.sleep(sleep_sec)


def run_ai_for_tag_batched(
    *,
    tag_name,
    tag_id: str,
    markets_by_tag: dict,
    events_by_tag: dict,
    client,
    model: str,
    temperature: float = 0.01,
    max_retries: int = 2,
    sleep_sec: float = 1.0,
    event_batch_size: int = 12,
    max_markets_per_batch: int = 60,
    final_market_cap: int = 10,
    final_event_cap: int = 10,
    topic_overrides: Optional[dict] = None,
) -> Optional[dict]:
    topic_cfg = get_ai_topic_config(tag_name, overrides=topic_overrides)
    event_batch_size = int(topic_cfg.get("event_batch_size", event_batch_size))
    max_markets_per_batch = int(topic_cfg.get("max_markets_per_batch", max_markets_per_batch))
    final_market_cap = int(topic_cfg.get("final_market_cap", final_market_cap))
    final_event_cap = int(topic_cfg.get("final_event_cap", final_event_cap))

    markets = markets_by_tag.get(str(tag_id), [])
    events = events_by_tag.get(str(tag_id), [])
    if not markets and not events:
        return None

    print(
        "[ai-start] "
        f"tag={tag_name} "
        f"final_market_cap={final_market_cap} "
        f"final_event_cap={final_event_cap} "
        f"event_batch_size={event_batch_size} "
        f"max_markets_per_batch={max_markets_per_batch} "
        f"market_candidates={len(markets)} "
        f"event_candidates={len(events)}"
    )

    batch_pick_results: list[dict] = []
    event_pick_results: list[dict] = []
    for batch_markets, batch_events in _chunk_tag_payload(
        markets,
        events,
        event_batch_size=event_batch_size,
        max_markets_per_batch=max_markets_per_batch,
    ):
        for attempt in range(1, max_retries + 1):
            try:
                prompt = build_market_pick_prompt(
                    tag_name,
                    batch_markets,
                    batch_events,
                    top_market_cap=final_market_cap,
                    top_event_cap=final_event_cap,
                )
                parsed = _chat_completion_json(
                    client=client,
                    model=model,
                    prompt=prompt,
                    temperature=temperature,
                )
                batch_pick_results.append(parsed)
                print(
                    "[ai-stage1-market] "
                    f"tag={tag_name} "
                    f"batch_markets={len(batch_markets)} "
                    f"batch_events={len(batch_events)} "
                    f"picked_markets={len(parsed.get('markets', []))}"
                )
                break
            except Exception as e:
                print(f"[{tag_name}] Batch attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    time.sleep(sleep_sec)
                else:
                    raise

        if batch_events:
            for attempt in range(1, max_retries + 1):
                try:
                    event_prompt = build_event_pick_prompt(
                        tag_name,
                        batch_markets,
                        batch_events,
                        top_market_cap=final_market_cap,
                        top_event_cap=final_event_cap,
                    )
                    parsed_events = _chat_completion_json(
                        client=client,
                        model=model,
                        prompt=event_prompt,
                        temperature=temperature,
                    )
                    event_pick_results.append(parsed_events)
                    print(
                        "[ai-stage1-event] "
                        f"tag={tag_name} "
                        f"batch_markets={len(batch_markets)} "
                        f"batch_events={len(batch_events)} "
                        f"picked_events={len(parsed_events.get('events', []))}"
                    )
                    break
                except Exception as e:
                    print(f"[{tag_name}] Event batch attempt {attempt} failed: {e}")
                    if attempt < max_retries:
                        time.sleep(sleep_sec)
                    else:
                        raise

    selected_markets_stage1 = _merge_market_pick_results(batch_pick_results)
    selected_events_stage1 = _merge_event_pick_results(event_pick_results)
    selected_market_ids = {str(item.get("market_id")) for item in selected_markets_stage1 if item.get("market_id") is not None}
    selected_event_slugs_stage1 = {str(item.get("event_slug")) for item in selected_events_stage1 if item.get("event_slug") is not None}

    finalist_markets = _subset_payload(markets, "market_id", selected_market_ids)[:final_market_cap]
    market_event_slugs = {str(item.get("event_slug")) for item in finalist_markets if item.get("event_slug") is not None}
    all_selected_event_slugs = selected_event_slugs_stage1 | market_event_slugs
    finalist_events = _subset_payload(events, "event_slug", all_selected_event_slugs)[:final_event_cap]
    if not finalist_markets and not finalist_events:
        return {"markets": [], "events": []}

    print(
        "[ai-shortlist] "
        f"tag={tag_name} "
        f"stage1_market_picks={len(selected_markets_stage1)} "
        f"stage1_event_picks={len(selected_events_stage1)} "
        f"finalist_markets={len(finalist_markets)} "
        f"finalist_events={len(finalist_events)}"
    )

    stage1_fields = {str(item.get("market_id")): item for item in selected_markets_stage1 if item.get("market_id") is not None}
    shortlisted_markets_payload = []
    for market in finalist_markets:
        enriched = dict(market)
        stage1 = stage1_fields.get(str(market.get("market_id")), {})
        for key in [
            "topic_name",
            "event_status",
            "event_emergence_type",
            "selection_rationale",
            "ubs_channels",
            "confidence_score",
            "risk_of_false_signal",
            "confirmation_strength",
            "time_horizon",
            "shock_type",
        ]:
            if key in stage1 and stage1.get(key) is not None:
                enriched[key] = stage1.get(key)
        shortlisted_markets_payload.append(enriched)

    market_commentary_prompt = build_market_commentary_prompt(
        tag_name,
        shortlisted_markets_payload,
        finalist_events,
        top_market_cap=final_market_cap,
        top_event_cap=final_event_cap,
    )
    market_final_result = _chat_completion_json(
        client=client,
        model=model,
        prompt=market_commentary_prompt,
        temperature=temperature,
    )
    market_commentary_records = list(market_final_result.get("markets", []))
    missing_market_ids = _missing_record_ids(shortlisted_markets_payload, market_commentary_records, "market_id")
    if missing_market_ids:
        print(
            "[ai-stage2-market-retry] "
            f"tag={tag_name} "
            f"missing_market_ids={len(missing_market_ids)}"
        )
        retry_markets_payload = _subset_payload(shortlisted_markets_payload, "market_id", missing_market_ids)
        retry_events_payload = [
            event for event in finalist_events
            if str(event.get("event_slug")) in {str(market.get("event_slug")) for market in retry_markets_payload if market.get("event_slug") is not None}
        ]
        retry_prompt = build_market_commentary_prompt(
            tag_name,
            retry_markets_payload,
            retry_events_payload,
            top_market_cap=len(retry_markets_payload),
            top_event_cap=min(final_event_cap, len(retry_events_payload) if retry_events_payload else final_event_cap),
        )
        retry_result = _chat_completion_json(
            client=client,
            model=model,
            prompt=retry_prompt,
            temperature=temperature,
        )
        market_commentary_records.extend(retry_result.get("markets", []))

    print(
        "[ai-stage2-market] "
        f"tag={tag_name} "
        f"commented_markets={len(market_commentary_records)}"
    )

    commentary_by_market = {
        str(item.get("market_id")): item
        for item in market_commentary_records
        if item.get("market_id") is not None
    }
    merged_markets = []
    for market in shortlisted_markets_payload:
        market_id = str(market.get("market_id"))
        if market_id not in commentary_by_market:
            continue
        merged = dict(market)
        merged.update(commentary_by_market.get(market_id, {}))
        merged_markets.append(merged)

    event_stage1_fields = {str(item.get("event_slug")): item for item in selected_events_stage1 if item.get("event_slug") is not None}
    shortlisted_events_payload = []
    supporting_market_names_by_event: dict[str, list[str]] = {}
    for market in merged_markets:
        event_slug = market.get("event_slug")
        question = market.get("question")
        if event_slug and question:
            supporting_market_names_by_event.setdefault(str(event_slug), []).append(question)

    for event in finalist_events:
        enriched = dict(event)
        stage1 = event_stage1_fields.get(str(event.get("event_slug")), {})
        for key in [
            "topic_name",
            "market_status",
            "selection_rationale",
            "top_markets_driving_signal",
            "ubs_channels",
            "confidence_score",
            "risk_of_false_signal",
            "confirmation_strength",
            "time_horizon",
            "shock_type",
        ]:
            if key in stage1 and stage1.get(key) is not None:
                enriched[key] = stage1.get(key)
        if "top_markets_driving_signal" not in enriched or not enriched.get("top_markets_driving_signal"):
            enriched["top_markets_driving_signal"] = supporting_market_names_by_event.get(str(event.get("event_slug")), [])[:3]
        shortlisted_events_payload.append(enriched)

    if shortlisted_events_payload:
        event_commentary_prompt = build_event_commentary_prompt(
            tag_name,
            merged_markets,
            shortlisted_events_payload,
            top_market_cap=final_market_cap,
            top_event_cap=final_event_cap,
        )
        event_final_result = _chat_completion_json(
            client=client,
            model=model,
            prompt=event_commentary_prompt,
            temperature=temperature,
        )
        event_commentary_records = list(event_final_result.get("events", []))
        missing_event_slugs = _missing_record_ids(shortlisted_events_payload, event_commentary_records, "event_slug")
        if missing_event_slugs:
            print(
                "[ai-stage2-event-retry] "
                f"tag={tag_name} "
                f"missing_event_slugs={len(missing_event_slugs)}"
            )
            retry_events_payload = _subset_payload(shortlisted_events_payload, "event_slug", missing_event_slugs)
            supporting_markets_for_retry = [
                market for market in merged_markets
                if str(market.get("event_slug")) in missing_event_slugs
            ]
            retry_event_prompt = build_event_commentary_prompt(
                tag_name,
                supporting_markets_for_retry,
                retry_events_payload,
                top_market_cap=min(final_market_cap, len(supporting_markets_for_retry) if supporting_markets_for_retry else final_market_cap),
                top_event_cap=len(retry_events_payload),
            )
            retry_event_result = _chat_completion_json(
                client=client,
                model=model,
                prompt=retry_event_prompt,
                temperature=temperature,
            )
            event_commentary_records.extend(retry_event_result.get("events", []))
        print(
            "[ai-stage2-event] "
            f"tag={tag_name} "
            f"commented_events={len(event_commentary_records)}"
        )
        commentary_by_event = {
            str(item.get("event_slug")): item
            for item in event_commentary_records
            if item.get("event_slug") is not None
        }
        merged_events = []
        for event in shortlisted_events_payload:
            event_slug = str(event.get("event_slug"))
            if event_slug not in commentary_by_event:
                continue
            merged = dict(event)
            merged.update(commentary_by_event.get(event_slug, {}))
            merged_events.append(merged)
    else:
        merged_events = []

    print(
        "[ai-done] "
        f"tag={tag_name} "
        f"final_markets={len(merged_markets)} "
        f"final_events={len(merged_events)}"
    )

    return {"markets": merged_markets, "events": merged_events}


def list_to_bullet_list(lst):
    if lst is None:
        return ""
    if isinstance(lst, float) and pd.isna(lst):
        return ""
    if isinstance(lst, list):
        return "\n".join([f"• {item}" for item in lst])
    return str(lst)
list_columns_events = ["ubs_channels", "shock_type", "recommended_actions", "follow_ups", "top_markets_driving_signal"]
list_columns_markets = ["ubs_channels", "shock_type", "recommended_actions", "follow_ups"]


def materialize_ai_results(per_tag_results: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not per_tag_results:
        return pd.DataFrame(), pd.DataFrame()

    df_raw = pd.DataFrame.from_dict(per_tag_results, orient="index")
    df_raw = df_raw[df_raw["markets"].apply(lambda x: isinstance(x, list) and x != [])]

    df_markets = pd.DataFrame(df_raw["markets"].explode().tolist()) if not df_raw.empty else pd.DataFrame()
    df_events = pd.DataFrame(df_raw["events"].explode().tolist()) if not df_raw.empty else pd.DataFrame()

    if not df_markets.empty:
        for column in list_columns_markets:
            if column in df_markets.columns:
                df_markets[column] = df_markets[column].apply(list_to_bullet_list)

    if not df_events.empty:
        for column in list_columns_events:
            if column in df_events.columns:
                df_events[column] = df_events[column].apply(list_to_bullet_list)

    print(
        "[materialize] "
        f"tags_with_results={len(per_tag_results)} "
        f"df_markets_rows={len(df_markets)} "
        f"df_events_rows={len(df_events)}"
    )

    return df_markets, df_events


def prepare_excel_export_frame(frame: pd.DataFrame, timezone_name: str = "US/Eastern") -> pd.DataFrame:
    """
    Convert timezone-aware datetime columns into timezone-naive local timestamps for Excel export.

    Excel does not support timezone-aware datetimes, so every exportable DataFrame should pass
    through this helper before calling `.to_excel(...)`.
    """
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()

    out = frame.copy()

    for column in out.columns:
        series = out[column]
        if pd.api.types.is_datetime64tz_dtype(series):
            out[column] = series.dt.tz_convert(timezone_name).dt.tz_localize(None)
            continue

        if pd.api.types.is_object_dtype(series):
            non_null = series.dropna()
            if non_null.empty:
                continue
            sample = non_null.iloc[0]
            if isinstance(sample, pd.Timestamp) and sample.tzinfo is not None:
                converted = pd.to_datetime(series, errors="coerce")
                out[column] = converted.dt.tz_convert(timezone_name).dt.tz_localize(None)

    return out


def export_pipeline_outputs(results: dict, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[export] output_dir={output_dir}")
    include_volume_history = bool(results.get("include_volume_history", True))

    df_markets = results.get("df_markets", pd.DataFrame())
    df_events = results.get("df_events", pd.DataFrame())

    excel_frames = [df_markets, df_events]
    output_filenames = ["AI Market Pick 10.xlsx", "AI Event Pick 10.xlsx"]
    sheet_names = ["markets", "events"]
    wrap_columns = [list_columns_markets, list_columns_events]

    for frame, filename, sheet_name, columns in zip(excel_frames, output_filenames, sheet_names, wrap_columns):
        export_frame = prepare_excel_export_frame(frame)
        with pd.ExcelWriter(output_dir / filename, engine="xlsxwriter") as writer:
            export_frame.to_excel(writer, index=False, sheet_name=sheet_name)
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            wrap_format = workbook.add_format({"text_wrap": True})
            for idx, column in enumerate(export_frame.columns):
                if column in columns:
                    worksheet.set_column(idx, idx, None, wrap_format)
        print(f"[export] wrote {filename} rows={len(export_frame)}")

    if "daily_features_df" in results and not results["daily_features_df"].empty:
        daily_features_export = prepare_excel_export_frame(results["daily_features_df"])
        daily_features_export.to_excel(output_dir / "Price History.xlsx", index=False)
        print(f"[export] wrote Price History.xlsx rows={len(daily_features_export)}")

    export_items = [
        ("markets_filtered", "Filtered Markets.xlsx"),
        ("events_df_selected", "Events.xlsx"),
        ("df_event_tags", "Events Tags.xlsx"),
    ]
    if include_volume_history:
        export_items = [
            ("daily_price_volume_features_df", "Daily Price Volume History.xlsx"),
            ("volume_history_df", "Volume History.xlsx"),
            ("volume_reconciliation_df", "Volume Reconciliation.xlsx"),
            ("market_coverage_summary_df", "Market Coverage Summary.xlsx"),
            ("market_missing_dates_df", "Market Missing Dates.xlsx"),
            *export_items,
        ]

    for key, filename in export_items:
        frame = results.get(key, pd.DataFrame())
        if isinstance(frame, pd.DataFrame):
            export_frame = prepare_excel_export_frame(frame)
            export_frame.to_excel(output_dir / filename, index=False)
            print(f"[export] wrote {filename} rows={len(export_frame)}")


def run_pipeline(
    *,
    tags_df: Optional[pd.DataFrame] = None,
    legacy_theme_name: str = "geopolitics",
    legacy_theme_overrides: Optional[dict] = None,
    legacy_theme_overrides_by_topic: Optional[dict[str, dict]] = None,
    run_ai: bool = True,
    export: bool = True,
    output_dir: Optional[Path] = None,
    include_volume_history: bool = True,
) -> dict:
    tags_df = tags_df.copy() if tags_df is not None else DEFAULT_TAGS_DF.copy()
    resolved_output_dir = output_dir or DEFAULT_OUTPUT_DIR
    print(
        "[pipeline] start "
        f"tags={len(tags_df)} "
        f"run_ai={run_ai} "
        f"export={export} "
        f"include_volume_history={include_volume_history} "
        f"output_dir={resolved_output_dir if export else 'disabled'}"
    )

    events_df_full, markets_df, prices_long_df = build_tables_from_tags_df(
        tags_df=tags_df,
        tag_id_col="tag_id",
        tag_name_col="tag_name",
        closed=False,
        active=True,
        limit=200,
        related_tags=True,
        keep_event_tags=True,
    )

    selected_universe = select_relevant_event_universe_multi_topic(
        events_df_full,
        markets_df,
        prices_long_df,
        tags_df,
        theme_overrides_by_topic=legacy_theme_overrides_by_topic,
    )

    history_results = build_history_and_feature_data(
        selected_universe["prices_long_df_filtered"],
        selected_universe["markets_filtered"],
        include_volume_history=include_volume_history,
    )

    ranking_results = build_ranked_markets(
        history_results["daily_features_df"],
        selected_universe["markets_filtered"],
    )

    payload_results = build_ai_payloads(ranking_results["ranked_markets"], tags_df)

    per_tag_results = {}
    if run_ai:
        for tag in range(tags_df.shape[0]):
            print(
                "[pipeline] "
                f"running_ai_for_tag={tags_df.loc[tag, 'tag_name']} "
                f"tag_id={tags_df.loc[tag, 'tag_id']}"
            )
            result = run_ai_for_tag_batched(
                tag_name=tags_df.loc[tag, "tag_name"],
                tag_id=tags_df.loc[tag, "tag_id"],
                markets_by_tag=payload_results["markets_by_tag"],
                events_by_tag=payload_results["events_by_tag"],
                client=client,
                model=deployment,
            )
            if result:
                per_tag_results[tags_df.loc[tag, "tag_name"]] = result

    df_markets, df_events = materialize_ai_results(per_tag_results)

    results = {
        "tags_df": tags_df,
        "events_df_full": events_df_full,
        "markets_df": markets_df,
        "prices_long_df": prices_long_df,
        **selected_universe,
        **history_results,
        **ranking_results,
        **payload_results,
        "per_tag_results": per_tag_results,
        "df_markets": df_markets,
        "df_events": df_events,
    }

    if export:
        export_pipeline_outputs(results, resolved_output_dir)

    print(
        "[pipeline] done "
        f"events_fetched={len(events_df_full)} "
        f"markets_fetched={len(markets_df)} "
        f"events_selected={len(results['events_df_selected'])} "
        f"markets_selected={len(results['markets_filtered'])} "
        f"top_markets={len(df_markets)} "
        f"top_events={len(df_events)}"
    )

    return results


def main() -> None:
    output_dir_env = os.getenv("POLYMARKET_OUTPUT_DIR")
    output_dir = Path(output_dir_env) if output_dir_env else DEFAULT_OUTPUT_DIR
    run_pipeline(export=True, output_dir=output_dir)


if __name__ == "__main__":
    main()


# ===== Notebook cell 30 =====

markets_filtered[markets_filtered['event_slug'] == 'trump-announces-end-of-military-operations-against-iran-by']


# ===== Notebook cell 31 =====

ranked_markets.sort_values('vol24_norm',ascending = False)


# ===== Notebook cell 32 =====
