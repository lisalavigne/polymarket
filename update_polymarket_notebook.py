from __future__ import annotations

import json
from pathlib import Path


SOURCE_NOTEBOOK = Path("/Users/lisalavigne/Downloads/polymarket.ipynb")
TARGET_NOTEBOOK = Path(
    "/Users/lisalavigne/Documents/Codex/2026-04-20-files-mentioned-by-the-user-polymarket-2/polymarket_refined.ipynb"
)
HELPER_FILE = Path(
    "/Users/lisalavigne/Documents/Codex/2026-04-20-files-mentioned-by-the-user-polymarket-2/polymarket_llm_pipeline.py"
)


CELL_1 = """user = os.getenv('UBS_TNUMBER', '')
pwd = os.getenv('UBS_INET_PASSWORD', '')

def build_session(user: str = '', pwd: str = ''):
    proxy_session = requests.Session()
    if user and pwd:
        proxy_url = f'http://{user}:{pwd}@inet-proxy-b.adns.ubs.net:8080'
        proxy_session.proxies = {'http': proxy_url, 'https': proxy_url}
        try:
            test = proxy_session.get(
                'https://gamma-api.polymarket.com/events',
                params={'limit': 1, 'closed': 'false', 'related_tags': 'true'},
                timeout=10,
            )
            test.raise_for_status()
            print('Using UBS proxy-backed session.')
            return proxy_session
        except Exception as exc:
            print(f'Proxy session unavailable, falling back to direct internet: {exc}')

    print('Using direct internet session.')
    return requests.Session()

SESSION = build_session(user, pwd)

from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider

AZURE_OPENAI_ENDPOINT = os.getenv('AZURE_OPENAI_ENDPOINT')
OPENAI_API_VERSION = os.getenv('OPENAI_API_VERSION')
deployment = os.getenv('AZURE_OPENAI_DEPLOYMENT', 'gpt-5.2')

client = None
if AZURE_OPENAI_ENDPOINT and OPENAI_API_VERSION:
    credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    token_provider = get_bearer_token_provider(
        credential,
        'https://cognitiveservices.azure.com/.default'
    )
    client = AzureOpenAI(
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=OPENAI_API_VERSION,
        azure_ad_token_provider=token_provider,
        max_retries=12,
    )
    print(f'Azure OpenAI client ready for deployment: {deployment}')
else:
    print('Azure OpenAI env vars missing: set AZURE_OPENAI_ENDPOINT and OPENAI_API_VERSION before running AI cells.')
"""


CELL_19 = """from pathlib import Path
import sys

HELPER_DIR = Path(r"/Users/lisalavigne/Documents/Codex/2026-04-20-files-mentioned-by-the-user-polymarket-2")
if str(HELPER_DIR) not in sys.path:
    sys.path.append(str(HELPER_DIR))

from polymarket_llm_pipeline import (
    build_event_candidates,
    prepare_market_candidates,
    run_batched_event_selection,
    run_batched_market_selection,
    run_event_summary_pass,
    run_market_summary_pass,
    bulletize_columns,
)

event_candidates = build_event_candidates(ranked_markets, top_k_markets=4)
market_candidates = prepare_market_candidates(ranked_markets)

event_candidates[[
    "event_slug",
    "event_title",
    "event_candidate_score",
    "best_market_score",
    "event_volume24hr",
    "event_liquidity",
]].head()
"""


CELL_20 = """selection_by_tag = {}

tag_lookup = tags_df[["tag_id", "tag_name"]].copy()
tag_lookup["tag_id"] = tag_lookup["tag_id"].astype(str)

for row in tag_lookup.itertuples(index=False):
    tag_id = str(row.tag_id)
    tag_name = row.tag_name

    tag_events = event_candidates[event_candidates["source_tag_id"].astype(str) == tag_id].copy()
    tag_markets = market_candidates[market_candidates["source_tag_id"].astype(str) == tag_id].copy()

    if tag_events.empty and tag_markets.empty:
        continue

    selected_events = run_batched_event_selection(
        tag_events,
        tag_name=tag_name,
        client=client,
        model=deployment,
        batch_size=18,
        per_batch_top_k=5,
        final_top_k=10,
    )

    selected_markets = run_batched_market_selection(
        tag_markets[tag_markets["event_slug"].isin(selected_events["event_slug"])].copy(),
        tag_name=tag_name,
        client=client,
        model=deployment,
        batch_size=24,
        per_batch_top_k=6,
        final_top_k=10,
    )

    selection_by_tag[tag_name] = {
        "events": selected_events,
        "markets": selected_markets,
    }

{tag: {"events": payload["events"].shape, "markets": payload["markets"].shape} for tag, payload in selection_by_tag.items()}
"""


CELL_21 = """per_tag_results = {}

for tag_name, payload in selection_by_tag.items():
    selected_events = payload["events"]
    selected_markets = payload["markets"]

    event_summaries = run_event_summary_pass(
        selected_events,
        tag_name=tag_name,
        client=client,
        model=deployment,
        batch_size=5,
    )

    market_summaries = run_market_summary_pass(
        selected_markets,
        tag_name=tag_name,
        client=client,
        model=deployment,
        batch_size=8,
    )

    per_tag_results[tag_name] = {
        "events": event_summaries.to_dict("records"),
        "markets": market_summaries.to_dict("records"),
    }

list(per_tag_results.keys())
"""


CELL_22 = """df_raw = pd.DataFrame.from_dict(per_tag_results, orient="index")

if df_raw.empty:
    df_markets = pd.DataFrame()
    df_events = pd.DataFrame()
else:
    df_raw = df_raw[df_raw["markets"].apply(lambda x: isinstance(x, list) and len(x) > 0)]
    df_markets = pd.DataFrame(df_raw["markets"].explode().tolist())
    df_events = pd.DataFrame(df_raw["events"].explode().tolist())

list_columns_events = [
    "ubs_channels",
    "shock_type",
    "recommended_actions",
    "follow_ups",
    "top_markets_driving_signal",
]
list_columns_markets = [
    "ubs_channels",
    "shock_type",
    "recommended_actions",
    "follow_ups",
]

df_markets = bulletize_columns(df_markets, list_columns_markets)
df_events = bulletize_columns(df_events, list_columns_events)

{
    "df_markets_shape": df_markets.shape,
    "df_events_shape": df_events.shape,
}
"""


CELL_23 = """import pandas as pd
from pathlib import Path

output_dir = Path('/domino/edv/GS_CUSOERM_LVOVERRIDE_RW')
output_filename = ['AI Market Pick 10.xlsx', 'AI Event Pick 10.xlsx']
sheet_name = ['markets', 'events']
frames = [df_markets, df_events]
wrap_columns = [list_columns_markets, list_columns_events]

for i in range(len(output_filename)):
    with pd.ExcelWriter(output_dir / output_filename[i], engine='xlsxwriter') as writer:
        frames[i].to_excel(writer, index=False, sheet_name=sheet_name[i])

        workbook = writer.book
        worksheet = writer.sheets[sheet_name[i]]
        wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})

        for idx, column in enumerate(frames[i].columns):
            if column in wrap_columns[i]:
                worksheet.set_column(idx, idx, 40, wrap_format)

    print(f"Data exported to {output_filename[i]}")
"""


def replace_code_cell(nb: dict, idx: int, source: str) -> None:
    nb["cells"][idx]["cell_type"] = "code"
    nb["cells"][idx]["metadata"] = {}
    nb["cells"][idx]["execution_count"] = None
    nb["cells"][idx]["outputs"] = []
    nb["cells"][idx]["source"] = [line + "\n" for line in source.rstrip("\n").split("\n")]


def main() -> None:
    nb = json.loads(SOURCE_NOTEBOOK.read_text())
    replace_code_cell(nb, 1, CELL_1)
    replace_code_cell(nb, 19, CELL_19)
    replace_code_cell(nb, 20, CELL_20)
    replace_code_cell(nb, 21, CELL_21)
    replace_code_cell(nb, 22, CELL_22)
    replace_code_cell(nb, 23, CELL_23)
    TARGET_NOTEBOOK.write_text(json.dumps(nb, indent=1))
    print(f"Wrote {TARGET_NOTEBOOK}")
    print(f"Helper module available at {HELPER_FILE}")


if __name__ == "__main__":
    main()
