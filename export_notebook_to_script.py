from __future__ import annotations

import json
from pathlib import Path


SOURCE_NOTEBOOK = Path("/Users/lisalavigne/Downloads/polymarket.ipynb")
TARGET_SCRIPT = Path(
    "/Users/lisalavigne/Documents/Codex/2026-04-20-files-mentioned-by-the-user-polymarket-2/polymarket_pipeline.py"
)


def main() -> None:
    nb = json.loads(SOURCE_NOTEBOOK.read_text())
    parts: list[str] = []

    parts.append("# Auto-generated from polymarket.ipynb and then refined for script use.\n")
    parts.append("from __future__ import annotations\n")

    for idx, cell in enumerate(nb.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        parts.append(f"\n# ===== Notebook cell {idx} =====\n")
        parts.append(source.rstrip() + "\n")

    TARGET_SCRIPT.write_text("\n".join(parts))
    print(f"Wrote {TARGET_SCRIPT}")


if __name__ == "__main__":
    main()
