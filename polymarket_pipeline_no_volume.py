from pathlib import Path

from polymarket_pipeline import DEFAULT_OUTPUT_DIR, run_pipeline


def main() -> None:
    output_dir = Path(DEFAULT_OUTPUT_DIR) / "no_volume"
    run_pipeline(
        export=True,
        output_dir=output_dir,
        include_volume_history=False,
    )


if __name__ == "__main__":
    main()
