import pipelines.gdelt.cli as cli


def main() -> None:
    cli.run_stage(stage="ingestion")


if __name__ == "__main__":
    main()
