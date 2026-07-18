import pipelines.gdelt.cli as cli


def main() -> None:
    cli.run_stage(stage="cleanup")


if __name__ == "__main__":
    main()
