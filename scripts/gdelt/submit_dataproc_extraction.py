import pipelines.gdelt.cli as cli


def main() -> None:
    cli.submit_dataproc_extraction()


if __name__ == "__main__":
    main()
