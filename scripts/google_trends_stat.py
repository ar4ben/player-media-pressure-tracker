import pipelines.cli as cli
import pipelines.google_trends.application as application


def main() -> None:
    cli.run(application.run)


if __name__ == "__main__":
    main()
