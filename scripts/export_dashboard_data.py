import pipelines.dashboard.application as application
import pipelines.logging_config as logging_config


def main() -> None:
    logging_config.configure()
    application.run()


if __name__ == "__main__":
    main()
