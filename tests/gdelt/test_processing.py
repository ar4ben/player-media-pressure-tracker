from pathlib import Path

import pandas as pd

import pipelines.gdelt.storage_layout as layout
from pipelines.gdelt.processing import salience
from pipelines.gdelt.config import (
    StoragePaths,
    UnscopedRunContext,
)
from pipelines.gdelt.processing.runner import ProcessingRunner
from pipelines.gdelt.processing.schema import BASE_OUTPUT_COLS, INPUT_COLS, SALIENCE_COLS


def make_unscoped_context(tmp_path: Path) -> UnscopedRunContext:
    paths = StoragePaths(
        lake_root=tmp_path / "lake",
        log_root=tmp_path / "logs",
    )
    log_dir = paths.log_root / "processing" / "run=test-run"
    log_dir.mkdir(parents=True)

    return UnscopedRunContext(
        run_id="test-run",
        paths=paths,
        log_dir=log_dir,
    )


def make_input_row(**values: object) -> dict[str, object]:
    row: dict[str, object] = {column: "" for column in INPUT_COLS}
    row.update(
        {
            "GKGRECORDID": "record-id",
            "DATE": "20250101000000",
            "gkg_datetime": "2025-01-01T00:00:00",
            "gkg_date": "2025-01-01",
            "stream": "regular",
            "SourceCommonName": "example.com",
            "DocumentIdentifier": "https://example.com/article",
            "V2Tone": "1.5,2,3",
        }
    )
    row.update(values)

    return row


def write_bronze_mentions(
    context: UnscopedRunContext,
    rows: list[dict[str, object]],
) -> None:
    bronze_mentions_path = context.paths.build_path(layout.BRONZE_GDELT_MENTIONS)
    bronze_mentions_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=INPUT_COLS).to_parquet(
        bronze_mentions_path,
        index=False,
    )


def test_processing_runner_writes_silver_and_summary_for_regular_and_translation(
    tmp_path,
):
    context = make_unscoped_context(tmp_path)
    write_bronze_mentions(
        context,
        [
            make_input_row(
                GKGRECORDID="regular-candidate",
                stream="regular",
                V2Persons="Kylian Mbappe,10;Other Player,200",
                V2Tone="1.5,2,3",
            ),
            make_input_row(
                GKGRECORDID="translation-candidate",
                stream="translation",
                TranslationInfo="srclc:spa;eng:English",
                DocumentIdentifier="https://example.com/mbappe-transfer",
                AllNames="Kylian Mbappe,1000;Other Entity,10",
                V2Tone="-2.5,2,3",
            ),
        ],
    )

    ProcessingRunner(context).run()

    output_path = (
        context.paths.build_path(layout.SILVER_GDELT_ARTICLES)
        / "gdelt_articles.parquet"
    )
    output_df = pd.read_parquet(output_path).sort_values("GKGRECORDID")

    assert output_df.columns.tolist() == BASE_OUTPUT_COLS + SALIENCE_COLS
    assert output_df["GKGRECORDID"].tolist() == [
        "regular-candidate",
        "translation-candidate",
    ]
    assert output_df["source_language"].tolist() == ["eng", "spa"]
    assert output_df["tone"].tolist() == [1.5, -2.5]
    assert output_df["salience_class"].tolist() == [
        "high_salience",
        "medium_salience",
    ]


def test_processing_runner_sets_unknown_source_language_for_translation_without_code(
    tmp_path,
):
    context = make_unscoped_context(tmp_path)
    write_bronze_mentions(
        context,
        [
            make_input_row(
                GKGRECORDID="translation-without-language",
                stream="translation",
                TranslationInfo="malformed metadata",
            ),
        ],
    )

    ProcessingRunner(context).run()

    output_df = pd.read_parquet(
        context.paths.build_path(layout.SILVER_GDELT_ARTICLES)
        / "gdelt_articles.parquet"
    )

    assert output_df.loc[0, "source_language"] == "unknown"


def test_salience_rank_score_uses_same_entity_source_as_player_mentions():
    result = salience.compute(
        pd.Series(
            {
                "DocumentIdentifier": "https://example.com/article",
                "V2Persons": "Other Person,100",
                "AllNames": (
                    "Kylian Mbappe,10;Kylian Mbappe,20;"
                    "Kylian Mbappe,30;Other Entity,40"
                ),
            }
        )
    )

    assert result["player_mentions"] == 3
    assert result["max_mentions_of_any_person"] == 3
    assert result["rank_score"] == 1.0
    assert 0.0 <= result["salience_score"] <= 1.0
