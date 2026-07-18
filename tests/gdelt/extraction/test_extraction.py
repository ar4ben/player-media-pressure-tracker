import json
from datetime import datetime
from pathlib import Path

import pandas as pd

import pipelines.gdelt.config as config
import pipelines.gdelt.storage_layout as layout
from pipelines.gdelt.extraction.runner import ExtractionRunner
from pipelines.gdelt.extraction.schema import GKG_COLUMNS


def make_run_context(
    tmp_path: Path,
    start_date: str = "2025-01-01",
    end_date: str = "2025-01-01",
    streams: list[str] | None = None,
) -> config.RunContext:
    paths = config.StoragePaths(
        lake_root=tmp_path / "lake",
        log_root=tmp_path / "logs",
    )

    return config.RunContext(
        run_id="test-run",
        start_date=start_date,
        end_date=end_date,
        streams=streams or ["regular"],
        stage="extraction",
        paths=paths,
        log_dir=paths.build_stage_log_dir("test-run", "extraction"),
    )


def make_gkg_row(
    record_id: str,
    gdelt_datetime: str,
    **values: str,
) -> str:
    row = {column: "" for column in GKG_COLUMNS}
    row["GKGRECORDID"] = record_id
    row["DATE"] = gdelt_datetime
    row.update(values)

    return "\t".join(row[column] for column in GKG_COLUMNS)


def write_gkg_csv(path: Path, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def test_extraction_runner_filters_candidates_and_writes_parquet(tmp_path):
    context = make_run_context(
        tmp_path,
        streams=["regular", "translation"],
    )
    context.log_dir.mkdir(parents=True)

    regular_dir = context.paths.build_tmp_gdelt_stream_dir(
        datetime(2025, 1, 1).date(), "regular"
    )
    translation_dir = context.paths.build_tmp_gdelt_stream_dir(
        datetime(2025, 1, 1).date(), "translation"
    )

    write_gkg_csv(
        regular_dir / "20250101000000.gkg.csv",
        [
            make_gkg_row(
                "regular-candidate",
                "20250101000000",
                Persons="KYLIAN MBAPPE",
                SourceCommonName="example.com",
            ),
            make_gkg_row(
                "regular-noncandidate",
                "20250101001500",
                Persons="OTHER PLAYER",
            ),
            make_gkg_row(
                "regular-invalid-date",
                "bad-date",
                Persons="KYLIAN MBAPPE",
            ),
        ],
    )
    write_gkg_csv(
        translation_dir / "20250101003000.translation.gkg.csv",
        [
            make_gkg_row(
                "translation-candidate",
                "20250101003000",
                DocumentIdentifier="https://example.com/mbapp-news",
            ),
        ],
    )
    write_gkg_csv(
        regular_dir / "ignored.tmp",
        [
            make_gkg_row(
                "ignored-tmp-candidate",
                "20250101004500",
                Persons="KYLIAN MBAPPE",
            )
        ],
    )

    ExtractionRunner(context).run()

    output_df = pd.read_parquet(
        context.paths.build_path(layout.BRONZE_GDELT_MENTIONS)
    )
    output_df = output_df.sort_values("GKGRECORDID").reset_index(drop=True)

    assert output_df["GKGRECORDID"].tolist() == [
        "regular-candidate",
        "translation-candidate",
    ]
    assert output_df["stream"].tolist() == ["regular", "translation"]
    assert output_df["gkg_date"].astype(str).tolist() == [
        "2025-01-01",
        "2025-01-01",
    ]

    run_summary = json.loads(
        (context.log_dir / "run_summary.json").read_text(encoding="utf-8")
    )
    assert run_summary["status"] == "success"
    assert run_summary["total_output_rows"] == 2
    assert run_summary["invalid_datetime_rows_skipped"] == 1
