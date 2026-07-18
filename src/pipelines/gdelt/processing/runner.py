import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit

import pandas as pd

import pipelines.gdelt.processing.salience as salience
import pipelines.gdelt.processing.summary as summary
import pipelines.gdelt.storage_layout as layout
from pipelines.gdelt.config import UnscopedRunContext
from pipelines.gdelt.processing.schema import (
    BASE_OUTPUT_COLS,
    INPUT_COLS,
    SALIENCE_COLS,
)

logger = logging.getLogger(__name__)


def canonicalize_url(value: str) -> str:
    parts = urlsplit(value.strip())
    host = parts.netloc.lower()
    if host.startswith("www."):
        host = host.removeprefix("www.")

    path = parts.path.rstrip("/")
    canonical_url = f"{host}{path}" if host else path
    if parts.query:
        canonical_url = f"{canonical_url}?{parts.query}"

    return canonical_url


@dataclass
class ProcessingRunner:
    """Run GDELT processing for candidate rows."""

    context: UnscopedRunContext

    def run(self) -> None:
        """Parse language and tone, compute salience scores."""

        run_started_at = datetime.now(timezone.utc)
        run_status = "success"
        run_error_message: str | None = None
        processed_df: pd.DataFrame = pd.DataFrame()

        logger.info("Run GDELT GKG processing")

        try:
            processed_df = self._process_candidates_rows()
            self._write_parquet(processed_df)
        except Exception as e:
            run_status = "failed"
            run_error_message = str(e)
            logger.exception("Run failed")
            raise
        finally:
            run_finished_at = datetime.now(timezone.utc)

            summary.write_run_summary(
                context=self.context,
                status=run_status,
                error_message=run_error_message,
                run_started_at=run_started_at,
                run_finished_at=run_finished_at,
                df=processed_df,
            )

    def _parse_source_language(self, value) -> str:
        text = str(value).strip().lower()
        match = re.search(r"\bsrclc:([a-z]{2,3})\b", text)

        return match.group(1) if match else "unknown"

    def _add_source_language_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df["source_language"] = "eng"

        translated_articles = df["stream"].eq("translation")
        source_languages = df.loc[translated_articles, "TranslationInfo"].map(
            self._parse_source_language
        )

        df.loc[translated_articles, "source_language"] = source_languages

        return df

    def _deduplicate_articles_by_canonical_url(self, df: pd.DataFrame) -> pd.DataFrame:
        candidate_rows = len(df)
        df["canonical_url"] = df["DocumentIdentifier"].map(canonicalize_url)
        df = (
            df.sort_values(
                ["gkg_datetime", "stream", "GKGRECORDID", "DocumentIdentifier"],
            )
            .drop_duplicates("canonical_url", keep="first")
            .reset_index(drop=True)
        )
        logger.info(
            f"Deduplicated articles by canonical URL: "
            f"{candidate_rows - len(df)} duplicates removed, {len(df)} rows retained"
        )

        return df

    def _process_candidates_rows(self) -> pd.DataFrame:
        bronze_mentions_path = self.context.paths.build_path(
            layout.BRONZE_GDELT_MENTIONS
        )
        df = pd.read_parquet(bronze_mentions_path, columns=INPUT_COLS)

        deduplicated_df = self._deduplicate_articles_by_canonical_url(df)
        deduplicated_df["tone"] = pd.to_numeric(
            deduplicated_df["V2Tone"].str.split(",", n=1).str[0],
            errors="coerce",
        )
        deduplicated_df = self._add_source_language_columns(deduplicated_df)

        salience_df = deduplicated_df.apply(salience.compute, axis=1)

        result = pd.concat([deduplicated_df, salience_df], axis=1)
        result["gkg_date"] = pd.to_datetime(result["gkg_date"].astype("string")).dt.date

        return result[BASE_OUTPUT_COLS + SALIENCE_COLS]

    def _write_parquet(self, df: pd.DataFrame) -> None:
        silver_articles_path = self.context.paths.build_path(
            layout.SILVER_GDELT_ARTICLES
        )
        file_path = silver_articles_path / "gdelt_articles.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_path, index=False)
