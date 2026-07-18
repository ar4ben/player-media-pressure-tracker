import logging
import os
from datetime import datetime, timezone
from functools import reduce
from operator import or_

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import pipelines.gdelt.extraction.summary as summary
from pipelines.gdelt.config import RunContext
from pipelines.gdelt.extraction.schema import (
    CANDIDATE_TOKEN,
    GKG_COLUMNS,
    OUTPUT_COLUMNS,
    SEARCH_TEXT_COLUMNS,
)
from pipelines.gdelt.extraction.storage_policy import (
    ExtractionStorage,
    build_extraction_storage,
)

logger = logging.getLogger(__name__)


class ExtractionRunner:
    """Run GDELT extraction for a configured date range."""

    def __init__(
        self,
        context: RunContext,
        storage: ExtractionStorage | None = None,
    ) -> None:
        self.context = context
        self.storage: ExtractionStorage = storage or build_extraction_storage(
            context.mode
        )

    def run(self) -> None:
        """Extract candidate rows from GDELT GKG files for the run context."""

        run_started_at = datetime.now(timezone.utc)
        run_status = "success"
        run_error_message: str | None = None
        total_output_rows = 0
        invalid_datetime_rows_skipped = 0

        spark: SparkSession | None = None

        logger.info("Run GDELT GKG candidates extraction")

        try:
            csv_dir_list = self._get_csv_dir_list()
            spark = self._setup_spark()

            raw = self._read_raw_gkg(spark=spark, csv_dir_list=csv_dir_list)

            output_df, invalid_datetime_rows_skipped = self._build_output_df(raw=raw)

            self._write_output_df(output_df)

            total_output_rows = output_df.count()

        except Exception as e:
            run_status = "failed"
            run_error_message = str(e)
            logger.exception("Run failed")
            raise

        finally:
            run_finished_at = datetime.now(timezone.utc)

            summary.write_run_summary(
                context=self.context,
                run_started_at=run_started_at,
                run_finished_at=run_finished_at,
                status=run_status,
                error_message=run_error_message,
                total_output_rows=total_output_rows,
                invalid_datetime_rows_skipped=invalid_datetime_rows_skipped,
            )

            if spark:
                spark.stop()

    def _write_output_df(self, output_df: SparkDataFrame) -> None:
        logger.info("Writing parquet output")
        output_path = self.storage.get_bronze_output_path(self.context)

        (
            # Candidate output is small for expected backfill batches,
            # so coalesce(1) safely keeps the bronze layer from accumulating many tiny parquet files.
            output_df.coalesce(1)
            .write.mode("overwrite")
            .partitionBy("gkg_date")
            .parquet(output_path)
        )

    def _build_output_df(self, raw: SparkDataFrame) -> tuple[SparkDataFrame, int]:
        player_filter = reduce(
            or_,
            [F.col(col).ilike(CANDIDATE_TOKEN) for col in SEARCH_TEXT_COLUMNS],
        )

        filtered_df = (
            raw.filter(player_filter)
            .withColumn(
                "gkg_datetime", F.expr("try_to_timestamp(DATE, 'yyyyMMddHHmmss')")
            )
            .withColumn(
                "stream",
                F.regexp_extract(F.input_file_name(), r"stream=([^/]+)", 1),
            )
            .persist()
        )

        invalid_datetime_rows_skipped = filtered_df.filter(
            F.col("gkg_datetime").isNull()
        ).count()

        return (
            filtered_df.filter(F.col("gkg_datetime").isNotNull())
            .withColumn("gkg_date", F.to_date(F.col("gkg_datetime")))
            .select(*OUTPUT_COLUMNS),
            invalid_datetime_rows_skipped,
        )

    def _read_raw_gkg(
        self, spark: SparkSession, csv_dir_list: list[str]
    ) -> SparkDataFrame:
        return (
            spark.read.option("sep", "\t")
            .option("header", "false")
            .option("quote", "")
            .option("escape", "")
            .option("pathGlobFilter", "*.gkg.csv")
            .csv(csv_dir_list)
            .toDF(*GKG_COLUMNS)
        )

    def _setup_spark(self) -> SparkSession:
        spark = self._build_spark()
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        return spark

    def _build_spark(self) -> SparkSession:
        builder = SparkSession.builder.appName("extract-gdelt-gkg-candidates")

        if spark_master := os.getenv("GDELT_SPARK_MASTER"):
            builder = builder.master(spark_master)

        raw_conf = os.getenv("GDELT_SPARK_CONF", "")
        for item in filter(None, raw_conf.split(",")):
            key, sep, value = item.partition("=")
            if not sep:
                raise ValueError(f"Invalid GDELT_SPARK_CONF item: {item}")

            builder = builder.config(key.strip(), value.strip())

        return builder.getOrCreate()

    def _get_csv_dir_list(self) -> list[str]:
        return self.storage.get_csv_dir_list(self.context)
