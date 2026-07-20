"""
transform.py — Cleans the raw ingested job postings with PySpark.

Reads:  data/raw/*.json          (written by ingest.py)
Writes: data/processed/jobs.parquet   (clean, deduped, analysis-ready)
        data/processed/jobs.csv       (same data, for Power BI / Excel)

What "clean" means here:
  - Dedupe by jobId (Apify can occasionally return the same posting twice
    across paginated searches).
  - Normalize seniority into one bucket per posting (junior / mid / senior /
    unspecified), reading it from the job title first (titles say "Junior"
    or "Senior" far more reliably than LinkedIn's own experienceLevel field,
    which is "Not Applicable" on ~40% of postings) and falling back to
    experienceLevel only when the title gives no signal.
  - Strip a stray emoji/decoration some job titles ship with (e.g. a title
    literally wrapped in orange-circle emoji) so titles group correctly.
  - Extract a clean city name out of the free-text location field.
  - Keep the Apify skill-match fields (keywordMatchScorePercentage,
    matchedKeywords, unmatchedKeywords) as-is — they're already computed
    against your real skill list at ingest time.

The title/seniority/city normalization logic itself lives in normalize.py,
not here, so it can be unit-tested (see tests/test_normalize.py) without
paying PySpark's JVM startup cost.

Run:
  python transform.py
  python transform.py --input data/raw/some_other_snapshot.json
"""

import argparse
import glob
import json
import logging
import os
from pathlib import Path

# Avoids a "Java gateway exited" crash on machines/containers where the
# hostname doesn't resolve (common in sandboxes/CI). Harmless elsewhere.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from normalize import clean_title, derive_seniority, extract_city

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"


def latest_raw_file() -> Path:
    """Return the most recently written raw snapshot, or exit with a clear
    error if ingest.py hasn't been run yet."""
    files = sorted(glob.glob(str(RAW_DIR / "*.json")))
    if not files:
        raise SystemExit(f"No raw files found in {RAW_DIR}. Run ingest.py first.")
    return Path(files[-1])


def main(input_path: Path) -> None:
    try:
        with open(input_path, encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        raise SystemExit(f"Input file not found: {input_path}") from None
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{input_path} is not valid JSON ({exc}). Was the ingest run interrupted?") from exc

    items = raw["items"]
    logger.info("Loaded %d raw records from %s", len(items), input_path.name)
    if not items:
        logger.warning("Input snapshot has zero items — nothing to transform.")

    spark = SparkSession.builder.appName("warsaw-data-engineer-jobs").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.createDataFrame(items)

        clean_title_udf = F.udf(clean_title, StringType())
        derive_seniority_udf = F.udf(derive_seniority, StringType())
        extract_city_udf = F.udf(extract_city, StringType())

        before = df.count()
        df = df.dropDuplicates(["jobId"])
        after = df.count()
        logger.info("Deduped by jobId: %d -> %d rows (%d duplicates removed)", before, after, before - after)

        df = (
            df.withColumn("jobTitleClean", clean_title_udf(F.col("jobTitle")))
            .withColumn("seniority", derive_seniority_udf(F.col("jobTitle"), F.col("experienceLevel")))
            .withColumn("city", extract_city_udf(F.col("location")))
        )

        keep_cols = [
            "jobId",
            "jobTitleClean",
            "companyName",
            "city",
            "seniority",
            "contractType",
            "workType",
            "sector",
            "publishedAt",
            "keywordMatchScorePercentage",
            "matchedKeywords",
            "unmatchedKeywords",
            "jobUrl",
        ]
        df_clean = df.select(*[c for c in keep_cols if c in df.columns])

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        parquet_path = PROCESSED_DIR / "jobs.parquet"
        csv_path = PROCESSED_DIR / "jobs.csv"

        df_clean.write.mode("overwrite").parquet(str(parquet_path))

        # Spark writes CSV as a directory of part-files by default; coalesce
        # to one file and flatten array columns (matchedKeywords etc.) to
        # strings first since plain CSV can't hold nested arrays.
        df_csv = df_clean.withColumn(
            "matchedKeywords", F.concat_ws("|", F.col("matchedKeywords"))
        ).withColumn("unmatchedKeywords", F.concat_ws("|", F.col("unmatchedKeywords")))
        tmp_csv_dir = PROCESSED_DIR / "_jobs_csv_tmp"
        df_csv.coalesce(1).write.mode("overwrite").option("header", True).csv(str(tmp_csv_dir))
        part_file = next(tmp_csv_dir.glob("part-*.csv"))
        part_file.replace(csv_path)
        for leftover in tmp_csv_dir.glob("*"):
            leftover.unlink()
        tmp_csv_dir.rmdir()

        logger.info("Wrote %s", parquet_path)
        logger.info("Wrote %s", csv_path)

        logger.info("Seniority breakdown:")
        df_clean.groupBy("seniority").count().orderBy(F.desc("count")).show(truncate=False)

        logger.info("Avg skill-match score by seniority:")
        df_clean.groupBy("seniority").avg("keywordMatchScorePercentage").orderBy("seniority").show(
            truncate=False
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Path to a specific raw JSON file. Defaults to the newest file in data/raw/.",
    )
    args = parser.parse_args()

    main(args.input or latest_raw_file())
