#!/usr/bin/env bash
# Run the EDA script.
#   bash code/run_eda.sh sample                     # 1% sample, local Spark
#   bash code/run_eda.sh cluster [parquet_out_s3a]   # full file, spark-submit on the cluster
set -euo pipefail
cd "$(dirname "$0")/.."

INPUT="s3a://dsan6000-final/nyc-shelters/intake_log.csv"
OUT="outputs/eda_out"
MODE="${1:-sample}"

if [ "$MODE" = "sample" ]; then
  # Reading s3a:// locally needs the hadoop-aws package and AWS credentials in your environment.
  spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
    code/nyc_shelters_eda.py --input "$INPUT" --out "$OUT" --sample 0.01
else
  PARQUET_OUT="${2:-}"
  EXTRA=()
  if [ -n "$PARQUET_OUT" ]; then EXTRA=(--write-parquet "$PARQUET_OUT"); fi
  spark-submit \
    --deploy-mode client \
    code/nyc_shelters_eda.py --input "$INPUT" --out "$OUT" "${EXTRA[@]}"
fi

echo "Done. Paste $OUT/summary.md back into the progress report."
