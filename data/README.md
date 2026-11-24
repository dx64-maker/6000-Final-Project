# Data

The full dataset stays on S3 and is never committed to git:

- Raw OLTP log: `s3://dsan6000-final/nyc-shelters/intake_log.csv` (Spark: `s3a://...`), about 3.8 GB
- Codebook: `../docs/dataset/intakes_codebook.csv`
- Cleaned Parquet staging layer (written by `code/nyc_shelters_eda.py --write-parquet`): `s3a://<your-bucket>/nyc-shelters/parquet/`, partitioned by `year` and `month`

Small local samples for testing can go in this folder; `.gitignore` keeps them out of the repo.
