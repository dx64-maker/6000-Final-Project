"""
DSAN 6000 Final Project -- Progress Report (EDA)
NYC Shelter Intake Log: exploratory data analysis with PySpark.

Usage (on the Spark cluster / EMR master node):
    spark-submit nyc_shelters_eda.py \
        --input s3a://dsan6000-final/nyc-shelters/intake_log.csv \
        --out ./eda_out \
        [--write-parquet s3a://<your-bucket>/nyc-shelters/parquet/]

Quick test on a laptop with a sample:
    python nyc_shelters_eda.py --input s3a://... --sample 0.01 --out ./eda_out

Outputs (in --out):
    summary.md   <- paste this back to fill in the report numbers
    summary.json
    tables/*.csv
    figures/*.png
"""
import argparse
import json
import os
import time

from pyspark.sql import SparkSession, functions as F, types as T

CATEGORICAL = [
    "household_type", "program_type", "contact_type", "presenting_reason",
    "prior_living_situation_reported", "employment_status_reported",
    "income_status_reported", "primary_barrier", "potential_referral",
    "referral_status", "client_goal", "housing_eligibility_verified",
    "staff_role", "synthetic_data",
]
NUMERIC = [
    "age", "children_count", "reported_housing_instability_days",
    "follow_up_days", "note_word_count",
]
DATE_COLS = ["intake_date", "follow_up_target_date"]
DATE_FORMATS = ["yyyy-MM-dd", "MM/dd/yyyy", "M/d/yyyy", "dd-MM-yyyy",
                "yyyy/MM/dd", "d MMMM yyyy", "MMMM d, yyyy", "dd MMM yyyy"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="s3a://dsan6000-final/nyc-shelters/intake_log.csv")
    p.add_argument("--out", default="eda_out")
    p.add_argument("--sample", type=float, default=None,
                   help="fraction of rows to sample (e.g. 0.01) for a quick run")
    p.add_argument("--write-parquet", default=None,
                   help="optional output path for cleaned, partitioned Parquet")
    p.add_argument("--top", type=int, default=15)
    return p.parse_args()


def multi_format_date(col):
    return F.coalesce(*[F.to_date(F.col(col), fmt) for fmt in DATE_FORMATS])


def to_rows(df):
    return [r.asDict() for r in df.collect()]


def save_table(df, out, name):
    pdf = df.toPandas()
    pdf.to_csv(os.path.join(out, "tables", f"{name}.csv"), index=False)
    return pdf


def md_table(pdf, max_rows=20):
    if pdf is None or len(pdf) == 0:
        return "_(empty)_\n"
    pdf = pdf.head(max_rows)
    cols = [str(c) for c in pdf.columns]
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for _, row in pdf.iterrows():
        vals = []
        for v in row.values:
            if isinstance(v, float):
                vals.append(f"{v:,.2f}")
            elif isinstance(v, int):
                vals.append(f"{v:,}")
            else:
                vals.append(str(v))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines) + "\n"


def main():
    a = parse_args()
    os.makedirs(os.path.join(a.out, "tables"), exist_ok=True)
    os.makedirs(os.path.join(a.out, "figures"), exist_ok=True)

    spark = (SparkSession.builder.appName("nyc-shelters-eda")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .config("spark.sql.ansi.enabled", "false")
             .getOrCreate())
    t0 = time.time()

    # ---------- 1. Load raw OLTP log ----------
    # intake_note is free text (commas, quotes, possibly newlines) -> multiLine + escape.
    raw = (spark.read
           .option("header", True)
           .option("multiLine", True)
           .option("escape", '"')
           .option("mode", "PERMISSIVE")
           .option("columnNameOfCorruptRecord", "_corrupt_record")
           .csv(a.input))
    if a.sample:
        raw = raw.sample(fraction=a.sample, seed=6000)

    expected = set(CATEGORICAL + NUMERIC + DATE_COLS +
                   ["intake_id", "client_display_name", "immediate_needs",
                    "intake_note", "intake_photo_b64"])
    missing_cols = sorted(expected - set(raw.columns))
    extra_cols = sorted(set(raw.columns) - expected - {"_corrupt_record"})

    # Photo column: measure its share of the file, then drop it from the working set.
    photo_len = F.length("intake_photo_b64") if "intake_photo_b64" in raw.columns else F.lit(0)
    note_len = F.length("intake_note") if "intake_note" in raw.columns else F.lit(0)

    df = raw.withColumn("_photo_chars", photo_len).withColumn("_note_chars", note_len)
    if "intake_photo_b64" in df.columns:
        df = df.drop("intake_photo_b64")
    for c in NUMERIC:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(T.DoubleType()))
    for c in DATE_COLS:
        if c in df.columns:
            df = df.withColumn(c + "_raw", F.col(c)).withColumn(c, multi_format_date(c))
    df = df.cache()

    summary = {"input": a.input, "sample_fraction": a.sample,
               "missing_expected_columns": missing_cols,
               "unexpected_columns": extra_cols}

    # ---------- 2. Size and keys ----------
    n = df.count()
    summary["rows"] = n
    summary["distinct_intake_id"] = df.select("intake_id").distinct().count()
    size = df.agg(F.sum("_photo_chars").alias("photo_chars"),
                  F.sum("_note_chars").alias("note_chars"),
                  F.avg("_photo_chars").alias("avg_photo_chars"),
                  F.avg("_note_chars").alias("avg_note_chars")).first().asDict()
    summary["text_and_photo_size"] = size

    # ---------- 3. Missingness ----------
    miss_exprs = [F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), 1)
                        .otherwise(0)).alias(c) for c in df.columns if not c.startswith("_")]
    miss = df.agg(*miss_exprs).first().asDict()
    miss_pdf = (spark.createDataFrame([(k, int(v or 0)) for k, v in miss.items()],
                                      ["column", "n_missing"])
                .withColumn("pct_missing", F.round(F.col("n_missing") / F.lit(n) * 100, 2))
                .orderBy(F.desc("n_missing")))
    miss_pdf = save_table(miss_pdf, a.out, "missingness")

    # ---------- 4. Numeric summaries ----------
    num_cols = [c for c in NUMERIC if c in df.columns]
    num_rows = []
    for c in num_cols:
        s = df.agg(F.count(c).alias("n"), F.mean(c).alias("mean"), F.stddev(c).alias("sd"),
                   F.min(c).alias("min"), F.max(c).alias("max")).first().asDict()
        q = df.approxQuantile(c, [0.25, 0.5, 0.75, 0.99], 0.001)
        q = q + [None] * (4 - len(q))
        num_rows.append({"column": c, **s, "p25": q[0], "median": q[1], "p75": q[2], "p99": q[3]})
    import pandas as pd
    num_pdf = pd.DataFrame(num_rows)
    num_pdf.to_csv(os.path.join(a.out, "tables", "numeric_summary.csv"), index=False)

    # ---------- 5. Categorical distributions ----------
    cat_md = {}
    cat_cardinality = {}
    for c in [c for c in CATEGORICAL if c in df.columns]:
        cat_cardinality[c] = df.select(c).distinct().count()
        vc = (df.groupBy(c).count()
              .withColumn("pct", F.round(F.col("count") / F.lit(n) * 100, 2))
              .orderBy(F.desc("count")))
        pdf = save_table(vc, a.out, f"counts_{c}")
        cat_md[c] = md_table(pdf, a.top)
    summary["categorical_cardinality"] = cat_cardinality

    # ---------- 6. Time coverage ----------
    dr = df.agg(F.min("intake_date").alias("min_date"), F.max("intake_date").alias("max_date"),
                F.sum(F.when(F.col("intake_date").isNull() & F.col("intake_date_raw").isNotNull(), 1)
                      .otherwise(0)).alias("unparsed_intake_date")).first().asDict()
    summary["date_range"] = {k: str(v) for k, v in dr.items()}
    sample_raw_dates = [r[0] for r in df.select("intake_date_raw").where(F.col("intake_date_raw").isNotNull())
                        .limit(5).collect()]
    summary["sample_raw_intake_dates"] = sample_raw_dates

    monthly = (df.withColumn("month", F.date_trunc("month", "intake_date").cast("date"))
               .groupBy("month", "contact_type").count().orderBy("month"))
    monthly_pdf = save_table(monthly, a.out, "monthly_by_contact_type")
    dow = (df.withColumn("dow", F.date_format("intake_date", "E")).groupBy("dow").count()
           .orderBy(F.desc("count")))
    dow_pdf = save_table(dow, a.out, "day_of_week")

    # ---------- 7. Cross-tabs relevant to the research questions ----------
    reason_by_hh = (df.groupBy("household_type", "presenting_reason").count()
                    .orderBy("household_type", F.desc("count")))
    reason_by_hh_pdf = save_table(reason_by_hh, a.out, "presenting_reason_by_household")

    referral = (df.groupBy("potential_referral", "referral_status").count()
                .orderBy("potential_referral", F.desc("count")))
    save_table(referral, a.out, "referral_status_by_referral")

    completed_by_hh = (df.groupBy("household_type")
                       .agg(F.count("*").alias("n"),
                            F.round(F.avg((F.col("referral_status") == "Completed").cast("int")) * 100, 2)
                            .alias("pct_completed"))
                       .orderBy(F.desc("n")))
    completed_by_hh_pdf = save_table(completed_by_hh, a.out, "completion_by_household")

    needs = (df.select(F.explode(F.split(F.col("immediate_needs"), ";")).alias("need"))
             .withColumn("need", F.lower(F.trim("need")))
             .where(F.col("need") != "")
             .groupBy("need").count().orderBy(F.desc("count")))
    needs_pdf = save_table(needs, a.out, "immediate_needs")
    needs_per_intake = df.select(F.size(F.split(F.col("immediate_needs"), ";")).alias("k")) \
        .agg(F.avg("k").alias("avg_needs_per_intake")).first()[0]
    summary["avg_needs_per_intake"] = needs_per_intake

    # ---------- 8. Data-quality checks ----------
    words = F.size(F.split(F.trim(F.col("intake_note")), r"\s+"))
    dq = df.agg(
        F.sum(F.when((F.col("age") < 0) | (F.col("age") > 110), 1).otherwise(0)).alias("age_out_of_range"),
        F.sum(F.when(F.col("age") < 18, 1).otherwise(0)).alias("age_under_18"),
        F.sum(F.when(F.col("children_count") < 0, 1).otherwise(0)).alias("negative_children"),
        F.sum(F.when((F.col("household_type") == "Family with children") & (F.col("children_count") == 0), 1)
              .otherwise(0)).alias("family_with_zero_children"),
        F.sum(F.when((F.col("household_type") == "Single adult") & (F.col("children_count") > 0), 1)
              .otherwise(0)).alias("single_adult_with_children"),
        F.sum(F.when(F.col("reported_housing_instability_days") < 0, 1).otherwise(0)).alias("negative_instability_days"),
        F.sum(F.when(F.col("follow_up_target_date") < F.col("intake_date"), 1).otherwise(0)).alias("followup_before_intake"),
        F.sum(F.when(F.datediff("follow_up_target_date", "intake_date") != F.col("follow_up_days"), 1)
              .otherwise(0)).alias("followup_days_mismatch"),
        F.sum(F.when(F.abs(words - F.col("note_word_count")) > 2, 1).otherwise(0)).alias("note_word_count_mismatch"),
        F.sum(F.when(F.lower(F.col("synthetic_data").cast("string")) != "true", 1).otherwise(0)).alias("synthetic_flag_not_true"),
        F.sum(F.when(F.col("_corrupt_record").isNotNull(), 1).otherwise(0)).alias("corrupt_csv_rows")
        if "_corrupt_record" in df.columns else F.lit(0).alias("corrupt_csv_rows"),
    ).first().asDict()
    dup = (df.groupBy("intake_id", "contact_type").count().where("count > 1").count())
    dq["duplicate_intake_id_contact_type"] = dup
    # Do follow-ups share the intake_id of an initial intake?
    init_ids = df.where(F.col("contact_type") == "Initial Intake").select("intake_id").distinct()
    fu = df.where(F.col("contact_type") == "Followup").select("intake_id").distinct()
    dq["followups"] = fu.count()
    dq["followups_matching_an_initial_intake_id"] = fu.join(init_ids, "intake_id").count()
    summary["data_quality"] = dq

    # Text: note length summary
    nq = df.approxQuantile("note_word_count", [0.05, 0.5, 0.95], 0.001) if "note_word_count" in df.columns else []
    summary["note_word_count_p05_p50_p95"] = nq
    summary["sample_notes"] = [r[0][:300] for r in df.select("intake_note")
                               .where(F.col("intake_note").isNotNull()).limit(3).collect()]

    # ---------- 9. Optional: write cleaned Parquet (OLAP staging layer) ----------
    if a.write_parquet:
        helper_cols = ["_photo_chars", "_note_chars", "_corrupt_record",
                       "intake_date_raw", "follow_up_target_date_raw"]
        (df.drop(*[c for c in helper_cols if c in df.columns])
           .withColumn("year", F.year("intake_date"))
           .withColumn("month", F.month("intake_date"))
           .write.mode("overwrite").partitionBy("year", "month").parquet(a.write_parquet))
        summary["parquet_written_to"] = a.write_parquet

    summary["runtime_seconds"] = round(time.time() - t0, 1)
    summary["spark"] = {"version": spark.version,
                        "default_parallelism": spark.sparkContext.defaultParallelism,
                        "executors": spark.sparkContext.getConf().get("spark.executor.instances", "n/a")}

    # ---------- 10. Figures ----------
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    def savefig(name):
        plt.tight_layout()
        plt.savefig(os.path.join(a.out, "figures", name), dpi=150)
        plt.close()

    if len(monthly_pdf):
        piv = monthly_pdf.pivot_table(index="month", columns="contact_type", values="count", aggfunc="sum").fillna(0)
        piv.plot(figsize=(8, 4))
        plt.title("Contacts per month, by contact type"); plt.ylabel("records"); plt.xlabel("")
        savefig("fig1_monthly_contacts.png")

    rp = pd.read_csv(os.path.join(a.out, "tables", "counts_presenting_reason.csv")).head(a.top)
    if len(rp):
        rp.iloc[::-1].plot.barh(x="presenting_reason", y="count", legend=False, figsize=(8, 5))
        plt.title("Most common presenting reasons"); plt.xlabel("intakes"); plt.ylabel("")
        savefig("fig2_presenting_reasons.png")

    if "age" in df.columns:
        edges, counts = df.select("age").where(F.col("age").isNotNull()).rdd.map(lambda r: r[0]).histogram(30)
        plt.figure(figsize=(8, 4)); plt.bar(edges[:-1], counts, width=edges[1] - edges[0], align="edge")
        plt.title("Age of primary client"); plt.xlabel("age"); plt.ylabel("intakes")
        savefig("fig3_age_hist.png")

    if len(needs_pdf):
        needs_pdf.head(a.top).iloc[::-1].plot.barh(x="need", y="count", legend=False, figsize=(8, 5))
        plt.title("Immediate needs (exploded from ';' list)"); plt.xlabel("mentions"); plt.ylabel("")
        savefig("fig4_immediate_needs.png")

    if len(reason_by_hh_pdf):
        top_reasons = rp["presenting_reason"].head(8).tolist()
        sub = reason_by_hh_pdf[reason_by_hh_pdf["presenting_reason"].isin(top_reasons)]
        piv = sub.pivot_table(index="presenting_reason", columns="household_type", values="count", aggfunc="sum").fillna(0)
        (piv.div(piv.sum(axis=0), axis=1) * 100).plot.barh(figsize=(8, 5))
        plt.title("Presenting reason share within each household type (%)"); plt.xlabel("% of household type")
        savefig("fig5_reason_by_household.png")

    if "reported_housing_instability_days" in df.columns:
        edges, counts = (df.select("reported_housing_instability_days")
                         .where(F.col("reported_housing_instability_days").isNotNull())
                         .rdd.map(lambda r: r[0]).histogram(40))
        plt.figure(figsize=(8, 4)); plt.bar(edges[:-1], counts, width=edges[1] - edges[0], align="edge")
        plt.title("Reported days of housing instability before intake"); plt.xlabel("days"); plt.ylabel("intakes")
        savefig("fig6_instability_days.png")

    # ---------- 11. Write summary ----------
    with open(os.path.join(a.out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2, default=str)

    md = ["# NYC Shelters EDA summary\n",
          "## Overview\n", "```json", json.dumps({k: v for k, v in summary.items()
                                                  if k not in ("sample_notes",)}, indent=2, default=str), "```\n",
          "## Missingness\n", md_table(miss_pdf, 30),
          "## Numeric columns\n", md_table(num_pdf, 10),
          "## Completion rate by household type\n", md_table(completed_by_hh_pdf),
          "## Immediate needs\n", md_table(needs_pdf, a.top),
          "## Day of week\n", md_table(dow_pdf),
          "## Sample notes (first 300 chars)\n"] + [f"> {s}\n" for s in summary["sample_notes"]]
    for c, t in cat_md.items():
        md += [f"## {c}\n", t]
    with open(os.path.join(a.out, "summary.md"), "w") as f:
        f.write("\n".join(md))
    print("\n".join(md))
    print(f"\nDone in {summary['runtime_seconds']}s. Outputs in {a.out}/")
    spark.stop()


if __name__ == "__main__":
    main()
