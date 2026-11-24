# Progress Report (EDA): NYC Shelter Intake Log

**Team:** [Your Name] ([netid]) · **Dataset:** `s3a://dsan6000-final/nyc-shelters/intake_log.csv` · **Code:** `code/nyc_shelters_eda.py` (PySpark)

> **Before submitting:** every value in **[brackets]** comes from `outputs/eda_out/summary.md` after you run the script on the full file. Delete this note once they are filled in.

## 1. Progress since the proposal

- Read the raw append-only CSV into Spark using `multiLine` and quote-escaping, because the free-text `intake_note` field contains commas, quotes and line breaks. Checked the result against the 26 columns in the codebook.
- Measured how much of the file the Base64 photo column takes up, then dropped it from the analysis set (it is not needed for any research question).
- Cast numeric and date fields. Date parsing tries several formats, and the script counts any dates that still fail to parse instead of silently dropping them.
- Wrote the cleaned data as **Parquet partitioned by intake year/month**. This is the staging layer for the star schema in Q1; every later stage reads it instead of the 3.8 GB CSV.
- Ran the full EDA in Spark: missingness, numeric summaries, value counts for every categorical field, time coverage, cross-tabs for our research questions, and eleven data-quality checks. All tables are saved to CSV and all figures to PNG.

## 2. The data at a glance

| Measure | Value |
|---|---|
| Rows (contacts) | **[rows]** |
| Distinct `intake_id` | **[distinct_intake_id]** |
| Initial Intakes / Followups | **[from counts_contact_type]** |
| Intake date range | **[min_date] – [max_date]** |
| Share of raw file that is the photo column | **[photo_chars ÷ (photo_chars + note_chars + rest)] — the main reason the CSV is 3.8 GB** |
| Parquet size after dropping photos | **[check with `aws s3 ls --summarize --recursive`]** |
| Intake note length (words, 5th / 50th / 95th pct.) | **[note_word_count_p05_p50_p95]** |
| Full-pass runtime / cluster | **[runtime_seconds] s on [N × instance type]** |

## 3. Key findings

**Figure 1: Contacts per month by contact type** (`fig1_monthly_contacts.png`). [Describe the trend: steady, growing or seasonal? Do Followups lag Initial Intakes by roughly `follow_up_days`?] The weekly pattern (`day_of_week.csv`) shows [busiest / quietest day].

**Figure 2: Most common presenting reasons** (`fig2_presenting_reasons.png`). The top three reasons, [reason 1, 2, 3], account for **[x]%** of intakes. `presenting_reason` has **[k]** distinct values.

**Figure 5: Presenting reasons within each household type** (`fig5_reason_by_household.png`). Families with children are [more / less] likely than single adults to report [reason]. This supports our plan to model each household type separately, or with interaction terms.

**Figure 3 and Figure 6: Client age and days of housing instability** (`fig3_age_hist.png`, `fig6_instability_days.png`). Median age is **[x]** (IQR **[p25–p75]**). Instability days are [right-skewed, median **[x]**, 99th percentile **[x]**], so we will log-transform that field before modeling.

**Figure 4: Immediate needs** (`fig4_immediate_needs.png`). Clients list on average **[avg_needs_per_intake]** needs. The most common are [need 1, need 2, need 3], which suggests [an implication for services].

**Referral outcomes.** **[x]%** of contacts have `referral_status = Completed`. The rate ranges from **[x]%** to **[y]%** across household types (`completion_by_household.csv`). This is the target for Q4. If it is imbalanced, we will use class weights and judge models by AUC-PR rather than accuracy.

## 4. Data-quality issues and how we will handle them

| Check | Count | Plan |
|---|---|---|
| Unparseable `intake_date` | **[ ]** | Add the missing format; quarantine anything left |
| Follow-up date earlier than intake date | **[ ]** | Flag; exclude from time-to-follow-up analysis |
| `follow_up_target_date − intake_date ≠ follow_up_days` | **[ ]** | Trust the dates; keep `follow_up_days` as entered |
| `note_word_count` differs from the note's actual word count by more than 2 | **[ ]** | Recompute from the text |
| "Family with children" with 0 children / "Single adult" with children | **[ ] / [ ]** | Treat as data-entry conflicts; also feed into Q3's checks of whether notes and dropdowns agree |
| Age outside 0–110 / under 18 | **[ ] / [ ]** | Drop out-of-range values; check that under-18s are "Young adult" |
| Duplicate (`intake_id`, `contact_type`) pairs | **[ ]** | Deduplicate on all fields, then keep the latest |
| Followups whose `intake_id` matches an Initial Intake | **[ ] of [ ]** | Decides how `fact_contact` links follow-ups to intakes (Q1) |
| Columns with the most missing values | **[top 3 from missingness.csv]** | Add an explicit "Unknown" category for categoricals; median-impute numerics, with an indicator column |

## 5. What this changes for modeling

- **Categorical cardinality:** the highest is **[column = k]**. If any field has more than 32 levels, we will raise `maxBins` for the tree models (MLlib requires `maxBins` ≥ the largest number of categories) or group rare levels into "Other".
- **Text:** with a median note length of **[x]** words, sentence embeddings in Spark NLP are practical. A sample of notes suggests they repeat the dropdown fields in prose, which supports Q3's plan to compare the note with the presenting reason.
- **Leakage:** `referral_status` on Followup rows shows the outcome itself. Q4's features will come only from the Initial Intake row.

## 6. Next steps (Spark cluster report, Nov 29)

1. Build the star schema (`fact_contact`, the dimension tables, and a needs bridge table) from the Parquet staging layer.
2. Run the full pipeline on a multi-node cluster and record runtime, number of partitions and cost.
3. Prototype the Spark NLP pipeline on a 1% sample, and set up MLflow tracking for Q4's baseline models.
