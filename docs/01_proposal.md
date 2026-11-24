# DSAN 6000 Final Project Proposal: From Intake Log to Insight — NYC Shelter Intakes at Scale

## 1. Team

- **[Your Name]** (NetID: [netid])
- [Teammate name, if any — or delete this line for a solo project]

## 2. Dataset

**NYC Shelter Intake Reports (DSAN 6000–exclusive synthetic data)** — the course's own dataset, described at jjacobs.me/dsan6000/final-project/nyc-shelters/.

| | |
|---|---|
| **Location** | `s3://dsan6000-final/nyc-shelters/intake_log.csv` (read from Spark as `s3a://dsan6000-final/nyc-shelters/intake_log.csv`) |
| **Format** | A single append-only CSV: the OLTP log of every intake, written as each one reaches the server |
| **Size** | About 3.8 GB raw. Too large to work with comfortably in memory on a laptop, especially once text features and embeddings are added |
| **Schema** | 26 columns, per the course codebook (`intakes_codebook.csv`) |
| **Data types** | Numeric (`age`, `children_count`, `reported_housing_instability_days`, `follow_up_days`, `note_word_count`), datetime (`intake_date`, `follow_up_target_date`), categorical (`household_type`, `program_type`, `presenting_reason`, `employment_status_reported`, `income_status_reported`, `primary_barrier`, `potential_referral`, `referral_status`, `client_goal`, …), multi-valued (`immediate_needs`, `;`-delimited), boolean (`housing_eligibility_verified`), **free text** (`intake_note`), and image (`intake_photo_b64`, a Base64 photo) |

**Scale and feasibility.** We expect much of the 3.8 GB to come from the Base64 photo column. Our first Spark job will read the CSV once on a multi-node cluster (EMR or a standalone Spark cluster on several EC2 instances). It will drop or separate the photo column, apply types, and write **Parquet partitioned by intake year/month**. Every later stage reads that Parquet, which should be much smaller and faster than the raw CSV. The free-text `intake_note` gives us the text component the course recommends, alongside many structured fields.

## 3. Extended Abstract

**Motivation.** The Midterm vignette describes how NYC's current shelter reporting works. Staff enter counts into spreadsheets, and the system publishes a daily PDF of aggregate totals that is overwritten every 24 hours. That makes it almost impossible to ask *why* people come to shelters or *what happens to them afterwards*. The new intake log records much more: why each client arrived, what they need, which referral they were offered, and a narrative note written by an Intake Specialist. Our project turns that raw log into an analytics-ready warehouse. We then use Spark MLlib, Spark NLP and MLflow to answer questions that could help the Department of Homeless Services plan services.

**Question 1: OLTP → OLAP (ETL and data quality).** We will turn the raw append-only CSV into a **star schema** in Parquet. The fact table will be `fact_contact`, with one row per Initial Intake or Followup. Dimension tables will cover client, program type, presenting reason, referral and date. We will also explode `immediate_needs` into a bridge table. Planned data-quality checks:

- inconsistent categorical spellings and new `program_type` values (the codebook notes that new types may appear)
- linking each `Followup` to its `Initial Intake`
- impossible values, such as negative day counts or a follow-up date earlier than the intake date
- making sure the records stay synthetic (`synthetic_data = true`)

**Question 2: What brings people in, and how is it changing? (Large-scale EDA).** Using Spark SQL on the warehouse, we will measure:

- trends over time in `presenting_reason` and `prior_living_situation_reported`, broken down by `household_type`
- the distribution of `reported_housing_instability_days`
- which `immediate_needs` occur together most often, as a basket-style co-occurrence analysis
- demand by `program_type`

The results will be a set of report-ready visualizations, the modern version of the daily PDF.

**Question 3: What do the intake notes say that the dropdowns don't? (Spark NLP).** We will build a Spark NLP pipeline on `intake_note`: DocumentAssembler → Tokenizer → Normalizer/Lemmatizer → StopWordsCleaner → sentence embeddings. On top of it we will:

- **(a)** fit LDA topic models (MLlib) to find themes that the categorical fields don't capture
- **(b)** train a text classifier that predicts `presenting_reason` from the note alone

Wherever the classifier confidently disagrees with the dropdown, that record becomes a candidate data-entry error. This is an automated version of the manual coherence check described on the dataset page.

**Question 4: Which intakes are likely to reach a completed referral? (Spark MLlib).** We will predict `referral_status == 'Completed'` (or a related outcome, such as whether housing eligibility was verified by follow-up). Features will be the structured fields plus the text features from Q3. The pipeline is StringIndexer → OneHotEncoder → VectorAssembler, feeding logistic regression, random forest and gradient-boosted trees. We will pay attention to the `maxBins` limit on high-cardinality categoricals and to class imbalance. Every run, with its parameters, metrics and model artifact, will be tracked in **MLflow** so the model comparison can be reproduced.

**Deliverables and timeline.**

| Due | Deliverable |
|---|---|
| Nov 22 | EDA 1-pager: schema validation, missingness, first distributions |
| Nov 29 | Spark 1-pager: full ETL to partitioned Parquet on the cluster, with runtime and cost notes |
| Dec 6–7 | Slides and final presentation: NLP and MLlib results with an MLflow comparison |
| Report | Final written report |

**Ethical note.** The data is synthetic, but it imitates highly sensitive records. We will not try to re-identify anyone. We will not use the photo column for facial analysis. We will report on errors and fairness across household types, not only accuracy.

## 4. Questions for Course Staff

1. Do `Followup` rows reuse the original `intake_id`, or is there another key that links a follow-up to its initial intake?
2. Is `intake_photo_b64` meant to be used for anything, or is it fine to drop it during ETL? (It seems to account for much of the file size.)
3. The codebook has no shelter/location column, unlike the Midterm OLTP sample. Is location intentionally left out, or will it be added?
4. What cluster set-up and budget do you expect for the Spark run (EMR vs. EC2 standalone, number of nodes, instance types)?
5. Is there a preferred way to share MLflow tracking (a local server on the master node, or S3-backed artifacts) for grading?
