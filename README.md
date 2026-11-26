# DSAN 6000 Final Project — NYC Shelter Intakes at Scale

**Team:** Dun Xie (dx64)
**Dataset:** NYC Shelter Intake Reports (DSAN 6000 synthetic data), `s3a://dsan6000-final/nyc-shelters/intake_log.csv` (~3.8 GB CSV, 26 columns)
**Course page:** https://jjacobs.me/dsan6000/final-project/

## Folder layout

```
dsan6000-final-project/
├── README.md                  this file
├── requirements.txt           Python packages
├── .gitignore                 keeps data and big outputs out of git
├── code/
│   ├── nyc_shelters_eda.py    PySpark EDA (milestone 2)
│   └── run_eda.sh             how to run it locally or on the cluster
├── docs/
│   ├── 01_proposal.md         milestone 1: proposal (submitted Nov 15)
│   ├── 02_progress_report_eda.md   milestone 2: EDA report (fill [brackets] from eda_out/summary.md)
│   └── dataset/intakes_codebook.csv  course codebook (26 columns)
├── data/                      local samples only; never commit (see data/README.md)
├── outputs/eda_out/           script output: summary.md, tables/*.csv, figures/*.png
└── report/                    Quarto manuscript for the final report (GitHub Pages)
    ├── _quarto.yml
    ├── index.qmd
    ├── references.bib
    └── figures/
```

## Milestones

| Due | Deliverable | Where |
|---|---|---|
| Sun Nov 15 | Proposal | `docs/01_proposal.md` |
| Sun Nov 22 | Progress report (EDA) | `docs/02_progress_report_eda.md` + `code/nyc_shelters_eda.py` |
| Sun Nov 29 | Results (Spark cluster usage) | `docs/03_spark_cluster_report.md` (to do) |
| Sun Dec 6 | Presentation slides | `slides/` (to do) |
| Dec 7 / Dec 10 | Final presentation | |
| Sun Dec 13 | Final report: Quarto manuscript on GitHub Pages, with PDF | `report/` |

## Quick start

```bash
pip install -r requirements.txt

# quick test on a 1% sample
bash code/run_eda.sh sample

# full run on the cluster (EMR master node)
bash code/run_eda.sh cluster s3a://<your-bucket>/nyc-shelters/parquet/
```

Copy `outputs/eda_out/figures/*.png` to `report/figures/` when you use them in the final report.

## Final report (Quarto manuscript)

```bash
cd report
quarto preview          # local preview
quarto publish gh-pages # publishes to <gh_username>.github.io/<repo-name>
```
