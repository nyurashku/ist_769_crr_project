# CAISO CRR vs LMP Hedging Analysis

## Overview
This project evaluates how well Congestion Revenue Rights (CRRs) hedge real-time congestion costs in CAISO by
comparing CRR auction clearing prices with realised nodal price differences (sink − source).

## Technologies
- **HDFS** – raw data lake  
- **Apache Spark 3** (PySpark) – ETL & analytics  
- **Apache Cassandra 5** – curated time-series store  
- **Python 3.10** – orchestration scripts & notebooks  
- **Docker / docker-compose** – reproducible local stack  

## Data sources
| Feed | Endpoint | Granularity |
|------|----------|-------------|
| Locational Marginal Prices (LMP) | CAISO OASIS `PRC_LMP` / `PRC_INTVL_LMP` | ≤ 5-min |
| Congestion Revenue Rights (CRR)  | CAISO CRR Auction REST `/marketData`     | monthly / annual |

## Repository layout
├── README.md
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── src/
│   ├── ingest/
│   │   ├── download_lmp.py
│   │   └── download_crr.py
│   ├── pipeline/
│   │   ├── clean_lmp.py
│   │   ├── clean_crr.py
│   │   └── enrich_join.py
│   └── utils/
├── notebooks/
│   └── eda_crr_vs_lmp.ipynb
└── data/               # (KEEP EMPTY – large files live in HDFS)

## Quick start
```bash
# clone and enter
git clone https://github.com/nyurashku/ist_769_crr_project.git
cd ist_769_crr_project

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt        # deps

docker-compose up -d                   # spin up HDFS, Spark, Cassandra, Jupyter
make ingest-jan-2025                   # sample month ingestion

Primary Make targets

Target
Description
ingest-<month>
Download raw OASIS & CRR data to HDFS
backfill-year
Ingest an entire calendar year
run-pipeline
Spark cleaning → Cassandra
notebook
Launch Jupyter Lab

Contributing workflow
	1.	Branch: git checkout -b feature/<topic>
	2.	Commit small logical changes.
	3.	Push and open a Pull Request.

License

MIT


