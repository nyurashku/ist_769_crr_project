.PHONY: help pull start stop logs ingest-% run-pipeline notebook

# ----------------------------------------------------------------------
# compose helpers
COMPOSE        = docker compose
SPARK_EXEC_BSH = docker exec -it spark-master bash -c
SPARK_USER_BSH = docker exec -it -u sparkuser spark-master bash -c

# ----------------------------------------------------------------------
# generic help
help:
	@echo "Common targets:"
	@echo "  make pull            – docker compose pull"
	@echo "  make start           – docker compose up -d"
	@echo "  make stop            – docker compose down"
	@echo "  make logs            – live stack logs"
	@echo "  make ingest-YYYY-MM  – download LMP+CRR files into HDFS"
	@echo "  make run-pipeline    – Spark bronze→silver/enrich job"
	@echo "  make notebook        – open Jupyter hint"

# ----------------------------------------------------------------------
# dev-stack lifecycle
pull:
	$(COMPOSE) pull

start:
	$(COMPOSE) up -d

stop:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

# ----------------------------------------------------------------------
# data-workflow conveniences
# Usage:  make ingest-2024-02   or   make ingest-2023-12 MARKET=RTM
MARKET ?= DAM         # override on command line if needed

ingest-%:   ## pattern rule – $* expands to the YYYY-MM part
	$(SPARK_USER_BSH) \
	  'source /etc/profile.d/hadoop.sh && \
	   python /opt/work/src/ingest/download_lmp.py $* --market $(MARKET)'

	$(SPARK_USER_BSH) \
	  'source /etc/profile.d/hadoop.sh && \
	   python /opt/work/src/ingest/download_crr.py $* --market $(MARKET)'

# ----------------------------------------------------------------------
# spark SQL / ETL pipeline – runs the parse → silver + enrichment step
run-pipeline:
	$(SPARK_USER_BSH) \
	  'spark-submit --master local[*] \
	      /opt/work/src/etl/parse_lmp_silver.py \
	      --market $(MARKET) --year 2024'

# ----------------------------------------------------------------------
# quick link to the notebook server
notebook:
	@echo ""
	@echo "🔗  Open →  http://localhost:8888  (tokenless, courtesy of compose)"
	@echo ""