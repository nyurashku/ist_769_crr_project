.PHONY: pull start stop logs ingest-% run-pipeline notebook

COMPOSE = docker compose

# Convenience targets for the dev stack
pull:
		$(COMPOSE) pull

start:
		$(COMPOSE) up -d

stop:
		$(COMPOSE) down

logs:
		$(COMPOSE) logs -f

# Data-workflow placeholders
ingest-%:
		docker exec spark-master python /opt/work/src/ingest/download_lmp.py $*
		docker exec spark-master python /opt/work/src/ingest/download_crr.py $*

run-pipeline:
		docker exec spark-master python /opt/work/src/pipeline/enrich_join.py

notebook:
		@echo "Open http://localhost:8888 in your browser (tokenless)."