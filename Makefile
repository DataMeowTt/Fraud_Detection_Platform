.DEFAULT_GOAL := help

.PHONY: help up down reset benchmark

help:
	@echo "Available targets:"
	@echo "  make up         - docker compose up + build + submit Flink job   (scripts/start-all.sh)"
	@echo "  make down       - truncate ClickHouse + docker compose down      (scripts/stop-all.sh)"
	@echo "  make reset      - wipe ClickHouse/Kafka/checkpoints, rebuild job (infra/reset.sh)"
	@echo "  make benchmark  - run stress test load + collect metrics        (scripts/run-benchmark.sh)"
	@echo "                    forward args: make benchmark ARGS=\"--levels 500,1000 --duration 30\""

up:
	./scripts/start-all.sh

down:
	./scripts/stop-all.sh

reset:
	cd infra && ./reset.sh

benchmark:
	./scripts/run-benchmark.sh $(ARGS)
