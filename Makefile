UV ?= uv
PYTHON ?= python3
PACKAGE ?= grapharrow
SRC_DIRS := src tests
ANTLR_JAR ?=
CYPHER_GENERATED := src/grapharrow/interfaces/opencypher/generated/vendor/opencypher

.PHONY: help sync sync-dev lint format format-check typecheck test test-unit test-interface-comparison test-integration test-integration-live coverage check cli-help server-help bolt-help db-init db-show-catalog db-show-snapshot db-verify db-recover-dry-run db-bulk-add-nodes db-bulk-add-edges db-import-nodes db-import-edges serve bolt benchmark-backends benchmark-cypher-engine benchmark-tabular-json benchmark-ana-sessions clean
.PHONY: generate-cypher-parser
.PHONY: benchmark-cypher-columnar
.PHONY: version

help:
	@printf "Available targets:\n"
	@printf "  make sync         Install runtime dependencies with uv\n"
	@printf "  make sync-dev     Install development dependencies with uv\n"
	@printf "  make lint         Run Ruff lint checks\n"
	@printf "  make format       Format the repository with Ruff\n"
	@printf "  make format-check Check formatting with Ruff\n"
	@printf "  make typecheck    Run mypy on the source tree\n"
	@printf "  make generate-cypher-parser ANTLR_JAR=/path/to/antlr-4.13.2-complete.jar\n"
	@printf "  make test         Run the test suite\n"
	@printf "  make test-unit    Run the unit test suite\n"
	@printf "  make test-integration Run the integration test suite\n"
	@printf "  make test-integration-live Run live object-storage integration tests when env vars are configured\n"
	@printf "  make coverage     Run the test suite with coverage\n"
	@printf "  make check        Run lint, typecheck, and tests\n"
	@printf "  make cli-help     Show the GraphArrow CLI help\n"
	@printf "  make server-help  Show the HTTP server help (REST and ANA)\n"
	@printf "  make bolt-help    Show the GraphArrow Bolt help\n"
	@printf "  make db-init DB=path             Initialize a local database path\n"
	@printf "  make db-show-catalog DB=path     Print the current catalog\n"
	@printf "  make db-show-snapshot DB=path    Print the current snapshot\n"
	@printf "  make db-verify DB=path           Verify the current database state\n"
	@printf "  make db-recover-dry-run DB=path  Run recovery in dry-run mode\n"
	@printf "  make db-bulk-add-nodes DB=path FILE=input.json [DEFAULT_LABEL=Label]\n"
	@printf "  make db-bulk-add-edges DB=path FILE=input.json [SOURCE_LABEL=Label SOURCE_KEY=key TARGET_LABEL=Label TARGET_KEY=key]\n"
	@printf "  make db-import-nodes DB=path FILE=input.csv [DEFAULT_LABEL=Label]\n"
	@printf "  make db-import-edges DB=path FILE=input.parquet [SOURCE_LABEL=Label SOURCE_KEY=key TARGET_LABEL=Label TARGET_KEY=key]\n"
	@printf "  make serve DB=path               Run the HTTP server (REST and ANA)\n"
	@printf "  make bolt DB=path [HOST=127.0.0.1 PORT=18080] Run the Bolt server\n"
	@printf "  make benchmark-backends          Run the backend comparison benchmark\n"
	@printf "  make benchmark-cypher-engine     Run the Cypher engine benchmark\n"
	@printf "  make benchmark-cypher-columnar   Compare composable row and columnar reads\n"
	@printf "  make benchmark-tabular-json      Compare json vs orjson tabular serialization\n"
	@printf "  make benchmark-ana-sessions      Measure ANA session latency and memory\n"
	@printf "  make test-interface-comparison   Compare public graph interfaces and save results\n"
	@printf "  make version                     Show the checkout version\n"
	@printf "  make version BUMP=major|minor|patch  Create the next stable tag locally\n"
	@printf "  make version BUMP=tag VERSION=X.Y.Z  Create a specific stable tag locally\n"
	@printf "  make clean        Remove local tool caches\n"

version:
	$(if $(strip $(BETA)),$(error Use a final version bump. Nightly builds run once a week))
	@"$(PYTHON)" .github/version.py "$(or $(BUMP),show)" --version "$(VERSION)"

sync:
	$(UV) sync --no-dev

sync-dev:
	$(UV) sync

lint:
	$(UV) run ruff check .

format:
	$(UV) run ruff format .

format-check:
	$(UV) run ruff format --check .

typecheck:
	$(UV) run mypy src

generate-cypher-parser:
	@test -n "$(ANTLR_JAR)" || (echo "Set ANTLR_JAR to the ANTLR 4.13.2 complete JAR path"; exit 1)
	java -jar "$(ANTLR_JAR)" -Dlanguage=Python3 -visitor -no-listener -Xexact-output-dir -lib vendor/opencypher -o "$(CYPHER_GENERATED)" vendor/opencypher/GraphArrowCypher.g4

test:
	$(UV) run pytest

test-interface-comparison:
	GRAPHARROW_INTERFACE_COMPARISON_REPORT=/tmp/grapharrow-interface-comparison.json $(UV) run pytest -v tests/integration/test_interface_comparison.py --junitxml=/tmp/grapharrow-interface-comparison.xml

test-unit:
	$(UV) run pytest tests/unit

test-integration:
	$(UV) run pytest -m integration tests/integration

test-integration-live:
	$(UV) run pytest -m "integration and live_storage" tests/integration

coverage:
	$(UV) run pytest --cov=grapharrow tests

check: lint typecheck test

cli-help:
	$(UV) run grapharrow --help

server-help:
	$(UV) run grapharrow-server --help

bolt-help:
	$(UV) run grapharrow-bolt --help

db-init:
	$(UV) run grapharrow init $(DB)

db-show-catalog:
	$(UV) run grapharrow show-catalog $(DB)

db-show-snapshot:
	$(UV) run grapharrow show-snapshot $(DB)

db-verify:
	$(UV) run grapharrow verify $(DB)

db-recover-dry-run:
	$(UV) run grapharrow recover $(DB)

db-bulk-add-nodes:
	$(UV) run grapharrow bulk-add-nodes $(DB) $(FILE) $(if $(DEFAULT_LABEL),--default-label $(DEFAULT_LABEL),)

db-bulk-add-edges:
	$(UV) run grapharrow bulk-add-edges $(DB) $(FILE) $(if $(SOURCE_LABEL),--source-label $(SOURCE_LABEL),) $(if $(SOURCE_KEY),--source-key $(SOURCE_KEY),) $(if $(TARGET_LABEL),--target-label $(TARGET_LABEL),) $(if $(TARGET_KEY),--target-key $(TARGET_KEY),)

db-import-nodes:
	$(UV) run grapharrow import-nodes $(DB) $(FILE) $(if $(DEFAULT_LABEL),--default-label $(DEFAULT_LABEL),)

db-import-edges:
	$(UV) run grapharrow import-edges $(DB) $(FILE) $(if $(SOURCE_LABEL),--source-label $(SOURCE_LABEL),) $(if $(SOURCE_KEY),--source-key $(SOURCE_KEY),) $(if $(TARGET_LABEL),--target-label $(TARGET_LABEL),) $(if $(TARGET_KEY),--target-key $(TARGET_KEY),)

serve:
	$(UV) run grapharrow-server --path $(DB)

bolt:
	$(UV) run grapharrow-bolt --path $(DB) --host $(or $(HOST),127.0.0.1) --port $(or $(PORT),18080) $(BOLT_ARGS)

benchmark-backends:
	$(UV) run --locked --group benchmark python scripts/benchmark_backends.py

benchmark-cypher-engine:
	$(UV) run python scripts/benchmark_cypher_engine.py

benchmark-cypher-columnar:
	$(UV) run python scripts/benchmark_cypher_columnar.py --directory /tmp/grapharrow-columnar-data --output /tmp/grapharrow-columnar-results.json $(COLUMNAR_BENCH_ARGS)

benchmark-ana-sessions:
	$(UV) run python scripts/benchmark_ana_sessions.py

benchmark-tabular-json:
	$(UV) run python scripts/benchmark_tabular_json.py

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache
