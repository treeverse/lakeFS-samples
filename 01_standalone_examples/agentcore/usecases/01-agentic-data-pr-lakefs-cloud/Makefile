# Agentic Data PRs with Amazon Bedrock AgentCore and lakeFS Cloud.
#
# Quickstart:
#   make configure && make setup && make check
#   make deploy && make seed && make run
#   make test-policy-denial
#   make show-pr PR_ID=<id>
#   make approve PR_ID=<id>
#   make cleanup

PY ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
export PYTHONPATH := .

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

$(BIN)/python:
	$(PY) -m venv $(VENV)

.PHONY: setup
setup: $(BIN)/python ## Create venv and install locked dependencies + local packages
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -r requirements.lock
	$(BIN)/pip install -e ".[agent,ui,dev]"
	@echo "Setup complete. Activate with: source $(BIN)/activate"

.PHONY: configure
configure: ## Interactively configure lakeFS + AWS and write a gitignored .env
	$(BIN)/python -m scripts.configure || $(PY) -m scripts.configure

.PHONY: check
check: ## Verify AWS, Bedrock, lakeFS, repository, branches and APIs are ready
	$(BIN)/python -m scripts.check || $(PY) -m scripts.check

.PHONY: deploy
deploy: ## Provision the AWS AgentCore stack (Secrets, Lambda, Gateway, Policy, Runtime)
	$(BIN)/python -m scripts.deploy || $(PY) -m scripts.deploy

.PHONY: seed
seed: ## Create disposable baseline + workspace branches and seed synthetic data
	$(BIN)/python -m scripts.seed || $(PY) -m scripts.seed

.PHONY: run
run: ## Invoke the deployed AgentCore agent for the current run
	$(BIN)/python -m scripts.run || $(PY) -m scripts.run

.PHONY: run-local
run-local: ## Run curation deterministically (no AWS/Bedrock) and create the PR
	$(BIN)/python -m scripts.run --local || $(PY) -m scripts.run --local

.PHONY: show-status
show-status: ## Show the current run's state and source-branch integrity
	$(BIN)/python -m scripts.status || $(PY) -m scripts.status

.PHONY: show-pr
show-pr: ## Show a Data PR (make show-pr PR_ID=<id>)
	$(BIN)/python -m scripts.show_pr $(PR_ID) || $(PY) -m scripts.show_pr $(PR_ID)

.PHONY: test-policy-denial
test-policy-denial: ## Prove AgentCore Policy denies the merge tool (live Gateway call)
	$(BIN)/python -m scripts.policy_denial || $(PY) -m scripts.policy_denial

.PHONY: approve
approve: ## Human approval: merge the Data PR (make approve PR_ID=<id>)
	$(BIN)/python -m scripts.approve $(PR_ID) || $(PY) -m scripts.approve $(PR_ID)

.PHONY: reject
reject: ## Human rejection: close the Data PR without merging (make reject PR_ID=<id>)
	$(BIN)/python -m scripts.reject $(PR_ID) || $(PY) -m scripts.reject $(PR_ID)

.PHONY: cleanup
cleanup: ## Delete all resources this run created; preserve repo and source branch
	$(BIN)/python -m scripts.cleanup || $(PY) -m scripts.cleanup

.PHONY: demo
demo: ## Launch the Streamlit demo UI
	$(BIN)/streamlit run ui/app.py || streamlit run ui/app.py

.PHONY: test
test: ## Run unit tests (live tests are opt-in; see `make test-live`)
	$(BIN)/python -m pytest -q || $(PY) -m pytest -q

.PHONY: test-live
test-live: ## Run opt-in live integration tests (requires live lakeFS + AWS)
	$(BIN)/python -m pytest -q -m live || $(PY) -m pytest -q -m live

.PHONY: fmt
fmt: ## Format and lint with ruff
	$(BIN)/ruff format . && $(BIN)/ruff check --fix . || (ruff format . && ruff check --fix .)

.PHONY: regen-expected
regen-expected: ## Regenerate seed/expected-results.json from the corpus declarations
	$(BIN)/python -m seed.generate || $(PY) -m seed.generate
