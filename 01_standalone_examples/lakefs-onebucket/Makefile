PYTHON  := .venv/bin/python
PIP     := .venv/bin/pip
COMPOSE := docker compose

.PHONY: help setup check-onebucket up init demo logs down clean

help:
	@echo "lakeFS + OneBucket demo"
	@echo ""
	@echo "Typical flow:"
	@echo "  make setup            Install Python dependencies into .venv"
	@echo "  make check-onebucket  Validate OneBucket endpoint/credentials/bucket"
	@echo "  make up               Start lakeFS + Postgres in Docker"
	@echo "  make init             Create (or validate) the lakeFS repository"
	@echo "  make demo             Run the full demo workflow"
	@echo "  make logs             Stream container logs (Ctrl-C to stop)"
	@echo "  make down             Stop containers"
	@echo "  make clean            Stop containers, remove volumes and .venv"
	@echo ""
	@echo "Repo modes (set in .env):"
	@echo "  CREATE_LAKEFS_REPO=true   make init creates the repo (default)"
	@echo "  CREATE_LAKEFS_REPO=false  make init validates an existing repo"

setup:
	@echo "==> Creating Python virtual environment..."
	python3 -m venv .venv
	$(PIP) install --upgrade pip --quiet
	$(PIP) install -r requirements.txt
	@echo ""
	@echo "==> Done. Next steps:"
	@echo "    1. cp .env.example .env"
	@echo "    2. Edit .env with your OneBucket credentials"
	@echo "    3. make check-onebucket"

check-onebucket: _require-env
	$(PYTHON) scripts/check_onebucket.py

up: _require-env
	$(COMPOSE) up -d
	@echo ""
	@echo "==> lakeFS starting on http://localhost:8000"
	@echo "    Watch startup: make logs"
	@echo "    When healthy:  make init"

init: _require-env
	@# The admin user is created by the lakefs container itself (see docker-compose.yml).
	$(PYTHON) scripts/init_lakefs.py

demo: _require-env
	$(PYTHON) scripts/run_demo.py

logs:
	$(COMPOSE) logs -f

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v
	rm -rf .venv
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	find . -name ".DS_Store" -delete 2>/dev/null; true
	@echo "==> Cleaned."

# Internal guard — fails fast if .env is missing
_require-env:
	@test -f .env || (echo "ERROR: .env not found.\n  Run: cp .env.example .env  then edit it." && exit 1)
