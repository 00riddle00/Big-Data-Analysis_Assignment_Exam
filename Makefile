# vim: set ft=make tw=100 nu noet ts=8 sw=8:
# =============================================================================
# Makefile — AIS Vessel Collision Detection
# =============================================================================
# Usage: make <target>
# Run `make help` to see all available targets.
# =============================================================================

PYTHON_VERSION := 3.13.13

IMAGE_NAME     := ais-collision-detection
CONTAINER_NAME := ais-collision

# Load environment variables from .env file if it exists
ifneq (,$(wildcard .env))
    include .env
    export
endif

.PHONY: help all deps data lint build run stop rmi \
        test clean clean-env distclean

# -----------------------------------------------------------------------------

help:
	@echo ""
	@echo "AIS Vessel Collision Detection"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  help        Show this help message"
	@echo "  all         Run full pipeline"
	@echo "  deps        Install Python dependencies via uv (incremental)"
	@echo "  data        Show instructions for downloading AIS data"
	@echo "  lint        Lint and format Python code (ruff + black)"
	@echo "  build       Build Docker image"
	@echo "  run         Run the container"
	@echo "  stop        Stop the container"
	@echo "  rmi         Stop container and remove Docker image"
	@echo "  test        Run unit tests"
	@echo "  clean       Remove generated output files"
	@echo "  clean-env   Remove Python virtual environment"
	@echo "  distclean   clean + clean-env + rmi"
	@echo ""

# -----------------------------------------------------------------------------

all:
	@echo "==> TODO: run full pipeline"

# -----------------------------------------------------------------------------

# Incremental dependency install using uv.
# Uses a stamp file to avoid unnecessary reinstalls — uv sync only runs
# if uv.lock is newer than .venv/.stamp.
deps: .venv/.stamp

.venv/.stamp: uv.lock
	@echo "==> Installing Python $(PYTHON_VERSION)"
	pyenv install -s $(PYTHON_VERSION)
	pyenv local $(PYTHON_VERSION)
	@echo "==> Creating virtual environment and syncing dependencies"
	uv venv
	uv sync --all-groups
	@date "+%F %T %Z" > $@
	@echo "==> Dependencies installed."

# -----------------------------------------------------------------------------

data:
	@echo ""
	@echo "Download the AIS dataset manually:"
	@echo ""
	@echo "  wget http://aisdata.ais.dk/aisdk-2021-12.zip"
	@echo "  unzip aisdk-2021-12.zip -d data_arch/"
	@echo ""
	@echo "Or paste the URL directly into your browser address bar."
	@echo "Place the resulting CSV files at: data_arch/"
	@echo ""

# -----------------------------------------------------------------------------

lint:
	@echo "==> Linting and formatting Python code"
	uv run ruff check . --fix
	uv run ruff check --select I --fix .
	uv run ruff format .
	uv run black --line-length=88 --preview \
		--enable-unstable-feature=string_processing .

# -----------------------------------------------------------------------------

build:
	@echo "==> TODO: build Docker image"

run:
	@echo "==> TODO: run container"

stop:
	@echo "==> TODO: stop container"

rmi: stop
	@echo "==> TODO: remove Docker image"

# -----------------------------------------------------------------------------

test:
	@echo "==> No tests defined."

# -----------------------------------------------------------------------------

clean:
	@echo "==> Removing generated output files"
	find outputs/ -type f ! -name '.gitkeep' -delete
	@echo "==> Done."

clean-env:
	@echo "==> Removing Python virtual environment"
	rm -rf .venv
	@echo "==> Removed .venv."

distclean: clean clean-env rmi
	@echo "==> distclean complete."
