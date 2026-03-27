#!/bin/bash
set -e

MARIMO_URL="http://localhost:2718"

# ── helpers ──────────────────────────────────────────────────────────────────

log()  { echo "[marimo-poc] $*"; }
err()  { echo "[marimo-poc] ERROR: $*" >&2; exit 1; }

check_dependency() {
    command -v "$1" &>/dev/null || err "$1 is required but not installed. See README.md."
}

open_browser() {
    if command -v open &>/dev/null; then        # macOS
        open "$MARIMO_URL"
    elif command -v xdg-open &>/dev/null; then  # Linux
        xdg-open "$MARIMO_URL"
    else
        log "Open $MARIMO_URL in your browser."
    fi
}

# ── checks ────────────────────────────────────────────────────────────────────

check_dependency docker
check_dependency gcloud

if ! docker info &>/dev/null; then
    err "Docker is not running. Please start Docker Desktop and try again."
fi

# ── env ───────────────────────────────────────────────────────────────────────

if [ ! -f .env ]; then
    if [ ! -f .env.example ]; then
        err ".env.example not found. Are you running this from the marimo-poc directory?"
    fi
    cp .env.example .env
    log "Created .env from .env.example."
fi

source .env

if [ -z "$GOOGLE_CLOUD_PROJECT" ]; then
    read -rp "[marimo-poc] Enter your GCP project ID: " GOOGLE_CLOUD_PROJECT
    sed -i.bak "s/^GOOGLE_CLOUD_PROJECT=.*/GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT/" .env
    rm -f .env.bak
    log "Saved project ID to .env."
fi

export GOOGLE_CLOUD_PROJECT

# ── auth ──────────────────────────────────────────────────────────────────────

ADC_FILE="$HOME/.config/gcloud/application_default_credentials.json"

if [ ! -f "$ADC_FILE" ]; then
    log "No GCP credentials found. Starting authentication..."
    gcloud auth application-default login
else
    log "GCP credentials found."
fi

# ── docker ────────────────────────────────────────────────────────────────────

if ! docker compose images marimo 2>/dev/null | grep -q marimo; then
    log "Building Docker image (first run, this may take a minute)..."
    docker compose build
fi

log "Starting Marimo..."
docker compose up -d

# ── wait for marimo to be ready ───────────────────────────────────────────────

log "Waiting for Marimo to be ready..."
for i in $(seq 1 20); do
    if curl -sf "$MARIMO_URL" &>/dev/null; then
        break
    fi
    sleep 1
    if [ "$i" -eq 20 ]; then
        err "Marimo did not start in time. Run 'docker compose logs' to debug."
    fi
done

# ── open ──────────────────────────────────────────────────────────────────────

log "Marimo is ready at $MARIMO_URL"
open_browser

log "To stop: docker compose down"
