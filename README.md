# Marimo POC

Marimo notebooks connected to BigQuery, running locally via Docker.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and **running**
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) installed

## Usage

```bash
./run.sh
```

The script will:
1. Prompt for your GCP project ID if not set
2. Authenticate to GCP if needed (`gcloud auth application-default login`)
3. Build the Docker image on first run
4. Start Marimo and open it in your browser

Your notebooks are saved to the `./notebooks/` folder and persist between sessions.

To stop: `docker compose down`

## Querying BigQuery

```python
import marimo as mo
from google.cloud import bigquery
import os

client = bigquery.Client(project=os.environ["GOOGLE_CLOUD_PROJECT"])
df = client.query("SELECT * FROM `project.dataset.table` LIMIT 100").to_dataframe()
mo.ui.table(df)
```

A working example is available at `notebooks/bigquery_example.py`.

## Updating

```bash
git pull
docker compose build
docker compose up
```
