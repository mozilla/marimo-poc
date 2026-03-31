"""Shared BigQuery connection for use in marimo notebooks.

Usage in a notebook cell:
    from bq import engine
    _df = mo.sql("SELECT ...", connection=engine)
"""
import os
from sqlalchemy import create_engine

project = os.environ["GOOGLE_CLOUD_PROJECT"]
engine = create_engine(f"bigquery://{project}")
