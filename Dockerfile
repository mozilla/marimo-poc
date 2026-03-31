FROM python:3.12-slim

RUN pip install --no-cache-dir \
    marimo \
    marimo[sql] \
    sqlalchemy-bigquery \
    google-cloud-bigquery \
    google-cloud-bigquery-storage \
    db-dtypes \
    pandas \
    pyarrow \
    molabel \
    altair

WORKDIR /home/user/notebooks

EXPOSE 2718
