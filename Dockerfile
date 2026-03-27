FROM python:3.12-slim

RUN pip install --no-cache-dir \
    marimo \
    google-cloud-bigquery \
    google-cloud-bigquery-storage \
    db-dtypes \
    pandas \
    pyarrow

WORKDIR /home/user/notebooks

EXPOSE 2718
