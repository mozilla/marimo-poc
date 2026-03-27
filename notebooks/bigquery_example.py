import marimo

app = marimo.App(sql_output="native")


@app.cell
def _():
    import marimo as mo
    import os
    from google.cloud import bigquery

    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    client = bigquery.Client(project=project)
    mo.md(f"Connected to project: **{project}**")
    return client, mo


@app.cell
def _(mo):
    query_input = mo.ui.text_area(
        label="BigQuery SQL",
        value="SELECT 1 AS hello",
        full_width=True,
    )
    run_button = mo.ui.run_button(label="Run query")
    mo.vstack([query_input, run_button])
    return query_input, run_button


@app.cell
def _(client, mo, query_input, run_button):
    mo.stop(not run_button.value)

    try:
        df = client.query(query_input.value).to_dataframe()
        result = mo.ui.table(df)
    except Exception as e:
        result = mo.callout(mo.md(f"**Query error:** {e}"), kind="danger")

    result
    return


if __name__ == "__main__":
    app.run()
