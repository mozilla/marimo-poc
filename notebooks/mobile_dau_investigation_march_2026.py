import marimo

__generated_with = "0.21.1"
app = marimo.App(sql_output="native")


@app.cell
def _():
    import marimo as mo
    import os
    import altair as alt
    from google.cloud import bigquery

    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    client = bigquery.Client(project=project)
    mo.md(f"Connected to project: **{project}**")
    return alt, client, mo


@app.cell
def _(mo):
    os_version = mo.ui.number(value=-1, label="OS Version (-1 = all versions)")
    os_version
    return (os_version,)


@app.cell
def _(mo):
    mo.md("""
    # Mobile DAU Investigation — March 2026

    ## Section 1: Mobile DAU Overview

    Fenix DAU ex-Iran is flat YoY — we were growing at this time last year.
    The forecast divergence begins around mid-February.
    """)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    WITH actuals AS (
        SELECT
            submission_date,
            SUM(dau) AS dau
        FROM `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
        WHERE
            app_name IN ('Fenix', 'Firefox iOS', 'Focus Android', 'Focus iOS')
            AND submission_date >= '2025-11-01'
        GROUP BY 1
    ),

    actuals_ma AS (
        SELECT
            submission_date,
            dau,
            AVG(dau) OVER (
                ORDER BY submission_date
                ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
            ) AS dau_28ma
        FROM actuals
    ),

    forecast AS (
        SELECT
            submission_date,
            dau_28_ma AS forecast_28ma
        FROM `mozdata.analysis.browser_kpi_forecasts_2026`
        WHERE
            product = 'mobile'
            AND forecast_name = 'MAR forecast'
    )

    SELECT
        a.submission_date,
        a.dau AS daily_dau,
        a.dau_28ma,
        f.forecast_28ma
    FROM actuals_ma AS a
    LEFT JOIN forecast AS f
        USING (submission_date)
    WHERE a.submission_date >= '2025-12-14'
    ORDER BY 1
    """

    _df = client.query(_query).to_dataframe()
    _df["submission_date"] = _df["submission_date"].astype(str)

    _long = _df.melt(
        id_vars="submission_date",
        value_vars=["daily_dau", "dau_28ma", "forecast_28ma"],
        var_name="series",
        value_name="count",
    )

    _series_order = ["daily_dau", "dau_28ma", "forecast_28ma"]
    _stroke_dash = alt.condition(
        alt.datum.series == "daily_dau",
        alt.value([]),
        alt.value([4, 4]),
    )
    _opacity = alt.condition(
        alt.datum.series == "daily_dau",
        alt.value(0.4),
        alt.value(1.0),
    )

    _hover = alt.selection_point(
        fields=["submission_date"],
        nearest=True,
        on="pointerover",
        empty=False,
    )

    _base = alt.Chart(_long).encode(
        x=alt.X("submission_date:T", title="Date"),
        color=alt.Color(
            "series:N",
            sort=_series_order,
            legend=alt.Legend(title="Series"),
        ),
    )

    _lines = _base.mark_line().encode(
        y=alt.Y("count:Q", title="DAU", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        strokeDash=_stroke_dash,
        opacity=_opacity,
    )

    _points = _base.mark_point(size=60, filled=True).encode(
        y=alt.Y("count:Q"),
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[
            alt.Tooltip("submission_date:T", title="Date", format="%b %d, %Y"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("count:Q", title="Value", format=",.0f"),
        ],
    ).add_params(_hover)

    _rule = (
        alt.Chart(_long)
        .mark_rule(color="gray", strokeWidth=1)
        .encode(x="submission_date:T")
        .transform_filter(_hover)
    )

    _chart = (
        (_lines + _rule + _points)
        .properties(
            title="Mobile DAU — Daily, 28-day MA, Forecast",
            width="container",
            height=400,
        )
    )

    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    WITH daily AS (
        SELECT
            aua.submission_date AS submission_date,
            COALESCE(SUM(aua.dau), 0) AS dau_all_countries,
            COALESCE(
                SUM(
                    IF(
                        countries.name = 'Iran, Islamic Republic of'
                        OR locale LIKE 'fa%',
                        0,
                        aua.dau
                    )
                ),
                0
            ) AS dau_all_countries_ex_iran
        FROM `moz-fx-data-shared-prod.telemetry.active_users_aggregates` AS aua
        LEFT JOIN `mozdata.static.country_codes_v1` AS countries
            ON aua.country = countries.code
        WHERE
            aua.app_name IN ('Fenix')
            AND aua.submission_date >= (
                DATE_ADD(
                    DATE_TRUNC(CURRENT_DATE('UTC'), WEEK(MONDAY)),
                    INTERVAL -115 WEEK
                )
            )
        GROUP BY 1
    ),

    ma AS (
        SELECT
            submission_date,
            dau_all_countries,
            dau_all_countries_ex_iran,
            AVG(dau_all_countries) OVER (
                ORDER BY submission_date
                ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
            ) AS dau_28d_avg_all_countries,
            AVG(dau_all_countries_ex_iran) OVER (
                ORDER BY submission_date
                ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
            ) AS dau_28d_avg_all_countries_ex_iran
        FROM daily
    ),

    yoy AS (
        SELECT
            cur.submission_date,
            cur.dau_all_countries,
            cur.dau_all_countries_ex_iran,
            cur.dau_28d_avg_all_countries,
            cur.dau_28d_avg_all_countries_ex_iran,
            prev.dau_28d_avg_all_countries
                AS dau_28d_avg_all_countries_prev_year,
            prev.dau_28d_avg_all_countries_ex_iran
                AS dau_28d_avg_all_countries_ex_iran_prev_year
        FROM ma AS cur
        LEFT JOIN ma AS prev
            ON prev.submission_date = DATE_SUB(cur.submission_date, INTERVAL 1 YEAR)
    )

    SELECT
        submission_date,
        dau_all_countries,
        dau_all_countries_ex_iran,
        dau_28d_avg_all_countries,
        dau_28d_avg_all_countries_ex_iran,
        dau_28d_avg_all_countries_prev_year,
        dau_28d_avg_all_countries_ex_iran_prev_year,
        SAFE_DIVIDE(
            dau_28d_avg_all_countries - dau_28d_avg_all_countries_prev_year,
            dau_28d_avg_all_countries_prev_year
        ) * 100 AS dau_28d_avg_all_countries_yoy_growth,
        SAFE_DIVIDE(
            dau_28d_avg_all_countries_ex_iran
            - dau_28d_avg_all_countries_ex_iran_prev_year,
            dau_28d_avg_all_countries_ex_iran_prev_year
        ) * 100 AS dau_28d_avg_all_countries_ex_iran_yoy_growth
    FROM yoy
    WHERE submission_date >= DATE '2025-01-01'
    ORDER BY submission_date DESC
    LIMIT 5000
    """

    _df = client.query(_query).to_dataframe()
    _df["submission_date"] = _df["submission_date"].astype(str)

    _long = _df.melt(
        id_vars="submission_date",
        value_vars=["dau_28d_avg_all_countries", "dau_28d_avg_all_countries_ex_iran"],
        var_name="series",
        value_name="count",
    ).replace({
        "dau_28d_avg_all_countries": "DAU Global (28d MA)",
        "dau_28d_avg_all_countries_ex_iran": "DAU Global ex-Iran (28d MA)",
    })

    _hover = alt.selection_point(
        fields=["submission_date"],
        nearest=True,
        on="pointerover",
        empty=False,
    )

    _base = alt.Chart(_long).encode(
        x=alt.X("submission_date:T", title="Date"),
        color=alt.Color("series:N", legend=alt.Legend(title=None, orient="bottom")),
    )
    _lines = _base.mark_line().encode(
        y=alt.Y("count:Q", title="DAU", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(size=60, filled=True).encode(
        y=alt.Y("count:Q"),
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[
            alt.Tooltip("submission_date:T", title="Date", format="%b %d, %Y"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("count:Q", title="DAU", format=",.0f"),
        ],
    ).add_params(_hover)
    _rule = alt.Chart(_long).mark_rule(color="gray", strokeWidth=1).encode(
        x="submission_date:T"
    ).transform_filter(_hover)

    mo.ui.altair_chart(
        (_lines + _rule + _points).properties(
            title="Fenix DAU Global vs ex-Iran (28d MA)",
            width="container",
            height=400,
        )
    )
    return


@app.cell
def _(alt, client, mo):
    _query = """
    WITH daily AS (
        SELECT
            aua.submission_date AS submission_date,
            COALESCE(SUM(aua.dau), 0) AS dau_all_countries,
            COALESCE(SUM(IF(countries.name = 'Iran, Islamic Republic of' OR locale LIKE 'fa%', 0, aua.dau)), 0) AS dau_all_countries_ex_iran
        FROM `moz-fx-data-shared-prod.telemetry.active_users_aggregates` AS aua
        LEFT JOIN `mozdata.static.country_codes_v1` AS countries ON aua.country = countries.code
        WHERE aua.app_name IN ('Fenix') AND aua.submission_date >= DATE_ADD(DATE_TRUNC(CURRENT_DATE('UTC'), WEEK(MONDAY)), INTERVAL -115 WEEK)
        GROUP BY 1
    ),
    ma AS (
        SELECT submission_date,
            AVG(dau_all_countries) OVER (ORDER BY submission_date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS dau_28d_avg_all_countries,
            AVG(dau_all_countries_ex_iran) OVER (ORDER BY submission_date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS dau_28d_avg_all_countries_ex_iran
        FROM daily
    ),
    yoy AS (
        SELECT cur.submission_date,
            SAFE_DIVIDE(cur.dau_28d_avg_all_countries - prev.dau_28d_avg_all_countries, prev.dau_28d_avg_all_countries) * 100 AS yoy_global,
            SAFE_DIVIDE(cur.dau_28d_avg_all_countries_ex_iran - prev.dau_28d_avg_all_countries_ex_iran, prev.dau_28d_avg_all_countries_ex_iran) * 100 AS yoy_ex_iran
        FROM ma AS cur LEFT JOIN ma AS prev ON prev.submission_date = DATE_SUB(cur.submission_date, INTERVAL 1 YEAR)
    )
    SELECT submission_date, yoy_global, yoy_ex_iran
    FROM yoy WHERE submission_date >= DATE '2025-01-01' ORDER BY submission_date
    """
    _df = client.query(_query).to_dataframe()
    _df["submission_date"] = _df["submission_date"].astype(str)
    _long = _df.melt(
        id_vars="submission_date",
        value_vars=["yoy_global", "yoy_ex_iran"],
        var_name="series",
        value_name="pct",
    ).replace({"yoy_global": "Global YoY %", "yoy_ex_iran": "ex-Iran YoY %"})
    _hover = alt.selection_point(fields=["submission_date"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_long).encode(
        x=alt.X("submission_date:T", title="Date"),
        color=alt.Color("series:N", legend=alt.Legend(title=None, orient="bottom")),
    )
    _lines = _base.mark_line().encode(
        y=alt.Y("pct:Q", title="YoY %", axis=alt.Axis(format=".1f"), scale=alt.Scale(zero=False)),
    )
    _zero = alt.Chart({"values": [{}]}).mark_rule(color="gray", strokeDash=[2, 2], strokeWidth=1).encode(y=alt.datum(0))
    _points = _base.mark_point(size=60, filled=True).encode(
        y=alt.Y("pct:Q"),
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[
            alt.Tooltip("submission_date:T", title="Date", format="%b %d, %Y"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("pct:Q", title="YoY %", format=".2f"),
        ],
    ).add_params(_hover)
    _rule = alt.Chart(_long).mark_rule(color="gray", strokeWidth=1).encode(x="submission_date:T").transform_filter(_hover)
    mo.ui.altair_chart(
        (_lines + _zero + _rule + _points).properties(
            title="Fenix DAU YoY % (Global vs ex-Iran)",
            width="container",
            height=300,
        )
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## Section 2: Fact — Mobile New Profiles Are Down vs 2025
    Both iOS and Android new profiles are trending below 2025 levels. The issue is concentrated in organic acquisition — paid is flat.
    """)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    WITH daily AS (
        SELECT
            first_seen_date,
            SUM(new_profiles) AS new_profiles
        FROM `moz-fx-data-shared-prod.telemetry.mobile_new_profiles`
        WHERE
            first_seen_date >= '2024-12-25'
            AND is_mobile
        GROUP BY 1
    ),
    with_ma AS (
        SELECT
            first_seen_date,
            new_profiles,
            AVG(new_profiles) OVER (
                ORDER BY first_seen_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS np_7d_ma
        FROM daily
    )
    SELECT
        DATE(2026, EXTRACT(MONTH FROM first_seen_date), EXTRACT(DAY FROM first_seen_date)) AS aligned_date,
        MAX(IF(EXTRACT(YEAR FROM first_seen_date) = 2025, np_7d_ma, NULL)) AS np_7dma_2025,
        MAX(IF(EXTRACT(YEAR FROM first_seen_date) = 2026, np_7d_ma, NULL)) AS np_7dma_2026,
        SAFE_DIVIDE(
            MAX(IF(EXTRACT(YEAR FROM first_seen_date) = 2026, np_7d_ma, NULL))
            - MAX(IF(EXTRACT(YEAR FROM first_seen_date) = 2025, np_7d_ma, NULL)),
            MAX(IF(EXTRACT(YEAR FROM first_seen_date) = 2025, np_7d_ma, NULL))
        ) * 100 AS yoy_pct
    FROM with_ma
    WHERE first_seen_date >= '2025-01-01'
    GROUP BY 1
    ORDER BY 1
    """
    _df = client.query(_query).to_dataframe()
    _df["aligned_date"] = _df["aligned_date"].astype(str)

    _long_np = _df.melt(
        id_vars="aligned_date",
        value_vars=["np_7dma_2025", "np_7dma_2026"],
        var_name="series",
        value_name="value",
    ).replace({"np_7dma_2025": "2025 (7d MA)", "np_7dma_2026": "2026 (7d MA)"})

    _hover = alt.selection_point(fields=["aligned_date"], nearest=True, on="pointerover", empty=False)

    _base_np = alt.Chart(_long_np).encode(
        x=alt.X("aligned_date:T", title=None, axis=alt.Axis(labels=False)),
        color=alt.Color("series:N", legend=alt.Legend(title=None, orient="bottom")),
    )
    _lines_np = _base_np.mark_line().encode(
        y=alt.Y("value:Q", title="New Profiles (7d MA)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points_np = _base_np.mark_point(size=60, filled=True).encode(
        y=alt.Y("value:Q"),
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[
            alt.Tooltip("aligned_date:T", title="Date", format="%b %d"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("value:Q", title="NP (7d MA)", format=",.0f"),
        ],
    ).add_params(_hover)
    _rule_np = alt.Chart(_long_np).mark_rule(color="gray", strokeWidth=1).encode(x="aligned_date:T").transform_filter(_hover)
    _panel_np = (_lines_np + _rule_np + _points_np).properties(
        title="Mobile New Profiles (7d MA) — 2025 vs 2026", height=300
    )

    _base_yoy = alt.Chart(_df).encode(x=alt.X("aligned_date:T", title="Date"))
    _line_yoy = _base_yoy.mark_line(color="steelblue").encode(
        y=alt.Y("yoy_pct:Q", title="YoY %", axis=alt.Axis(format=".1f"), scale=alt.Scale(zero=False)),
    )
    _zero = alt.Chart({"values": [{}]}).mark_rule(color="gray", strokeDash=[2, 2], strokeWidth=1).encode(y=alt.datum(0))
    _panel_yoy = (_line_yoy + _zero).properties(height=150)

    mo.ui.altair_chart(
        alt.vconcat(_panel_np, _panel_yoy).resolve_scale(x="shared").properties(width="container")
    )
    return


@app.cell
def _(alt, client, mo):
    _query_android = """
    WITH daily AS (
        SELECT
            first_seen_date,
            SUM(new_profiles) AS new_profiles
        FROM `moz-fx-data-shared-prod.telemetry.mobile_new_profiles`
        WHERE first_seen_date >= '2024-12-25' AND app_name = 'Fenix'
        GROUP BY 1
    ),
    with_ma AS (
        SELECT first_seen_date, AVG(new_profiles) OVER (
            ORDER BY first_seen_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS np_7d_ma FROM daily
    )
    SELECT
        DATE(2026, EXTRACT(MONTH FROM first_seen_date), EXTRACT(DAY FROM first_seen_date)) AS aligned_date,
        MAX(IF(EXTRACT(YEAR FROM first_seen_date) = 2025, np_7d_ma, NULL)) AS np_7dma_2025,
        MAX(IF(EXTRACT(YEAR FROM first_seen_date) = 2026, np_7d_ma, NULL)) AS np_7dma_2026
    FROM with_ma WHERE first_seen_date >= '2025-01-01' GROUP BY 1 ORDER BY 1
    """
    _query_ios = """
    WITH daily AS (
        SELECT
            first_seen_date,
            SUM(new_profiles) AS new_profiles
        FROM `moz-fx-data-shared-prod.telemetry.mobile_new_profiles`
        WHERE first_seen_date >= '2024-12-25' AND app_name = 'Firefox iOS'
        GROUP BY 1
    ),
    with_ma AS (
        SELECT first_seen_date, AVG(new_profiles) OVER (
            ORDER BY first_seen_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS np_7d_ma FROM daily
    )
    SELECT
        DATE(2026, EXTRACT(MONTH FROM first_seen_date), EXTRACT(DAY FROM first_seen_date)) AS aligned_date,
        MAX(IF(EXTRACT(YEAR FROM first_seen_date) = 2025, np_7d_ma, NULL)) AS np_7dma_2025,
        MAX(IF(EXTRACT(YEAR FROM first_seen_date) = 2026, np_7d_ma, NULL)) AS np_7dma_2026
    FROM with_ma WHERE first_seen_date >= '2025-01-01' GROUP BY 1 ORDER BY 1
    """
    _df_a = client.query(_query_android).to_dataframe()
    _df_a["aligned_date"] = _df_a["aligned_date"].astype(str)
    _df_i = client.query(_query_ios).to_dataframe()
    _df_i["aligned_date"] = _df_i["aligned_date"].astype(str)

    def _np_yoy_chart(df, title):
        _long = df.melt(
            id_vars="aligned_date",
            value_vars=["np_7dma_2025", "np_7dma_2026"],
            var_name="series",
            value_name="value",
        ).replace({"np_7dma_2025": "2025", "np_7dma_2026": "2026"})
        return alt.Chart(_long).mark_line().encode(
            x=alt.X("aligned_date:T", title="Date"),
            y=alt.Y("value:Q", title="NP (7d MA)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
            color=alt.Color("series:N", legend=alt.Legend(title=None)),
            tooltip=[alt.Tooltip("aligned_date:T", format="%b %d"), "series:N", alt.Tooltip("value:Q", format=",.0f")],
        ).properties(title=title, width=400, height=250)

    mo.ui.altair_chart(
        alt.hconcat(
            _np_yoy_chart(_df_a, "Android (Fenix) New Profiles (7d MA)"),
            _np_yoy_chart(_df_i, "iOS New Profiles (7d MA)"),
        )
    )
    return


@app.cell
def _(alt, client, mo):
    _query = """
    SELECT
        first_seen_date,
        SUM(IF(paid_vs_organic_gclid = 'Organic', new_profiles, 0)) AS np_organic,
        SUM(IF(paid_vs_organic_gclid = 'Paid', new_profiles, 0)) AS np_paid
    FROM `moz-fx-data-shared-prod.fenix.new_profiles` AS b
    WHERE
        first_seen_date >= DATE(2025, 11, 1)
        AND normalized_channel = 'release'
        AND country != 'IR'
        AND COALESCE(locale, '') NOT LIKE 'fa%'
        AND mozfun.norm.browser_version_info(app_version).major_version >= 140
    GROUP BY 1
    ORDER BY 1
    """
    _df = client.query(_query).to_dataframe()
    _df["first_seen_date"] = _df["first_seen_date"].astype(str)
    _long = _df.melt(
        id_vars="first_seen_date",
        value_vars=["np_organic", "np_paid"],
        var_name="series",
        value_name="value",
    ).replace({"np_organic": "Organic", "np_paid": "Paid"})
    _hover = alt.selection_point(fields=["first_seen_date"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_long).encode(
        x=alt.X("first_seen_date:T", title="Date"),
        color=alt.Color("series:N", legend=alt.Legend(title=None)),
    )
    _lines = _base.mark_line().encode(
        y=alt.Y("value:Q", title="New Profiles", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(size=60, filled=True).encode(
        y="value:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("first_seen_date:T", title="Date", format="%b %d, %Y"), "series:N", alt.Tooltip("value:Q", format=",.0f")],
    ).add_params(_hover)
    _rule = alt.Chart(_long).mark_rule(color="gray", strokeWidth=1).encode(x="first_seen_date:T").transform_filter(_hover)
    mo.ui.altair_chart((_lines + _rule + _points).properties(title="Fenix New Profiles — Organic vs Paid", width="container", height=300))
    return


@app.cell
def _(mo):
    mo.md("""
    ## Section 3: Fact — Android "Other Playstore" Installs Are Trending Down
    Android is falling in the organic "Other Playstore" category. Breaking out install source further, the drop is on Play Store installs and also on Null (sideload).
    """)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    SELECT
        first_seen_date AS dt,
        CASE
            WHEN ((paid_vs_organic_gclid = 'Paid') AND (install_source = 'com.android.vending')) THEN 'Paid Playstore'
            WHEN ((paid_vs_organic_gclid != 'Paid') AND (install_source = 'com.android.vending')) THEN 'Organic Playstore'
            WHEN ((paid_vs_organic_gclid = 'Paid') AND (install_source IS NULL)) THEN 'Paid NULL'
            WHEN ((paid_vs_organic_gclid = 'Paid') AND (install_source != 'com.android.vending')) THEN 'Paid Non-Playstore'
            WHEN ((paid_vs_organic_gclid != 'Paid') AND (install_source IS NULL)) THEN 'Organic Null'
            WHEN ((paid_vs_organic_gclid != 'Paid') AND (install_source != 'com.android.vending')) THEN 'Organic Non-Playstore'
            ELSE 'Other'
        END AS acquisition,
        SUM(new_profiles) AS nps
    FROM `moz-fx-data-shared-prod.fenix_derived.new_profiles_v1` AS np
    WHERE
        np.first_seen_date >= '2025-01-01'
        AND np.country != 'IR'
        AND np.locale NOT LIKE '%fa%'
        AND normalized_channel = 'release'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    _df = client.query(_query).to_dataframe()
    _df["dt"] = _df["dt"].astype(str)
    _hover = alt.selection_point(fields=["dt"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_df).encode(
        x=alt.X("dt:T", title="Date"),
        color=alt.Color("acquisition:N", legend=alt.Legend(title="Acquisition")),
    )
    _lines = _base.mark_line().encode(
        y=alt.Y("nps:Q", title="New Profiles", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(size=60, filled=True).encode(
        y="nps:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("dt:T", title="Date", format="%b %d, %Y"), "acquisition:N", alt.Tooltip("nps:Q", title="NP", format=",.0f")],
    ).add_params(_hover)
    _rule = alt.Chart(_df).mark_rule(color="gray", strokeWidth=1).encode(x="dt:T").transform_filter(_hover)
    mo.ui.altair_chart((_lines + _rule + _points).properties(title="NP by Acquisition Source", width="container", height=350))
    return


@app.cell
def _(alt, client, mo):
    _query = """
    WITH anchor AS (
        SELECT
            first_seen_date,
            CASE
                WHEN c.install_source = 'com.android.vending' THEN 'Play Store'
                WHEN c.install_source = 'com.huawei.appmarket' THEN 'Huawei AppGallery'
                WHEN c.install_source = 'com.farsitel.bazaar' THEN 'Cafe Bazaar Iran'
                WHEN c.install_source = 'ir.mservices.market' THEN 'Myket Iran'
                WHEN c.install_source = 'com.heytap.market' THEN 'OPPO App Market'
                WHEN c.install_source = 'com.vivo.appstore' THEN 'Vivo App Store'
                WHEN c.install_source IN ('com.samsung.applestore', 'com.amazon.venezia') THEN 'Other Official Stores'
                WHEN c.install_source IS NULL OR c.install_source = '' THEN 'Null'
                WHEN c.install_source LIKE '%packageinstaller%' THEN 'Sideload'
                ELSE 'Other'
            END AS install_source_bucket,
            SUM(new_profiles) AS new_profiles
        FROM `moz-fx-data-shared-prod.fenix_derived.new_profiles_v1` AS c
        WHERE
            first_seen_date >= '2025-09-01'
            AND normalized_channel = 'release'
        GROUP BY 1, 2
    )
    SELECT * FROM anchor ORDER BY 1, 2
    """
    _df = client.query(_query).to_dataframe()
    _df["first_seen_date"] = _df["first_seen_date"].astype(str)
    _hover = alt.selection_point(fields=["first_seen_date"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_df).encode(
        x=alt.X("first_seen_date:T", title="Date"),
        color=alt.Color("install_source_bucket:N", legend=alt.Legend(title="Install Source")),
    )
    _lines = _base.mark_line().encode(
        y=alt.Y("new_profiles:Q", title="New Profiles", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(size=60, filled=True).encode(
        y="new_profiles:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("first_seen_date:T", title="Date", format="%b %d, %Y"), "install_source_bucket:N", alt.Tooltip("new_profiles:Q", format=",.0f")],
    ).add_params(_hover)
    _rule = alt.Chart(_df).mark_rule(color="gray", strokeWidth=1).encode(x="first_seen_date:T").transform_filter(_hover)
    mo.ui.altair_chart((_lines + _rule + _points).properties(title="NP by Install Source", width="container", height=350))
    return


@app.cell
def _(alt, client, mo):
    _query = """
    WITH base AS (
        SELECT
            date,
            SUM(install_count) AS install_count,
            SUM(uninstall_count) AS uninstall_count,
            SUM(update_count) AS update_count
        FROM `mozdata.fenix.gplay_installs_by_country`
        WHERE date >= DATE(2025, 1, 1)
        GROUP BY 1
    )
    SELECT
        *,
        install_count - uninstall_count AS net_delta,
        AVG(install_count) OVER (ORDER BY date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS install_count_28ma,
        AVG(uninstall_count) OVER (ORDER BY date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS uninstall_count_28ma,
        AVG(install_count - uninstall_count) OVER (ORDER BY date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS net_delta_28ma
    FROM base
    ORDER BY 1
    """
    _df = client.query(_query).to_dataframe()
    _df["date"] = _df["date"].astype(str)
    _long = _df.melt(
        id_vars="date",
        value_vars=["install_count_28ma", "uninstall_count_28ma", "net_delta_28ma"],
        var_name="series",
        value_name="value",
    ).replace({
        "install_count_28ma": "Installs (28MA)",
        "uninstall_count_28ma": "Uninstalls (28MA)",
        "net_delta_28ma": "Net Delta (28MA)",
    })
    _hover = alt.selection_point(fields=["date"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_long).encode(
        x=alt.X("date:T", title="Date"),
        color=alt.Color("series:N", legend=alt.Legend(title=None)),
    )
    _lines = _base.mark_line().encode(
        y=alt.Y("value:Q", title="Count", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(size=60, filled=True).encode(
        y="value:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("date:T", title="Date", format="%b %d, %Y"), "series:N", alt.Tooltip("value:Q", format=",.0f")],
    ).add_params(_hover)
    _rule = alt.Chart(_long).mark_rule(color="gray", strokeWidth=1).encode(x="date:T").transform_filter(_hover)
    mo.ui.altair_chart((_lines + _rule + _points).properties(title="GPlay Installs — 28-Day Moving Averages", width="container", height=300))
    return


@app.cell
def _(mo):
    mo.md("""
    ## Section 4: Theory — Step Change at Version 147.0.4
    There is a step change in Fenix new profiles starting with 147.0.4 (a chemspill release). Chart filtered to US/FR/DE to avoid holiday noise. New profiles are recovering on OS 16 with app version 148, but older OS versions continue to decline.
    """)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    SELECT
        first_seen_date AS ds,
        app_version AS version,
        SUM(new_profiles) AS new_profiles
    FROM `moz-fx-data-shared-prod.telemetry.mobile_new_profiles`
    WHERE
        first_seen_date > DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
        AND app_name = 'Fenix'
        AND country IN ('US', 'DE', 'FR')
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    _df = client.query(_query).to_dataframe()
    _df["ds"] = _df["ds"].astype(str)
    _hover = alt.selection_point(fields=["ds"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_df).encode(
        x=alt.X("ds:T", title="Date"),
        color=alt.Color("version:N", legend=alt.Legend(title="App Version")),
    )
    _lines = _base.mark_line(strokeWidth=1.5).encode(
        y=alt.Y("new_profiles:Q", title="New Profiles", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(size=40, filled=True).encode(
        y="new_profiles:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("ds:T", title="Date", format="%b %d, %Y"), "version:N", alt.Tooltip("new_profiles:Q", format=",.0f")],
    ).add_params(_hover)
    _rule = alt.Chart(_df).mark_rule(color="gray", strokeWidth=1).encode(x="ds:T").transform_filter(_hover)
    mo.ui.altair_chart((_lines + _rule + _points).properties(title="NP by App Version (US/DE/FR)", width="container", height=350))
    return


@app.cell
def _(alt, client, mo):
    _query = """
    SELECT
        first_seen_date,
        app_version,
        SUM(new_profiles) AS np
    FROM `moz-fx-data-shared-prod.fenix.new_profiles` AS b
    WHERE
        first_seen_date >= DATE(2026, 1, 1)
        AND normalized_channel = 'release'
        AND country != 'IR'
        AND COALESCE(locale, '') NOT LIKE 'fa%'
        AND mozfun.norm.browser_version_info(app_version).major_version >= 140
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    _df = client.query(_query).to_dataframe()
    _df["first_seen_date"] = _df["first_seen_date"].astype(str)
    _hover = alt.selection_point(fields=["first_seen_date"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_df).encode(
        x=alt.X("first_seen_date:T", title="Date"),
        color=alt.Color("app_version:N", legend=alt.Legend(title="App Version")),
    )
    _lines = _base.mark_line(strokeWidth=1.5).encode(
        y=alt.Y("np:Q", title="New Profiles", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(size=40, filled=True).encode(
        y="np:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("first_seen_date:T", title="Date", format="%b %d, %Y"), "app_version:N", alt.Tooltip("np:Q", format=",.0f")],
    ).add_params(_hover)
    _rule = alt.Chart(_df).mark_rule(color="gray", strokeWidth=1).encode(x="first_seen_date:T").transform_filter(_hover)
    mo.ui.altair_chart((_lines + _rule + _points).properties(title="NP by Version (140+, ex-Iran)", width="container", height=350))
    return


@app.cell
def _(alt, client, mo, os_version):
    _os_val = int(os_version.value)
    _os_filter = (
        "TRUE"
        if _os_val == -1
        else f"SAFE_CAST(REGEXP_EXTRACT(os_version, r'^(\\\\d+)') AS INT64) = {_os_val}"
    )
    _query = f"""
    SELECT
        first_seen_date AS ds,
        app_version AS version,
        SUM(new_profiles) AS new_profiles
    FROM `moz-fx-data-shared-prod.telemetry.mobile_new_profiles`
    WHERE
        first_seen_date > DATE_SUB(CURRENT_DATE(), INTERVAL 4 MONTH)
        AND app_name = 'Fenix'
        AND {_os_filter}
    GROUP BY 1, 2
    ORDER BY 1 DESC
    """
    _df = client.query(_query).to_dataframe()
    _df["ds"] = _df["ds"].astype(str)
    _hover = alt.selection_point(fields=["ds"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_df).encode(
        x=alt.X("ds:T", title="Date"),
        color=alt.Color("version:N", legend=alt.Legend(title="App Version")),
    )
    _lines = _base.mark_line(strokeWidth=1.5).encode(
        y=alt.Y("new_profiles:Q", title="New Profiles", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(size=40, filled=True).encode(
        y="new_profiles:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("ds:T", title="Date", format="%b %d, %Y"), "version:N", alt.Tooltip("new_profiles:Q", format=",.0f")],
    ).add_params(_hover)
    _rule = alt.Chart(_df).mark_rule(color="gray", strokeWidth=1).encode(x="ds:T").transform_filter(_hover)
    _title = f"NP by App Version — OS {'all' if _os_val == -1 else _os_val}"
    mo.ui.altair_chart((_lines + _rule + _points).properties(title=_title, width="container", height=350))
    return


@app.cell
def _(mo):
    mo.md("""
    ## Section 5: Fact — Mobile Day-3 Retention Is Down vs 2025
    iOS day-3 retention is stable post DMA Wave 2. Android is falling, driven by day-1 dropoff. Existing user 1-week retention also showed a dip around late Feb (largest in DE), possibly related to 148.0.1.
    """)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    WITH daily_retention AS (
        SELECT
            cohort_date,
            SAFE_DIVIDE(SUM(num_clients_active_on_day), SUM(num_clients_in_cohort)) AS retention_day3
        FROM `moz-fx-data-shared-prod.telemetry.cohort_daily_statistics`
        WHERE
            normalized_app_name IN ('Fenix', 'Firefox iOS', 'Focus Android', 'Focus iOS')
            AND activity_date >= '2024-12-25'
            AND DATE_DIFF(activity_date, cohort_date, DAY) = 3
            AND cohort_date >= '2024-12-25'
        GROUP BY 1
    ),
    smoothed AS (
        SELECT
            cohort_date,
            AVG(retention_day3) OVER (ORDER BY cohort_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS retention_7d_avg
        FROM daily_retention
    )
    SELECT
        DATE(2026, EXTRACT(MONTH FROM cohort_date), EXTRACT(DAY FROM cohort_date)) AS aligned_date,
        MAX(IF(EXTRACT(YEAR FROM cohort_date) = 2025, ROUND(retention_7d_avg * 100, 2), NULL)) AS retention_2025,
        MAX(IF(EXTRACT(YEAR FROM cohort_date) = 2026, ROUND(retention_7d_avg * 100, 2), NULL)) AS retention_2026,
        SAFE_DIVIDE(
            MAX(IF(EXTRACT(YEAR FROM cohort_date) = 2026, retention_7d_avg, NULL))
            - MAX(IF(EXTRACT(YEAR FROM cohort_date) = 2025, retention_7d_avg, NULL)),
            MAX(IF(EXTRACT(YEAR FROM cohort_date) = 2025, retention_7d_avg, NULL))
        ) * 100 AS yoy_pct
    FROM smoothed
    WHERE cohort_date >= '2025-01-01'
    GROUP BY 1
    ORDER BY 1
    """
    _df = client.query(_query).to_dataframe()
    _df["aligned_date"] = _df["aligned_date"].astype(str)

    _long_ret = _df.melt(
        id_vars="aligned_date",
        value_vars=["retention_2025", "retention_2026"],
        var_name="series",
        value_name="pct",
    ).replace({"retention_2025": "2025 (7d avg)", "retention_2026": "2026 (7d avg)"})

    _hover = alt.selection_point(fields=["aligned_date"], nearest=True, on="pointerover", empty=False)
    _base_r = alt.Chart(_long_ret).encode(
        x=alt.X("aligned_date:T", title=None, axis=alt.Axis(labels=False)),
        color=alt.Color("series:N", legend=alt.Legend(title=None, orient="bottom")),
    )
    _lines_r = _base_r.mark_line().encode(
        y=alt.Y("pct:Q", title="Day-3 Retention %", axis=alt.Axis(format=".1f"), scale=alt.Scale(zero=False)),
    )
    _points_r = _base_r.mark_point(size=60, filled=True).encode(
        y="pct:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("aligned_date:T", title="Date", format="%b %d"), "series:N", alt.Tooltip("pct:Q", title="Retention %", format=".2f")],
    ).add_params(_hover)
    _rule_r = alt.Chart(_long_ret).mark_rule(color="gray", strokeWidth=1).encode(x="aligned_date:T").transform_filter(_hover)
    _panel_r = (_lines_r + _rule_r + _points_r).properties(title="Mobile Day-3 Retention (7d avg) — 2025 vs 2026", height=300)

    _base_yoy = alt.Chart(_df).encode(x=alt.X("aligned_date:T", title="Date"))
    _line_yoy = _base_yoy.mark_line(color="steelblue").encode(
        y=alt.Y("yoy_pct:Q", title="YoY %", axis=alt.Axis(format=".1f"), scale=alt.Scale(zero=False)),
    )
    _zero = alt.Chart({"values": [{}]}).mark_rule(color="gray", strokeDash=[2, 2], strokeWidth=1).encode(y=alt.datum(0))
    _panel_yoy = (_line_yoy + _zero).properties(height=150)

    mo.ui.altair_chart(
        alt.vconcat(_panel_r, _panel_yoy).resolve_scale(x="shared").properties(width="container")
    )
    return


@app.cell
def _(alt, client, mo):
    _query_fenix_ios = """
    WITH daily_retention AS (
        SELECT
            cohort_date,
            normalized_app_name AS product,
            SAFE_DIVIDE(SUM(num_clients_active_on_day), SUM(num_clients_in_cohort)) AS retention_day3
        FROM `moz-fx-data-shared-prod.telemetry.cohort_daily_statistics`
        WHERE
            normalized_app_name IN ('Fenix', 'Firefox iOS')
            AND activity_date >= '2024-12-25'
            AND DATE_DIFF(activity_date, cohort_date, DAY) = 3
            AND cohort_date >= '2024-12-25'
        GROUP BY 1, 2
    ),
    smoothed AS (
        SELECT
            cohort_date, product,
            AVG(retention_day3) OVER (PARTITION BY product ORDER BY cohort_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS retention_7d_avg
        FROM daily_retention
    )
    SELECT cohort_date AS ds, product, ROUND(retention_7d_avg * 100, 2) AS retention_pct
    FROM smoothed WHERE cohort_date >= '2025-01-01' ORDER BY ds, product
    """
    _query_d1d7 = """
    WITH daily_retention AS (
        SELECT
            cohort_date,
            DATE_DIFF(activity_date, cohort_date, DAY) AS days_retained,
            SAFE_DIVIDE(SUM(num_clients_active_on_day), SUM(num_clients_in_cohort)) AS retention
        FROM `moz-fx-data-shared-prod.telemetry.cohort_daily_statistics`
        WHERE
            normalized_app_name = 'Fenix'
            AND activity_date >= '2023-12-25'
            AND DATE_DIFF(activity_date, cohort_date, DAY) BETWEEN 1 AND 7
            AND cohort_date >= '2023-12-25'
        GROUP BY 1, 2
    ),
    smoothed AS (
        SELECT
            cohort_date, days_retained,
            AVG(retention) OVER (PARTITION BY days_retained ORDER BY cohort_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS retention_7d_avg
        FROM daily_retention
    )
    SELECT cohort_date AS ds, CONCAT('Day ', CAST(days_retained AS STRING)) AS series, ROUND(retention_7d_avg * 100, 2) AS retention_pct
    FROM smoothed WHERE cohort_date >= '2024-01-01' ORDER BY ds, days_retained
    """
    _df_fi = client.query(_query_fenix_ios).to_dataframe()
    _df_fi["ds"] = _df_fi["ds"].astype(str)
    _df_d7 = client.query(_query_d1d7).to_dataframe()
    _df_d7["ds"] = _df_d7["ds"].astype(str)

    _chart_fi = alt.Chart(_df_fi).mark_line().encode(
        x=alt.X("ds:T", title="Date"),
        y=alt.Y("retention_pct:Q", title="Day-3 Retention %", axis=alt.Axis(format=".1f"), scale=alt.Scale(zero=False)),
        color=alt.Color("product:N", legend=alt.Legend(title=None)),
        tooltip=[alt.Tooltip("ds:T", format="%b %d, %Y"), "product:N", alt.Tooltip("retention_pct:Q", format=".2f")],
    ).properties(title="Fenix vs iOS Day-3 Retention (7d avg)", width=430, height=280)

    _chart_d7 = alt.Chart(_df_d7).mark_line().encode(
        x=alt.X("ds:T", title="Date"),
        y=alt.Y("retention_pct:Q", title="Retention %", axis=alt.Axis(format=".1f"), scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", legend=alt.Legend(title=None)),
        tooltip=[alt.Tooltip("ds:T", format="%b %d, %Y"), "series:N", alt.Tooltip("retention_pct:Q", format=".2f")],
    ).properties(title="Fenix D1–D7 Retention (7d avg)", width=430, height=280)

    mo.ui.altair_chart(alt.hconcat(_chart_fi, _chart_d7).resolve_scale(color="independent"))
    return


@app.cell
def _(alt, client, mo):
    _query = """
    WITH base AS (
        SELECT
            client_id,
            mozfun.bits28.retention(days_seen_bits, submission_date) AS retention
        FROM `mozdata.fenix.active_users`
        WHERE
            submission_date BETWEEN DATE(2026, 1, 1) AND CURRENT_DATE()
            AND mozfun.bits28.active_in_range(days_seen_bits, -13, 1) = TRUE
            AND normalized_channel = 'release'
            AND country != 'IR'
            AND COALESCE(locale, '') NOT LIKE 'fa%'
            AND DATE_DIFF(submission_date, first_seen_date, DAY) > 27
            AND country IN ('US', 'DE', 'FR')
    )
    SELECT
        retention.day_13.metric_date,
        SAFE_DIVIDE(COUNTIF(retention.day_13.active_in_week_1), COUNTIF(retention.day_13.active_in_week_0)) AS retention_1_week,
        SAFE_DIVIDE(COUNTIF(retention.day_13.active_in_week_0_after_metric_date), COUNTIF(retention.day_13.active_in_week_0)) AS retention_0_week
    FROM base
    GROUP BY metric_date
    ORDER BY metric_date
    """
    _df = client.query(_query).to_dataframe()
    _df["metric_date"] = _df["metric_date"].astype(str)
    _long = _df.melt(id_vars="metric_date", value_vars=["retention_1_week", "retention_0_week"], var_name="series", value_name="rate")
    _long["rate"] = _long["rate"] * 100
    _long["series"] = _long["series"].replace({"retention_1_week": "1-Week Retention", "retention_0_week": "0-Week Retention"})
    _chart = alt.Chart(_long).mark_line().encode(
        x=alt.X("metric_date:T", title="Date"),
        y=alt.Y("rate:Q", title="Retention %", axis=alt.Axis(format=".1f"), scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", legend=alt.Legend(title=None)),
        tooltip=[alt.Tooltip("metric_date:T", title="Date", format="%b %d, %Y"), "series:N", alt.Tooltip("rate:Q", title="Retention %", format=".2f")],
    ).properties(title="Existing User Retention (Tier 1, US/DE/FR)", width="container", height=300)
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, client, mo):
    _query_country = """
    WITH base AS (
        SELECT client_id, mozfun.bits28.retention(days_seen_bits, submission_date) AS retention, country
        FROM `mozdata.fenix.active_users`
        WHERE
            submission_date BETWEEN DATE(2026, 1, 1) AND CURRENT_DATE()
            AND mozfun.bits28.active_in_range(days_seen_bits, -13, 1) = TRUE
            AND normalized_channel = 'release' AND country != 'IR'
            AND COALESCE(locale, '') NOT LIKE 'fa%'
            AND DATE_DIFF(submission_date, first_seen_date, DAY) > 27
            AND country IN ('US', 'DE', 'FR')
    )
    SELECT
        retention.day_13.metric_date, country,
        SAFE_DIVIDE(COUNTIF(retention.day_13.active_in_week_1), COUNTIF(retention.day_13.active_in_week_0)) * 100 AS retention_1_week_pct
    FROM base GROUP BY metric_date, country ORDER BY metric_date
    """
    _query_os = """
    WITH base AS (
        SELECT client_id, normalized_os_version, mozfun.bits28.retention(days_seen_bits, submission_date) AS retention
        FROM `mozdata.fenix.active_users`
        WHERE
            submission_date BETWEEN DATE(2026, 1, 1) AND CURRENT_DATE()
            AND mozfun.bits28.active_in_range(days_seen_bits, -13, 1) = TRUE
            AND normalized_channel = 'release' AND country != 'IR'
            AND COALESCE(locale, '') NOT LIKE 'fa%'
            AND DATE_DIFF(submission_date, first_seen_date, DAY) > 27
            AND country IN ('US', 'DE', 'FR')
    )
    SELECT
        retention.day_13.metric_date, normalized_os_version,
        COUNT(DISTINCT client_id) AS total_clients,
        SAFE_DIVIDE(COUNTIF(retention.day_13.active_in_week_1), COUNTIF(retention.day_13.active_in_week_0)) * 100 AS retention_1_week_pct
    FROM base GROUP BY metric_date, normalized_os_version HAVING total_clients > 99999 ORDER BY metric_date
    """
    _df_c = client.query(_query_country).to_dataframe()
    _df_c["metric_date"] = _df_c["metric_date"].astype(str)
    _df_os = client.query(_query_os).to_dataframe()
    _df_os["metric_date"] = _df_os["metric_date"].astype(str)

    _chart_c = alt.Chart(_df_c).mark_line().encode(
        x=alt.X("metric_date:T", title="Date"),
        y=alt.Y("retention_1_week_pct:Q", title="1-Week Retention %", axis=alt.Axis(format=".1f"), scale=alt.Scale(zero=False)),
        color=alt.Color("country:N", legend=alt.Legend(title="Country")),
        tooltip=[alt.Tooltip("metric_date:T", format="%b %d, %Y"), "country:N", alt.Tooltip("retention_1_week_pct:Q", format=".2f")],
    ).properties(title="1-Week Retention by Country", width=430, height=280)

    _chart_os = alt.Chart(_df_os).mark_line().encode(
        x=alt.X("metric_date:T", title="Date"),
        y=alt.Y("retention_1_week_pct:Q", title="1-Week Retention %", axis=alt.Axis(format=".1f"), scale=alt.Scale(zero=False)),
        color=alt.Color("normalized_os_version:N", legend=alt.Legend(title="OS Version")),
        tooltip=[alt.Tooltip("metric_date:T", format="%b %d, %Y"), "normalized_os_version:N", alt.Tooltip("retention_1_week_pct:Q", format=".2f")],
    ).properties(title="1-Week Retention by OS Version", width=430, height=280)

    mo.ui.altair_chart(alt.hconcat(_chart_c, _chart_os).resolve_scale(color="independent"))
    return


@app.cell
def _(mo):
    mo.md("""
    ## Section 6: Newer-Day Users Have Been Contributing Less to DAU Since Early Jan
    DAU decomposed by user age: first-day, days 2–7, days 8–28, and 28+ day users. The days 8–28 cohort (green) shows a steep YoY decline starting ~Jan 2026, while 28+ day users remain stable.
    """)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    WITH daily AS (
        SELECT
            submission_date,
            DATE_DIFF(submission_date, first_seen_date, DAY) AS age_days,
            COUNT(*) AS clients
        FROM `mozdata.fenix.active_users`
        WHERE submission_date >= '2023-12-25'
            AND normalized_channel = 'release'
            AND is_dau
            AND country != 'IR'
            AND locale NOT LIKE 'fa%'
        GROUP BY submission_date, age_days
    ),
    bucketed AS (
        SELECT
            submission_date,
            SUM(IF(age_days = 0, clients, 0)) AS first_day,
            SUM(IF(age_days BETWEEN 1 AND 6, clients, 0)) AS days_2_7,
            SUM(IF(age_days BETWEEN 7 AND 27, clients, 0)) AS days_8_28,
            SUM(IF(age_days >= 28, clients, 0)) AS days_28_plus
        FROM daily GROUP BY submission_date
    )
    SELECT
        submission_date,
        ROUND(AVG(first_day) OVER w) AS first_day_7d_avg,
        ROUND(AVG(days_2_7) OVER w) AS days_2_7_7d_avg,
        ROUND(AVG(days_8_28) OVER w) AS days_8_28_7d_avg,
        ROUND(AVG(days_28_plus) OVER w) AS days_28_plus_7d_avg
    FROM bucketed
    WHERE submission_date >= '2024-01-01'
    WINDOW w AS (ORDER BY submission_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    ORDER BY submission_date
    """
    _df = client.query(_query).to_dataframe()
    _df["submission_date"] = _df["submission_date"].astype(str)
    _long = _df.melt(
        id_vars="submission_date",
        value_vars=["first_day_7d_avg", "days_2_7_7d_avg", "days_8_28_7d_avg", "days_28_plus_7d_avg"],
        var_name="series",
        value_name="dau",
    ).replace({
        "first_day_7d_avg": "First Day",
        "days_2_7_7d_avg": "Days 2–7",
        "days_8_28_7d_avg": "Days 8–28",
        "days_28_plus_7d_avg": "Days 28+",
    })
    _hover = alt.selection_point(fields=["submission_date"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_long).encode(
        x=alt.X("submission_date:T", title="Date"),
        color=alt.Color("series:N", legend=alt.Legend(title="Age Vintage")),
    )
    _lines = _base.mark_line().encode(
        y=alt.Y("dau:Q", title="DAU (7d avg)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(size=60, filled=True).encode(
        y="dau:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("submission_date:T", title="Date", format="%b %d, %Y"), "series:N", alt.Tooltip("dau:Q", format=",.0f")],
    ).add_params(_hover)
    _rule = alt.Chart(_long).mark_rule(color="gray", strokeWidth=1).encode(x="submission_date:T").transform_filter(_hover)
    mo.ui.altair_chart((_lines + _rule + _points).properties(title="DAU by Age Vintage — 7 Day Avg", width="container", height=350))
    return


@app.cell
def _(alt, client, mo):
    _query = """
    WITH daily AS (
        SELECT
            submission_date,
            DATE_DIFF(submission_date, first_seen_date, DAY) AS age_days,
            COUNT(*) AS clients
        FROM `mozdata.fenix.active_users`
        WHERE submission_date >= '2022-12-04'
            AND normalized_channel = 'release' AND is_dau
            AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
        GROUP BY 1, 2
    ),
    bucketed AS (
        SELECT submission_date,
            SUM(IF(age_days = 0, clients, 0)) AS first_day,
            SUM(IF(age_days BETWEEN 1 AND 6, clients, 0)) AS days_2_7,
            SUM(IF(age_days BETWEEN 7 AND 27, clients, 0)) AS days_8_28,
            SUM(IF(age_days >= 28, clients, 0)) AS days_28_plus
        FROM daily GROUP BY 1
    ),
    smoothed AS (
        SELECT submission_date,
            AVG(first_day) OVER w AS first_day_28d,
            AVG(days_2_7) OVER w AS days_2_7_28d,
            AVG(days_8_28) OVER w AS days_8_28_28d,
            AVG(days_28_plus) OVER w AS days_28_plus_28d
        FROM bucketed WINDOW w AS (ORDER BY submission_date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)
    ),
    yoy AS (
        SELECT a.submission_date,
            SAFE_DIVIDE(a.first_day_28d - b.first_day_28d, b.first_day_28d) * 100 AS yoy_first_day,
            SAFE_DIVIDE(a.days_2_7_28d - b.days_2_7_28d, b.days_2_7_28d) * 100 AS yoy_days_2_7,
            SAFE_DIVIDE(a.days_8_28_28d - b.days_8_28_28d, b.days_8_28_28d) * 100 AS yoy_days_8_28,
            SAFE_DIVIDE(a.days_28_plus_28d - b.days_28_plus_28d, b.days_28_plus_28d) * 100 AS yoy_days_28_plus
        FROM smoothed AS a
        INNER JOIN smoothed AS b ON a.submission_date = DATE_ADD(b.submission_date, INTERVAL 1 YEAR)
        WHERE a.submission_date >= '2024-01-01' AND b.first_day_28d IS NOT NULL
    )
    SELECT submission_date AS ds, 'First Day' AS series, yoy_first_day AS yoy_pct FROM yoy
    UNION ALL SELECT submission_date, 'Days 2–7', yoy_days_2_7 FROM yoy
    UNION ALL SELECT submission_date, 'Days 8–28', yoy_days_8_28 FROM yoy
    UNION ALL SELECT submission_date, 'Days 28+', yoy_days_28_plus FROM yoy
    ORDER BY ds, series
    """
    _df = client.query(_query).to_dataframe()
    _df["ds"] = _df["ds"].astype(str)
    _hover = alt.selection_point(fields=["ds"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_df).encode(
        x=alt.X("ds:T", title="Date"),
        color=alt.Color("series:N", legend=alt.Legend(title="Age Vintage")),
    )
    _lines = _base.mark_line().encode(
        y=alt.Y("yoy_pct:Q", title="YoY %", axis=alt.Axis(format=".1f"), scale=alt.Scale(zero=False)),
    )
    _zero = alt.Chart({"values": [{}]}).mark_rule(color="gray", strokeDash=[2, 2], strokeWidth=1).encode(y=alt.datum(0))
    _points = _base.mark_point(size=60, filled=True).encode(
        y="yoy_pct:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("ds:T", title="Date", format="%b %d, %Y"), "series:N", alt.Tooltip("yoy_pct:Q", title="YoY %", format=".2f")],
    ).add_params(_hover)
    _rule = alt.Chart(_df).mark_rule(color="gray", strokeWidth=1).encode(x="ds:T").transform_filter(_hover)
    mo.ui.altair_chart((_lines + _zero + _rule + _points).properties(title="Fenix DAU YoY % Change by Age Vintage", width="container", height=350))
    return


@app.cell
def _(mo):
    mo.md("""
    ## Section 7: New User DAU Deep Dive
    DAU from new users on the latest version is down since v147. The drop is concentrated among install_source = null. A ~200k drop in new user DAU coincides with 147.0.1 (Jan ~17), but only ~18k in new profiles — suggesting something beyond just the client_id regen fix. Pings are also decreasing, meaning we are genuinely ingesting less data from new users.
    """)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    DECLARE start_date DATE DEFAULT DATE(2025,8,1);
    DECLARE end_sample INT64 DEFAULT 4;
    DECLARE m FLOAT64 DEFAULT 100 / (end_sample + 1);
    WITH releases AS (
        SELECT date AS start_date, major_version AS current_version,
            DATE_SUB(LEAD(date,1) OVER (ORDER BY date), INTERVAL 1 DAY) AS end_date
        FROM telemetry.releases WHERE category = 'major' AND product = 'firefox' AND date >= start_date
    ),
    baseline AS (
        SELECT submission_date,
            CASE WHEN DATE_DIFF(submission_date, first_seen_date, DAY) < 27 THEN 'new' ELSE 'old' END AS age,
            mozfun.norm.browser_version_info(app_version).major_version,
            m * COUNT(1) AS dau
        FROM fenix.active_users b
        WHERE submission_date >= start_date AND sample_id <= end_sample
            AND normalized_channel = 'release' AND is_dau
            AND DATE_DIFF(submission_date, first_seen_date, DAY) >= 0
            AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
        GROUP BY 1,2,3
    ),
    behind AS (
        SELECT submission_date, age,
            IF((SAFE_CAST(major_version AS FLOAT64) - SAFE_CAST(current_version AS FLOAT64)) > 0, 0,
               (SAFE_CAST(major_version AS FLOAT64) - SAFE_CAST(current_version AS FLOAT64))) AS versions_behind,
            SUM(dau) AS n_baseline_gcid
        FROM baseline b JOIN releases r ON b.submission_date BETWEEN r.start_date AND COALESCE(r.end_date, CURRENT_DATE)
        GROUP BY 1,2,3
    )
    SELECT submission_date, age, SUM(n_baseline_gcid) AS dau,
        SUM(SUM(n_baseline_gcid)) OVER (PARTITION BY submission_date) AS total_dau
    FROM behind WHERE versions_behind = 0
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    _df = client.query(_query).to_dataframe()
    _df["submission_date"] = _df["submission_date"].astype(str)
    _hover = alt.selection_point(fields=["submission_date"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_df).encode(
        x=alt.X("submission_date:T", title="Date"),
        color=alt.Color("age:N", legend=alt.Legend(title="User Age")),
    )
    _lines = _base.mark_line().encode(
        y=alt.Y("dau:Q", title="DAU (latest version)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(size=60, filled=True).encode(
        y="dau:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("submission_date:T", title="Date", format="%b %d, %Y"), "age:N", alt.Tooltip("dau:Q", format=",.0f")],
    ).add_params(_hover)
    _rule = alt.Chart(_df).mark_rule(color="gray", strokeWidth=1).encode(x="submission_date:T").transform_filter(_hover)
    mo.ui.altair_chart((_lines + _rule + _points).properties(title="New User DAU on Latest Version (new vs old)", width="container", height=300))
    return


@app.cell
def _(alt, client, mo):
    _query = """
    DECLARE start_date DATE DEFAULT DATE(2025,8,1);
    DECLARE end_sample INT64 DEFAULT 4;
    DECLARE m FLOAT64 DEFAULT 100 / (end_sample + 1);
    WITH releases AS (
        SELECT date AS start_date, major_version AS current_version,
            DATE_SUB(LEAD(date,1) OVER (ORDER BY date), INTERVAL 1 DAY) AS end_date
        FROM telemetry.releases WHERE category = 'major' AND product = 'firefox' AND date >= start_date
    ),
    baseline AS (
        SELECT submission_date,
            CASE WHEN DATE_DIFF(submission_date, first_seen_date, DAY) < 27 THEN 'new' ELSE 'old' END AS age,
            mozfun.norm.browser_version_info(app_version).major_version, country,
            m * COUNT(1) AS dau
        FROM fenix.active_users b
        WHERE submission_date >= start_date AND sample_id <= end_sample
            AND normalized_channel = 'release' AND is_dau
            AND DATE_DIFF(submission_date, first_seen_date, DAY) >= 0
            AND country IN ('US', 'DE', 'FR')
            AND COALESCE(locale, '') NOT LIKE 'fa%'
        GROUP BY 1,2,3,4
    ),
    behind AS (
        SELECT submission_date, age, country,
            IF((SAFE_CAST(major_version AS FLOAT64) - SAFE_CAST(current_version AS FLOAT64)) > 0, 0,
               (SAFE_CAST(major_version AS FLOAT64) - SAFE_CAST(current_version AS FLOAT64))) AS versions_behind,
            SUM(dau) AS n_baseline_gcid
        FROM baseline b JOIN releases r ON b.submission_date BETWEEN r.start_date AND COALESCE(r.end_date, CURRENT_DATE)
        GROUP BY 1,2,3,4
    )
    SELECT submission_date, country, SUM(n_baseline_gcid) AS dau
    FROM behind WHERE versions_behind = 0 AND age = 'new'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    _df = client.query(_query).to_dataframe()
    _df["submission_date"] = _df["submission_date"].astype(str)
    _chart = alt.Chart(_df).mark_line().encode(
        x=alt.X("submission_date:T", title="Date"),
        y=alt.Y("dau:Q", title="New User DAU (latest version)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        color=alt.Color("country:N", legend=alt.Legend(title="Country")),
        tooltip=[alt.Tooltip("submission_date:T", format="%b %d, %Y"), "country:N", alt.Tooltip("dau:Q", format=",.0f")],
    ).properties(title="New User DAU on Latest Version — US/DE/FR", width="container", height=300)
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    DECLARE start_date DATE DEFAULT DATE(2025,8,1);
    DECLARE end_sample INT64 DEFAULT 4;
    DECLARE m FLOAT64 DEFAULT 100 / (end_sample + 1);
    WITH releases AS (
        SELECT date AS start_date, major_version AS current_version,
            DATE_SUB(LEAD(date,1) OVER (ORDER BY date), INTERVAL 1 DAY) AS end_date
        FROM telemetry.releases WHERE category = 'major' AND product = 'firefox' AND date >= start_date
    ),
    baseline AS (
        SELECT submission_date,
            CASE WHEN DATE_DIFF(submission_date, first_seen_date, DAY) < 27 THEN 'new' ELSE 'old' END AS age,
            mozfun.norm.browser_version_info(app_version).major_version, install_source,
            m * COUNT(1) AS dau
        FROM fenix.active_users b
        WHERE submission_date >= start_date AND sample_id <= end_sample
            AND normalized_channel = 'release' AND is_dau
            AND DATE_DIFF(submission_date, first_seen_date, DAY) >= 0
            AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
        GROUP BY 1,2,3,4
    ),
    behind AS (
        SELECT submission_date, age, install_source,
            IF((SAFE_CAST(major_version AS FLOAT64) - SAFE_CAST(current_version AS FLOAT64)) > 0, 0,
               (SAFE_CAST(major_version AS FLOAT64) - SAFE_CAST(current_version AS FLOAT64))) AS versions_behind,
            SUM(dau) AS n_baseline_gcid
        FROM baseline b JOIN releases r ON b.submission_date BETWEEN r.start_date AND COALESCE(r.end_date, CURRENT_DATE)
        GROUP BY 1,2,3,4
    )
    SELECT submission_date, COALESCE(install_source, 'null') AS install_source, SUM(n_baseline_gcid) AS dau
    FROM behind WHERE versions_behind = 0 AND age = 'new'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    _df = client.query(_query).to_dataframe()
    _df["submission_date"] = _df["submission_date"].astype(str)
    _top_sources = _df.groupby("install_source")["dau"].sum().nlargest(8).index.tolist()
    _df_top = _df[_df["install_source"].isin(_top_sources)].copy()
    _chart = alt.Chart(_df_top).mark_line().encode(
        x=alt.X("submission_date:T", title="Date"),
        y=alt.Y("dau:Q", title="New User DAU (latest version)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        color=alt.Color("install_source:N", legend=alt.Legend(title="Install Source")),
        tooltip=[alt.Tooltip("submission_date:T", format="%b %d, %Y"), "install_source:N", alt.Tooltip("dau:Q", format=",.0f")],
    ).properties(title="New User DAU by Install Source (latest version, top 8)", width="container", height=300)
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, client, mo):
    _query_np_uptodate = """
    DECLARE start_date DATE DEFAULT DATE(2025,12,1);
    WITH releases AS (
        SELECT date AS start_date, major_version, minor_version, patch_version,
            DATE_SUB(LEAD(date,1) OVER (ORDER BY date), INTERVAL 1 DAY) AS end_date
        FROM telemetry.releases WHERE category IN ('major', 'stability') AND product = 'firefox' AND date >= start_date
    ),
    np AS (
        SELECT first_seen_date, app_version, SUM(new_profiles) AS new_profiles
        FROM fenix.new_profiles b
        WHERE first_seen_date >= start_date AND normalized_channel = 'release'
        GROUP BY 1,2
    ),
    behind AS (
        SELECT first_seen_date, app_version,
            (r.major_version = mozfun.norm.browser_version_info(app_version).major_version)
            AND (r.minor_version = mozfun.norm.browser_version_info(app_version).minor_version)
            AND (COALESCE(r.patch_version,0) = COALESCE(mozfun.norm.browser_version_info(app_version).patch_revision,0)) AS updated,
            SUM(new_profiles) AS new_profiles
        FROM np b JOIN releases r ON b.first_seen_date BETWEEN r.start_date AND COALESCE(r.end_date, CURRENT_DATE)
        GROUP BY 1,2,3
    )
    SELECT first_seen_date, app_version, new_profiles,
        SUM(new_profiles) OVER (PARTITION BY first_seen_date) AS total_new_profiles
    FROM behind WHERE updated ORDER BY 1,2
    """
    _query_np_null = """
    DECLARE start_date DATE DEFAULT DATE(2025,12,1);
    WITH releases AS (
        SELECT date AS start_date, major_version, minor_version, patch_version,
            DATE_SUB(LEAD(date,1) OVER (ORDER BY date), INTERVAL 1 DAY) AS end_date
        FROM telemetry.releases WHERE category IN ('major', 'stability') AND product = 'firefox' AND date >= start_date
    ),
    np AS (
        SELECT first_seen_date, app_version, SUM(new_profiles) AS new_profiles
        FROM fenix.new_profiles b
        WHERE first_seen_date >= start_date AND normalized_channel = 'release'
            AND install_source IS NULL AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
        GROUP BY 1,2
    ),
    behind AS (
        SELECT first_seen_date, app_version,
            (r.major_version = mozfun.norm.browser_version_info(app_version).major_version)
            AND (r.minor_version = mozfun.norm.browser_version_info(app_version).minor_version)
            AND (COALESCE(r.patch_version,0) = COALESCE(mozfun.norm.browser_version_info(app_version).patch_revision,0)) AS updated,
            SUM(new_profiles) AS new_profiles
        FROM np b JOIN releases r ON b.first_seen_date BETWEEN r.start_date AND COALESCE(r.end_date, CURRENT_DATE)
        GROUP BY 1,2,3
    )
    SELECT first_seen_date, app_version, new_profiles,
        SUM(new_profiles) OVER (PARTITION BY first_seen_date) AS total_new_profiles
    FROM behind WHERE updated ORDER BY 1,2
    """
    _df_up = client.query(_query_np_uptodate).to_dataframe()
    _df_up["first_seen_date"] = _df_up["first_seen_date"].astype(str)
    _df_null = client.query(_query_np_null).to_dataframe()
    _df_null["first_seen_date"] = _df_null["first_seen_date"].astype(str)

    def _np_by_version_chart(df, title):
        _agg = df.groupby(["first_seen_date", "app_version"])["new_profiles"].sum().reset_index()
        _top = _agg.groupby("app_version")["new_profiles"].sum().nlargest(10).index.tolist()
        _agg = _agg[_agg["app_version"].isin(_top)]
        return alt.Chart(_agg).mark_line(strokeWidth=1.5).encode(
            x=alt.X("first_seen_date:T", title="Date"),
            y=alt.Y("new_profiles:Q", title="New Profiles", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
            color=alt.Color("app_version:N", legend=alt.Legend(title="Version")),
            tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), "app_version:N", alt.Tooltip("new_profiles:Q", format=",.0f")],
        ).properties(title=title, width=430, height=280)

    mo.ui.altair_chart(alt.hconcat(
        _np_by_version_chart(_df_up, "NP from Up-to-Date Versions"),
        _np_by_version_chart(_df_null, "NP — Null Install Source, Up-to-Date"),
    ).resolve_scale(color="independent"))
    return


@app.cell
def _(mo):
    mo.md("""
    ## Section 8: Client Regen & Bot Analysis
    The non-DAU spike starting mid-Feb was driven by bot influx (ISP: "Suga Pte."), not DAU misclassification. Filtering out that ISP eliminates the spike. Client_id regens (~18k/day) explain part of the NP drop but not the much larger DAU drop. All three of daily client_ids, DAU, AND pings are dropping in tandem.
    """)
    return


@app.cell
def _(alt, client, mo):
    _query_non_dau = """
    WITH daily AS (
        SELECT submission_date, DATE_DIFF(submission_date, first_seen_date, DAY) AS age_days, COUNT(*) AS clients
        FROM `mozdata.fenix.active_users`
        WHERE (submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 187 DAY)
            AND normalized_channel = 'release' AND NOT is_dau AND NOT is_wau AND NOT is_mau
            AND is_mobile AND country NOT IN ('IR','ID','IN','CN','BR') AND locale NOT LIKE 'fa%')
        GROUP BY submission_date, age_days
    ),
    bucketed AS (
        SELECT submission_date,
            SUM(IF(age_days = 0, clients, 0)) AS first_day,
            SUM(IF(age_days BETWEEN 1 AND 6, clients, 0)) AS days_2_7,
            SUM(IF(age_days BETWEEN 7 AND 27, clients, 0)) AS days_8_28,
            SUM(IF(age_days >= 28, clients, 0)) AS days_28_plus
        FROM daily GROUP BY submission_date
    )
    SELECT submission_date,
        ROUND(AVG(first_day) OVER w) AS first_day_7d_avg,
        ROUND(AVG(days_2_7) OVER w) AS days_2_7_7d_avg,
        ROUND(AVG(days_8_28) OVER w) AS days_8_28_7d_avg,
        ROUND(AVG(days_28_plus) OVER w) AS days_28_plus_7d_avg
    FROM bucketed WHERE submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    WINDOW w AS (ORDER BY submission_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    ORDER BY submission_date
    """
    _query_ex_suga = """
    WITH daily AS (
        SELECT submission_date, DATE_DIFF(submission_date, first_seen_date, DAY) AS age_days, COUNT(*) AS clients
        FROM `mozdata.fenix.active_users`
        WHERE submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 187 DAY)
            AND normalized_channel = 'release' AND NOT is_dau AND NOT is_wau AND NOT is_mau
            AND is_mobile AND country NOT IN ('IR','ID','IN','CN','BR') AND locale NOT LIKE 'fa%'
            AND isp NOT LIKE 'Suga%'
        GROUP BY submission_date, age_days
    ),
    bucketed AS (
        SELECT submission_date,
            SUM(IF(age_days = 0, clients, 0)) AS first_day,
            SUM(IF(age_days BETWEEN 1 AND 6, clients, 0)) AS days_2_7,
            SUM(IF(age_days BETWEEN 7 AND 27, clients, 0)) AS days_8_28,
            SUM(IF(age_days >= 28, clients, 0)) AS days_28_plus
        FROM daily GROUP BY submission_date
    )
    SELECT submission_date,
        ROUND(AVG(first_day) OVER w) AS first_day_7d_avg,
        ROUND(AVG(days_2_7) OVER w) AS days_2_7_7d_avg,
        ROUND(AVG(days_8_28) OVER w) AS days_8_28_7d_avg,
        ROUND(AVG(days_28_plus) OVER w) AS days_28_plus_7d_avg
    FROM bucketed WHERE submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    WINDOW w AS (ORDER BY submission_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    ORDER BY submission_date
    """
    _df_nd = client.query(_query_non_dau).to_dataframe()
    _df_nd["submission_date"] = _df_nd["submission_date"].astype(str)
    _df_es = client.query(_query_ex_suga).to_dataframe()
    _df_es["submission_date"] = _df_es["submission_date"].astype(str)
    _age_cols = ["first_day_7d_avg", "days_2_7_7d_avg", "days_8_28_7d_avg", "days_28_plus_7d_avg"]
    _age_labels = {"first_day_7d_avg": "First Day", "days_2_7_7d_avg": "Days 2–7", "days_8_28_7d_avg": "Days 8–28", "days_28_plus_7d_avg": "Days 28+"}

    def _age_chart(df, title):
        _long = df.melt(id_vars="submission_date", value_vars=_age_cols, var_name="series", value_name="clients").replace(_age_labels)
        return alt.Chart(_long).mark_line().encode(
            x=alt.X("submission_date:T", title="Date"),
            y=alt.Y("clients:Q", title="Non-DAU Clients (7d avg)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
            color=alt.Color("series:N", legend=alt.Legend(title="Age")),
            tooltip=[alt.Tooltip("submission_date:T", format="%b %d, %Y"), "series:N", alt.Tooltip("clients:Q", format=",.0f")],
        ).properties(title=title, width=430, height=280)

    mo.ui.altair_chart(alt.hconcat(
        _age_chart(_df_nd, "Non-DAU by Age (7d avg)"),
        _age_chart(_df_es, "Non-DAU by Age (ex Suga Pte)"),
    ).resolve_scale(color="shared"))
    return


@app.cell
def _(alt, client, mo):
    _query = """
    WITH regen_clients AS (
        SELECT DATE(submission_timestamp) AS submission_date, client_info.client_id,
            LOGICAL_OR(metrics.string.glean_health_exception_state = 'regen-db') AS has_regen_signature
        FROM `mozdata.fenix.health`
        WHERE DATE(submission_timestamp) > '2026-01-01' AND ping_info.reason = 'pre_init'
            AND mozfun.norm.browser_version_info(client_info.app_display_version).major_version >= 145
        GROUP BY 1, 2
    ),
    new_profiles AS (
        SELECT first_seen_date AS submission_date, client_id
        FROM `mozdata.fenix.new_profile_clients`
        WHERE first_seen_date > '2026-01-01'
            AND mozfun.norm.browser_version_info(app_version).major_version >= 145
    ),
    j AS (
        SELECT * EXCEPT (has_regen_signature), COALESCE(has_regen_signature, FALSE) AS has_regen_signature
        FROM new_profiles LEFT JOIN regen_clients USING (client_id, submission_date)
    )
    SELECT submission_date, COUNT(*) AS new_profiles, COUNTIF(has_regen_signature) AS new_profiles_regen_signature
    FROM j GROUP BY 1 ORDER BY 1
    """
    _df = client.query(_query).to_dataframe()
    _df["submission_date"] = _df["submission_date"].astype(str)
    _long = _df.melt(id_vars="submission_date", value_vars=["new_profiles", "new_profiles_regen_signature"], var_name="series", value_name="count")
    _long["series"] = _long["series"].replace({"new_profiles": "New Profiles", "new_profiles_regen_signature": "Regen Signature"})
    _chart = alt.Chart(_long).mark_line().encode(
        x=alt.X("submission_date:T", title="Date"),
        y=alt.Y("count:Q", title="Profiles", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", legend=alt.Legend(title=None)),
        tooltip=[alt.Tooltip("submission_date:T", format="%b %d, %Y"), "series:N", alt.Tooltip("count:Q", format=",.0f")],
    ).properties(title="New Profiles vs Regen Signature", width="container", height=300)
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    SELECT
        first_seen_date AS ds, app_version AS version, SUM(new_profiles) AS new_profiles
    FROM `moz-fx-data-shared-prod.telemetry.mobile_new_profiles`
    WHERE
        first_seen_date > DATE_SUB(CURRENT_DATE(), INTERVAL 4 MONTH)
        AND install_source IS NULL AND app_name = 'Fenix'
        AND mozfun.norm.browser_version_info(app_version).major_version >= 144
    GROUP BY 1, 2 ORDER BY 1 DESC
    """
    _df = client.query(_query).to_dataframe()
    _df["ds"] = _df["ds"].astype(str)
    _top = _df.groupby("version")["new_profiles"].sum().nlargest(10).index.tolist()
    _df = _df[_df["version"].isin(_top)]
    _chart = alt.Chart(_df).mark_line(strokeWidth=1.5).encode(
        x=alt.X("ds:T", title="Date"),
        y=alt.Y("new_profiles:Q", title="New Profiles", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        color=alt.Color("version:N", legend=alt.Legend(title="App Version")),
        tooltip=[alt.Tooltip("ds:T", format="%b %d, %Y"), "version:N", alt.Tooltip("new_profiles:Q", format=",.0f")],
    ).properties(title="NP by Version — Null Install Source (top 10 versions)", width="container", height=300)
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    DECLARE start_date DATE DEFAULT DATE(2026, 1, 1);
    DECLARE end_sample INT64 DEFAULT 9;
    DECLARE m FLOAT64 DEFAULT 100 / (end_sample + 1);
    WITH baseline AS (
        SELECT DATE(submission_timestamp) AS submission_date, client_info.client_id,
            MAX(DATE_DIFF(DATE(submission_timestamp), DATE(mozfun.glean.parse_datetime(client_info.first_run_date)), DAY)) AS age,
            SUM(COALESCE(`moz-fx-data-shared-prod.udf.glean_timespan_seconds`(metrics.timespan.glean_baseline_duration), 0)) > 0 AS is_dau,
            COUNT(*) AS pings
        FROM `mozdata.fenix.baseline` AS b
        WHERE DATE(submission_timestamp) >= start_date AND sample_id <= end_sample
            AND normalized_channel = 'release' AND metrics.string.first_session_install_source IS NULL
            AND metadata.geo.country != 'IR' AND COALESCE(client_info.locale, '') NOT LIKE 'fa%'
        GROUP BY 1, 2
    )
    SELECT submission_date,
        m * COUNTIF(age < 28) AS clients_new, m * COUNTIF(age >= 28) AS clients_old,
        m * SUM(IF(age < 28, pings, 0)) AS pings_new, m * SUM(IF(age >= 28, pings, 0)) AS pings_old,
        m * COUNTIF(is_dau AND age < 28) AS dau_new, m * COUNTIF(is_dau AND age >= 28) AS dau_old
    FROM baseline GROUP BY 1 ORDER BY 1
    """
    _df = client.query(_query).to_dataframe()
    _df["submission_date"] = _df["submission_date"].astype(str)

    def _age_split_chart(df, col_new, col_old, title, y_title):
        _long = df.melt(id_vars="submission_date", value_vars=[col_new, col_old], var_name="series", value_name="value")
        _long["series"] = _long["series"].replace({col_new: "New (<28d)", col_old: "Old (28d+)"})
        return alt.Chart(_long).mark_line().encode(
            x=alt.X("submission_date:T", title="Date"),
            y=alt.Y("value:Q", title=y_title, axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
            color=alt.Color("series:N", legend=alt.Legend(title=None)),
            tooltip=[alt.Tooltip("submission_date:T", format="%b %d, %Y"), "series:N", alt.Tooltip("value:Q", format=",.0f")],
        ).properties(title=title, width=290, height=220)

    mo.ui.altair_chart(alt.hconcat(
        _age_split_chart(_df, "dau_new", "dau_old", "DAU by Age (null install source)", "DAU"),
        _age_split_chart(_df, "pings_new", "pings_old", "Pings by Age (null install source)", "Pings"),
        _age_split_chart(_df, "clients_new", "clients_old", "Clients by Age (null install source)", "Clients"),
    ).resolve_scale(color="shared"))
    return


@app.cell
def _(mo):
    mo.md("""
    ## Section 9: DOU & Engagement per New Profile
    Days of use per new profile is declining — a double whammy alongside falling new profile volume. Total DOU in first 28 days dropped sharply for install_source = null users (avg from ~3.5 to ~2.5). Searches and ad clicks are also down. The drop is only among the official "Mozilla" distribution_id. Modern Android (15+16) gets more DOU per user than older versions.
    """)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    DECLARE start_date_2026 DATE DEFAULT DATE(2026, 1, 1);
    WITH np AS (
        SELECT first_seen_date, client_id
        FROM fenix.new_profile_clients n
        WHERE first_seen_date >= start_date_2026 AND normalized_channel = 'release'
            AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
    ),
    dous AS (
        SELECT np.first_seen_date, np.client_id, BIT_COUNT(days_active_bits & 127) AS dou
        FROM np LEFT JOIN fenix.active_users a ON np.client_id = a.client_id
            AND a.submission_date = DATE_ADD(np.first_seen_date, INTERVAL 6 DAY)
        WHERE submission_date >= start_date_2026
    ),
    agg AS (
        SELECT first_seen_date, SUM(dou) AS total_dou, AVG(dou) AS avg_dou_pc
        FROM dous WHERE first_seen_date <= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
        GROUP BY 1 ORDER BY 1
    )
    SELECT *, AVG(total_dou) OVER (ORDER BY first_seen_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS total_dou7ma,
        AVG(avg_dou_pc) OVER (ORDER BY first_seen_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS dou_pc7ma
    FROM agg WHERE first_seen_date <= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY) ORDER BY 1
    """
    _df = client.query(_query).to_dataframe()
    _df["first_seen_date"] = _df["first_seen_date"].astype(str)

    _chart_total = alt.Chart(_df).mark_line(color="steelblue").encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("total_dou7ma:Q", title="Total DOU (7d MA)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), alt.Tooltip("total_dou7ma:Q", title="Total DOU", format=",.0f")],
    ).properties(title="Total DOU (7d MA)", width=430, height=280)

    _chart_avg = alt.Chart(_df).mark_line(color="darkorange").encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("dou_pc7ma:Q", title="Avg DOU per Client (7d MA)", scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), alt.Tooltip("dou_pc7ma:Q", title="Avg DOU", format=".2f")],
    ).properties(title="Avg DOU per Client (7d MA)", width=430, height=280)

    mo.ui.altair_chart(alt.hconcat(_chart_total, _chart_avg))
    return


@app.cell
def _(alt, client, mo):
    _query = """
    DECLARE start_date_2026 DATE DEFAULT DATE(2026, 1, 1);
    WITH np AS (
        SELECT first_seen_date, client_id,
            CASE WHEN install_source = 'com.android.vending' THEN 'Play Store'
                 WHEN (install_source IS NULL OR install_source = '') THEN 'Null'
                 WHEN install_source LIKE '%packageinstaller%' THEN 'Sideload'
                 ELSE 'Other' END AS install_source_bucket
        FROM fenix.new_profile_clients n
        WHERE first_seen_date >= start_date_2026 AND normalized_channel = 'release'
            AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
    ),
    dous AS (
        SELECT np.first_seen_date, np.client_id, install_source_bucket, BIT_COUNT(days_active_bits) AS dou
        FROM np LEFT JOIN fenix.active_users a ON np.client_id = a.client_id
            AND a.submission_date = DATE_ADD(np.first_seen_date, INTERVAL 27 DAY)
        WHERE submission_date >= start_date_2026
    )
    SELECT first_seen_date, install_source_bucket, SUM(dou) AS total_dou, AVG(dou) AS avg_dou_pc
    FROM dous WHERE first_seen_date <= DATE_SUB(CURRENT_DATE, INTERVAL 28 DAY)
    GROUP BY 1,2 ORDER BY 1,2
    """
    _df = client.query(_query).to_dataframe()
    _df["first_seen_date"] = _df["first_seen_date"].astype(str)

    _chart_t = alt.Chart(_df).mark_line().encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("total_dou:Q", title="Total DOU (28d)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        color=alt.Color("install_source_bucket:N", legend=alt.Legend(title="Install Source")),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), "install_source_bucket:N", alt.Tooltip("total_dou:Q", format=",.0f")],
    ).properties(title="Total DOU per 28 Days by Install Source", width=430, height=280)

    _chart_a = alt.Chart(_df).mark_line().encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("avg_dou_pc:Q", title="Avg DOU per Client (28d)", scale=alt.Scale(zero=False)),
        color=alt.Color("install_source_bucket:N", legend=alt.Legend(title="Install Source")),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), "install_source_bucket:N", alt.Tooltip("avg_dou_pc:Q", format=".2f")],
    ).properties(title="Avg DOU per Client (28d) by Install Source", width=430, height=280)

    mo.ui.altair_chart(alt.hconcat(_chart_t, _chart_a).resolve_scale(color="shared"))
    return


@app.cell
def _(alt, client, mo):
    _query = """
    DECLARE start_date_2026 DATE DEFAULT DATE(2026, 1, 1);
    WITH np AS (
        SELECT first_seen_date, client_id,
            CASE WHEN install_source = 'com.android.vending' THEN 'Play Store'
                 WHEN (install_source IS NULL OR install_source = '') THEN 'Null'
                 WHEN install_source LIKE '%packageinstaller%' THEN 'Sideload'
                 ELSE 'Other' END AS install_source_bucket
        FROM fenix.new_profile_clients n
        WHERE first_seen_date >= start_date_2026 AND normalized_channel = 'release'
            AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
    ),
    search AS (
        SELECT np.first_seen_date, np.client_id, install_source_bucket,
            SUM(COALESCE(search_count, 0)) AS search_count, SUM(COALESCE(ad_click, 0)) AS ad_click
        FROM np LEFT JOIN search.mobile_search_clients_daily a
            ON np.client_id = a.client_id
            AND a.submission_date BETWEEN np.first_seen_date AND DATE_ADD(np.first_seen_date, INTERVAL 27 DAY)
        WHERE submission_date >= start_date_2026 AND app_name = 'Fenix'
        GROUP BY 1,2,3
    )
    SELECT first_seen_date, install_source_bucket,
        SUM(search_count) AS total_search_count, AVG(search_count) AS avg_search_count
    FROM search WHERE first_seen_date <= DATE_SUB(CURRENT_DATE, INTERVAL 28 DAY)
    GROUP BY 1,2 ORDER BY 1,2
    """
    _df = client.query(_query).to_dataframe()
    _df["first_seen_date"] = _df["first_seen_date"].astype(str)

    _chart_t = alt.Chart(_df).mark_line().encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("total_search_count:Q", title="Total Searches (28d)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        color=alt.Color("install_source_bucket:N", legend=alt.Legend(title="Install Source")),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), "install_source_bucket:N", alt.Tooltip("total_search_count:Q", format=",.0f")],
    ).properties(title="Total Searches per 28 Days by Install Source", width=430, height=280)

    _chart_a = alt.Chart(_df).mark_line().encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("avg_search_count:Q", title="Avg Searches per Client (28d)", scale=alt.Scale(zero=False)),
        color=alt.Color("install_source_bucket:N", legend=alt.Legend(title="Install Source")),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), "install_source_bucket:N", alt.Tooltip("avg_search_count:Q", format=".2f")],
    ).properties(title="Avg Searches per Client (28d)", width=430, height=280)

    mo.ui.altair_chart(alt.hconcat(_chart_t, _chart_a).resolve_scale(color="shared"))
    return


@app.cell
def _(alt, client, mo):
    _query_dbr = """
    DECLARE start_date_2026 DATE DEFAULT DATE(2026, 1, 1);
    WITH np AS (
        SELECT first_seen_date, client_id
        FROM fenix.new_profile_clients n
        WHERE first_seen_date >= start_date_2026 AND normalized_channel = 'release'
            AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
            AND (install_source IS NULL OR install_source = '')
    ),
    dous AS (
        SELECT np.first_seen_date, np.client_id,
            LOGICAL_OR(COALESCE(metrics.boolean.metrics_default_browser, FALSE)) AS is_default_browser
        FROM np LEFT JOIN fenix.baseline a ON np.client_id = a.client_info.client_id
            AND date(a.submission_timestamp) BETWEEN np.first_seen_date AND DATE_ADD(np.first_seen_date, INTERVAL 27 DAY)
        WHERE date(a.submission_timestamp) >= start_date_2026
        GROUP BY 1,2
    )
    SELECT first_seen_date, AVG(CAST(is_default_browser AS INT64)) AS avg_is_default_browser
    FROM dous WHERE first_seen_date <= DATE_SUB(CURRENT_DATE, INTERVAL 28 DAY)
    GROUP BY 1 ORDER BY 1
    """
    _query_dist = """
    DECLARE start_date_2026 DATE DEFAULT DATE(2026, 1, 1);
    WITH np AS (
        SELECT first_seen_date, client_id, distribution_id
        FROM fenix.new_profile_clients n
        WHERE first_seen_date >= start_date_2026 AND normalized_channel = 'release'
            AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
            AND (install_source IS NULL OR install_source = '')
    ),
    dous AS (
        SELECT np.first_seen_date, np.client_id, np.distribution_id, BIT_COUNT(days_active_bits) AS dou
        FROM np LEFT JOIN fenix.active_users a ON np.client_id = a.client_id
            AND a.submission_date = DATE_ADD(np.first_seen_date, INTERVAL 27 DAY)
        WHERE submission_date >= start_date_2026
    )
    SELECT first_seen_date, distribution_id, AVG(dou) AS avg_dou_pc
    FROM dous WHERE first_seen_date <= DATE_SUB(CURRENT_DATE, INTERVAL 28 DAY)
    GROUP BY 1,2 ORDER BY 1,2
    """
    _df_dbr = client.query(_query_dbr).to_dataframe()
    _df_dbr["first_seen_date"] = _df_dbr["first_seen_date"].astype(str)
    _df_dbr["pct"] = _df_dbr["avg_is_default_browser"] * 100

    _df_dist = client.query(_query_dist).to_dataframe()
    _df_dist["first_seen_date"] = _df_dist["first_seen_date"].astype(str)
    _top_dist = _df_dist.groupby("distribution_id")["avg_dou_pc"].mean().nlargest(8).index.tolist()
    _df_dist = _df_dist[_df_dist["distribution_id"].isin(_top_dist)]

    _chart_dbr = alt.Chart(_df_dbr).mark_line(color="steelblue").encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("pct:Q", title="Default Browser Rate %", axis=alt.Axis(format=".1f"), scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), alt.Tooltip("pct:Q", title="Default Browser %", format=".2f")],
    ).properties(title="Default Browser Rate (null install source, 28d)", width=430, height=280)

    _chart_dist = alt.Chart(_df_dist).mark_line().encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("avg_dou_pc:Q", title="Avg DOU per Client (28d)", scale=alt.Scale(zero=False)),
        color=alt.Color("distribution_id:N", legend=alt.Legend(title="Distribution")),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), "distribution_id:N", alt.Tooltip("avg_dou_pc:Q", format=".2f")],
    ).properties(title="Total DOU by Distribution (28d)", width=430, height=280)

    mo.ui.altair_chart(alt.hconcat(_chart_dbr, _chart_dist).resolve_scale(color="independent"))
    return


@app.cell
def _(alt, client, mo):
    _query = """
    DECLARE start_date_2026 DATE DEFAULT DATE(2026, 1, 1);
    WITH np AS (
        SELECT first_seen_date, client_id,
            CASE WHEN LEFT(os_version,2) IN ('15', '16') THEN 'modern (15/16)' ELSE 'old' END AS os_version_bucket
        FROM fenix.new_profile_clients n
        WHERE first_seen_date >= start_date_2026 AND normalized_channel = 'release'
            AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
            AND (install_source IS NULL OR install_source = '')
    ),
    dous AS (
        SELECT np.first_seen_date, np.client_id, np.os_version_bucket, BIT_COUNT(days_active_bits) AS dou
        FROM np LEFT JOIN fenix.active_users a ON np.client_id = a.client_id
            AND a.submission_date = DATE_ADD(np.first_seen_date, INTERVAL 27 DAY)
        WHERE submission_date >= start_date_2026
    )
    SELECT first_seen_date, os_version_bucket, SUM(dou) AS total_dou, AVG(dou) AS avg_dou_pc
    FROM dous WHERE first_seen_date <= DATE_SUB(CURRENT_DATE, INTERVAL 28 DAY)
    GROUP BY 1,2 ORDER BY 1,2
    """
    _df = client.query(_query).to_dataframe()
    _df["first_seen_date"] = _df["first_seen_date"].astype(str)

    _chart_t = alt.Chart(_df).mark_line().encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("total_dou:Q", title="Total DOU (28d)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        color=alt.Color("os_version_bucket:N", legend=alt.Legend(title="OS Version")),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), "os_version_bucket:N", alt.Tooltip("total_dou:Q", format=",.0f")],
    ).properties(title="Total DOU by OS Version (28d)", width=430, height=280)

    _chart_a = alt.Chart(_df).mark_line().encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("avg_dou_pc:Q", title="Avg DOU per Client (28d)", scale=alt.Scale(zero=False)),
        color=alt.Color("os_version_bucket:N", legend=alt.Legend(title="OS Version")),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), "os_version_bucket:N", alt.Tooltip("avg_dou_pc:Q", format=".2f")],
    ).properties(title="Avg DOU by OS Version (28d)", width=430, height=280)

    mo.ui.altair_chart(alt.hconcat(_chart_t, _chart_a).resolve_scale(color="shared"))
    return


@app.cell
def _(mo):
    mo.md("""
    ## Section 10: Ping-Level Analysis
    DOU per new client-id is dropping, but per-client baseline ping count is stable — no differential change in any ping type. First session ping clients are barely down, but first baseline ping clients show a definite drop.
    """)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    DECLARE start_date_2026 DATE DEFAULT DATE(2026, 1, 18);
    WITH np AS (
        SELECT first_seen_date, client_id
        FROM fenix.new_profile_clients n
        WHERE first_seen_date >= start_date_2026 AND normalized_channel = 'release'
            AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
            AND NOT (install_source IS NULL OR install_source = '')
            AND paid_vs_organic_gclid = 'Organic'
    ),
    dous AS (
        SELECT np.first_seen_date, np.client_id, COALESCE(COUNT(a.document_id), 0) AS pings
        FROM np LEFT JOIN fenix.baseline a ON np.client_id = a.client_info.client_id
            AND date(a.submission_timestamp) BETWEEN np.first_seen_date AND DATE_ADD(np.first_seen_date, INTERVAL 6 DAY)
        WHERE date(a.submission_timestamp) >= start_date_2026
        GROUP BY 1,2
    ),
    agg AS (
        SELECT first_seen_date, AVG(pings) AS avg_pings_client, SUM(pings) AS total_pings
        FROM dous WHERE first_seen_date <= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
        GROUP BY 1 ORDER BY 1
    )
    SELECT *, AVG(avg_pings_client) OVER (ORDER BY first_seen_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_pings_client7ma,
        AVG(total_pings) OVER (ORDER BY first_seen_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS total_pings_pc7ma
    FROM agg WHERE first_seen_date <= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY) ORDER BY 1
    """
    _df = client.query(_query).to_dataframe()
    _df["first_seen_date"] = _df["first_seen_date"].astype(str)

    _chart_t = alt.Chart(_df).mark_line(color="steelblue").encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("total_pings_pc7ma:Q", title="Total Pings (7d MA)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), alt.Tooltip("total_pings_pc7ma:Q", title="Total Pings", format=",.0f")],
    ).properties(title="Total Pings (7d MA) — Organic", width=430, height=280)

    _chart_a = alt.Chart(_df).mark_line(color="darkorange").encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("avg_pings_client7ma:Q", title="Avg Pings per Client (7d MA)", scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), alt.Tooltip("avg_pings_client7ma:Q", title="Avg Pings", format=".2f")],
    ).properties(title="Avg Pings per Client (7d MA) — Organic", width=430, height=280)

    mo.ui.altair_chart(alt.hconcat(_chart_t, _chart_a))
    return


@app.cell
def _(alt, client, mo):
    _query = """
    DECLARE start_date_2026 DATE DEFAULT DATE(2026, 1, 18);
    WITH np AS (
        SELECT first_seen_date, client_id
        FROM fenix.new_profile_clients n
        WHERE first_seen_date >= start_date_2026 AND normalized_channel = 'release'
            AND country != 'IR' AND COALESCE(locale, '') NOT LIKE 'fa%'
            AND NOT (install_source IS NULL OR install_source = '')
            AND paid_vs_organic_gclid = 'Organic' AND is_mobile
    ),
    dous AS (
        SELECT np.first_seen_date, np.client_id,
            COUNTIF(ping_info.reason = 'active') AS n_pings_active,
            COUNTIF(ping_info.reason = 'inactive') AS n_pings_inactive,
            COUNTIF(ping_info.reason = 'dirty_startup') AS n_pings_ds,
            COALESCE(COUNT(a.document_id), 0) AS pings
        FROM np LEFT JOIN fenix.baseline a ON np.client_id = a.client_info.client_id
            AND date(a.submission_timestamp) BETWEEN np.first_seen_date AND DATE_ADD(np.first_seen_date, INTERVAL 6 DAY)
        WHERE date(a.submission_timestamp) >= start_date_2026
        GROUP BY 1,2
    ),
    agg AS (
        SELECT first_seen_date,
            AVG(n_pings_active) AS avg_pings_active_client, AVG(n_pings_inactive) AS avg_pings_inactive_client,
            AVG(n_pings_ds) AS avg_pings_ds_client, AVG(pings) AS avg_pings_client
        FROM dous WHERE first_seen_date <= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
        GROUP BY 1 ORDER BY 1
    )
    SELECT *,
        AVG(avg_pings_active_client) OVER (ORDER BY first_seen_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_pings_active_client7ma,
        AVG(avg_pings_inactive_client) OVER (ORDER BY first_seen_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_pings_inactive_client7ma,
        AVG(avg_pings_ds_client) OVER (ORDER BY first_seen_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_pings_ds_client7ma,
        AVG(avg_pings_client) OVER (ORDER BY first_seen_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_pings_client7ma
    FROM agg WHERE first_seen_date <= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY) ORDER BY 1
    """
    _df = client.query(_query).to_dataframe()
    _df["first_seen_date"] = _df["first_seen_date"].astype(str)
    _long = _df.melt(
        id_vars="first_seen_date",
        value_vars=["avg_pings_active_client7ma", "avg_pings_inactive_client7ma", "avg_pings_ds_client7ma"],
        var_name="series",
        value_name="avg_pings",
    ).replace({
        "avg_pings_active_client7ma": "Active",
        "avg_pings_inactive_client7ma": "Inactive",
        "avg_pings_ds_client7ma": "Dirty Startup",
    })
    _chart = alt.Chart(_long).mark_line().encode(
        x=alt.X("first_seen_date:T", title="Date"),
        y=alt.Y("avg_pings:Q", title="Avg Pings per Client (7d MA)", scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", legend=alt.Legend(title="Ping Type")),
        tooltip=[alt.Tooltip("first_seen_date:T", format="%b %d, %Y"), "series:N", alt.Tooltip("avg_pings:Q", format=".2f")],
    ).properties(title="Avg Pings per Client by Type (7d MA) — Organic", width="container", height=300)
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, client, mo):
    _query_fs = """
    DECLARE start_date_2026 DATE DEFAULT DATE(2026, 1, 18);
    SELECT date(submission_timestamp) AS submission_date,
        10 * COUNT(DISTINCT client_info.client_id) AS clients
    FROM fenix.first_session
    WHERE date(submission_timestamp) >= start_date_2026 AND sample_id < 10
        AND metadata.geo.country != 'IR' AND COALESCE(client_info.locale, '') NOT LIKE 'fa%'
    GROUP BY 1 ORDER BY 1
    """
    _query_fb = """
    DECLARE start_date_2026 DATE DEFAULT DATE(2026, 1, 18);
    SELECT date(submission_timestamp) AS submission_date,
        10 * COUNT(DISTINCT client_info.client_id) AS clients
    FROM fenix.baseline
    WHERE date(submission_timestamp) >= start_date_2026 AND ping_info.seq = 1 AND sample_id < 10
        AND metadata.geo.country != 'IR' AND COALESCE(client_info.locale, '') NOT LIKE 'fa%'
    GROUP BY 1 ORDER BY 1
    """
    _df_fs = client.query(_query_fs).to_dataframe()
    _df_fs["submission_date"] = _df_fs["submission_date"].astype(str)
    _df_fb = client.query(_query_fb).to_dataframe()
    _df_fb["submission_date"] = _df_fb["submission_date"].astype(str)

    _chart_fs = alt.Chart(_df_fs).mark_line(color="steelblue").encode(
        x=alt.X("submission_date:T", title="Date"),
        y=alt.Y("clients:Q", title="Clients", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("submission_date:T", format="%b %d, %Y"), alt.Tooltip("clients:Q", format=",.0f")],
    ).properties(title="First Session Ping Clients", width=430, height=280)

    _chart_fb = alt.Chart(_df_fb).mark_line(color="darkorange").encode(
        x=alt.X("submission_date:T", title="Date"),
        y=alt.Y("clients:Q", title="Clients", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("submission_date:T", format="%b %d, %Y"), alt.Tooltip("clients:Q", format=",.0f")],
    ).properties(title="First Baseline Ping Clients", width=430, height=280)

    mo.ui.altair_chart(alt.hconcat(_chart_fs, _chart_fb))
    return


@app.cell
def _(mo):
    mo.md("""
    ## Section 11: Other Signals
    DAU from users that aren't set as default browser is going down, but this is probably an artifact of re-enabling the native set-to-default prompt (per Vasant).
    """)
    return


@app.cell
def _(alt, client, mo):
    _query = """
    DECLARE start_date_2026 DATE DEFAULT DATE(2026, 1, 18);
    WITH dous AS (
        SELECT DATE(submission_timestamp) AS submission_date, client_info.client_id,
            COALESCE(LOGICAL_OR(metrics.boolean.metrics_default_browser), FALSE) AS is_default,
            COUNTIF(COALESCE(mozfun.glean.timespan_seconds(metrics.timespan.glean_baseline_duration), 0) > 0) AS n_pings_duration
        FROM fenix.baseline a
        WHERE normalized_channel = 'release' AND metadata.geo.country != 'IR'
            AND COALESCE(client_info.locale, '') NOT LIKE 'fa%'
            AND date(a.submission_timestamp) >= start_date_2026 AND sample_id < 10
        GROUP BY 1,2
    ),
    agg AS (
        SELECT submission_date,
            10 * COUNTIF(n_pings_duration > 0 AND is_default) AS dau_default,
            10 * COUNTIF(n_pings_duration > 0 AND NOT is_default) AS dau_not_default
        FROM dous GROUP BY 1
    )
    SELECT submission_date, dau_default, dau_not_default,
        AVG(dau_default) OVER (ORDER BY submission_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS dau_default_7ma,
        AVG(dau_not_default) OVER (ORDER BY submission_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS dau_not_default_7ma
    FROM agg ORDER BY 1
    """
    _df = client.query(_query).to_dataframe()
    _df["submission_date"] = _df["submission_date"].astype(str)
    _long = _df.melt(
        id_vars="submission_date",
        value_vars=["dau_default_7ma", "dau_not_default_7ma"],
        var_name="series",
        value_name="dau",
    ).replace({"dau_default_7ma": "Default Browser (7d MA)", "dau_not_default_7ma": "Not Default Browser (7d MA)"})
    _hover = alt.selection_point(fields=["submission_date"], nearest=True, on="pointerover", empty=False)
    _base = alt.Chart(_long).encode(
        x=alt.X("submission_date:T", title="Date"),
        color=alt.Color("series:N", legend=alt.Legend(title=None, orient="bottom")),
    )
    _lines = _base.mark_line().encode(
        y=alt.Y("dau:Q", title="DAU (7d MA)", axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
    )
    _points = _base.mark_point(size=60, filled=True).encode(
        y="dau:Q",
        opacity=alt.condition(_hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("submission_date:T", title="Date", format="%b %d, %Y"), "series:N", alt.Tooltip("dau:Q", format=",.0f")],
    ).add_params(_hover)
    _rule = alt.Chart(_long).mark_rule(color="gray", strokeWidth=1).encode(x="submission_date:T").transform_filter(_hover)
    mo.ui.altair_chart((_lines + _rule + _points).properties(title="DAU by Default Browser Status (7d MA)", width="container", height=300))
    return


if __name__ == "__main__":
    app.run()
