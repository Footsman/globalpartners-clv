"""
Global Partners — Business Insights Dashboards
==============================================

Six Streamlit dashboards over the medallion lake in S3, queried through Athena.

  Gold  (fact_customer_clv_daily)  -> segmentation, churn, loyalty
  Silver(fact_order_line)          -> trends, location, discounts

Run:
    pip install -r requirements.txt
    streamlit run app.py

Auth: the [globalpartners] profile in ~/.aws/credentials, which maps to the
read-only IAM user `streamlit-dashboard-reader`.
"""

from __future__ import annotations

import boto3
import awswrangler as wr
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

AWS_PROFILE = "globalpartners"
AWS_REGION = "us-east-1"
DATABASE = "globalpartners"
WORKGROUP = "primary"

# The gold crawler points at s3://.../gold/, so Glue named the table after the
# folder. Change to "fact_customer_clv_daily" if you repoint the crawler at
# s3://global-partners-lake-nh/gold/fact_customer_clv_daily/ and re-run it.
GOLD = "gold"
FACT = "fact_order_line"

CACHE_TTL = 3600  # seconds

# --------------------------------------------------------------------------
# Palette — validated categorical order, do not cycle or re-order
# --------------------------------------------------------------------------

SERIES = [
    "#2a78d6",  # 1 blue
    "#eb6834",  # 2 orange
    "#1baf7a",  # 3 aqua
    "#eda100",  # 4 yellow
    "#e87ba4",  # 5 magenta
    "#008300",  # 6 green
    "#6250d6",  # 7 violet
    "#e34948",  # 8 red
]

# Ordinal blue ramp for ordered categories (tiers, buckets, deciles).
# Light surface: start no lighter than step 250.
BLUE = {
    250: "#86b6ef",
    350: "#5598e7",
    450: "#2a78d6",
    550: "#1c5cab",
    650: "#104281",
}
ORDINAL_3 = [BLUE[250], BLUE[450], BLUE[650]]

STATUS = {
    "good": "#0ca30c",
    "warning": "#fab219",
    "serious": "#ec835a",
    "critical": "#d03b3b",
}

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"

FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'

TIER_ORDER = ["Low", "Medium", "High"]
SEGMENT_ORDER = ["VIP", "Loyal", "New", "At Risk", "Churn Risk", "Dormant"]

# --------------------------------------------------------------------------
# Athena
# --------------------------------------------------------------------------


@st.cache_resource
def _session() -> boto3.Session:
    return boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


@st.cache_data(ttl=CACHE_TTL, show_spinner="Querying Athena…")
def q(sql: str) -> pd.DataFrame:
    """Run a query and return a DataFrame.

    ctas_approach=False matters: the default writes a temporary CTAS table,
    which needs glue:CreateTable and S3 write outside athena-results/. The
    dashboard user has neither, by design.
    """
    return wr.athena.read_sql_query(
        sql,
        database=DATABASE,
        workgroup=WORKGROUP,
        ctas_approach=False,
        boto3_session=_session(),
    )


@st.cache_data(ttl=CACHE_TTL)
def latest_snapshot() -> str:
    return str(q(f"SELECT MAX(snapshot_date) AS d FROM {GOLD}").iloc[0]["d"])


# --------------------------------------------------------------------------
# Chart helpers
# --------------------------------------------------------------------------


def _layout(fig: go.Figure, *, height: int = 360, legend: bool = False,
            xtitle: str = "", ytitle: str = "") -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font=dict(family=FONT, size=13, color=INK_2),
        showlegend=legend,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0,
                    font=dict(color=INK_2), bgcolor="rgba(0,0,0,0)"),
        hoverlabel=dict(font_family=FONT, font_size=13),
        bargap=0.3,
    )
    fig.update_xaxes(title_text=xtitle, title_font=dict(color=MUTED, size=12),
                     tickfont=dict(color=MUTED, size=12),
                     showgrid=False, zeroline=False,
                     linecolor=AXIS, linewidth=1)
    fig.update_yaxes(title_text=ytitle, title_font=dict(color=MUTED, size=12),
                     tickfont=dict(color=MUTED, size=12),
                     gridcolor=GRID, gridwidth=1, zeroline=False,
                     linecolor="rgba(0,0,0,0)")
    return fig


def bar(df: pd.DataFrame, x: str, y: str, *, color: str = SERIES[0],
        horizontal: bool = False, fmt: str = "{:,.0f}", height: int = 360,
        xtitle: str = "", ytitle: str = "") -> go.Figure:
    """Single-series bar with direct value labels (the contrast relief rule)."""
    labels = [fmt.format(v) for v in df[y]]
    if horizontal:
        fig = go.Figure(go.Bar(
            x=df[y], y=df[x], orientation="h", marker_color=color,
            text=labels, textposition="outside",
            textfont=dict(color=INK_2, size=12),
            hovertemplate="%{y}<br>%{x:,.2f}<extra></extra>",
        ))
        fig.update_traces(marker_line_width=0)
        fig = _layout(fig, height=height, xtitle=ytitle, ytitle=xtitle)
        fig.update_xaxes(showgrid=True, gridcolor=GRID)
        fig.update_yaxes(showgrid=False)
    else:
        fig = go.Figure(go.Bar(
            x=df[x], y=df[y], marker_color=color,
            text=labels, textposition="outside",
            textfont=dict(color=INK_2, size=12),
            hovertemplate="%{x}<br>%{y:,.2f}<extra></extra>",
        ))
        fig.update_traces(marker_line_width=0)
        fig = _layout(fig, height=height, xtitle=xtitle, ytitle=ytitle)
    return fig


def line(df: pd.DataFrame, x: str, series: dict[str, str], *,
         height: int = 360, xtitle: str = "", ytitle: str = "") -> go.Figure:
    """One line per entry in `series` -> {column: label}. Colors by fixed slot."""
    fig = go.Figure()
    for i, (col, label) in enumerate(series.items()):
        fig.add_trace(go.Scatter(
            x=df[x], y=df[col], mode="lines", name=label,
            line=dict(color=SERIES[i % len(SERIES)], width=2),
            hovertemplate="%{x}<br>" + label + ": %{y:,.2f}<extra></extra>",
        ))
    fig.update_layout(hovermode="x unified")
    return _layout(fig, height=height, legend=len(series) > 1,
                   xtitle=xtitle, ytitle=ytitle)


def stat(col, label: str, value: str, sub: str = "") -> None:
    col.markdown(
        f"<div style='padding:14px 16px;border:1px solid rgba(11,11,11,.10);"
        f"border-radius:10px;background:{SURFACE}'>"
        f"<div style='color:{MUTED};font-size:12px;letter-spacing:.02em;"
        f"text-transform:uppercase'>{label}</div>"
        f"<div style='color:{INK};font-size:28px;font-weight:600;"
        f"line-height:1.25;margin-top:2px'>{value}</div>"
        f"<div style='color:{INK_2};font-size:12px'>{sub}</div></div>",
        unsafe_allow_html=True,
    )


def money(v: float) -> str:
    return f"${v:,.0f}"


# --------------------------------------------------------------------------
# 1. Customer Segmentation
# --------------------------------------------------------------------------


def page_segmentation(snap: str) -> None:
    st.subheader("Customer Segmentation")
    st.caption(f"RFM segments and CLV tiers as at snapshot {snap}.")

    df = q(f"""
        SELECT rfm_segment,
               clv_tier,
               COUNT(*)                        AS customers,
               CAST(SUM(clv_historic) AS double)   AS total_clv,
               CAST(AVG(clv_historic) AS double)   AS avg_clv,
               CAST(AVG(orders_to_date) AS double) AS avg_orders,
               CAST(AVG(avg_order_value) AS double) AS avg_aov
        FROM {GOLD}
        WHERE snapshot_date = '{snap}'
        GROUP BY rfm_segment, clv_tier
    """)

    total_cust = int(df["customers"].sum())
    total_clv = float(df["total_clv"].sum())

    c1, c2, c3, c4 = st.columns(4)
    stat(c1, "Customers", f"{total_cust:,}")
    stat(c2, "Total CLV", money(total_clv))
    stat(c3, "Average CLV", f"${total_clv / total_cust:,.2f}")
    vip = df.loc[df["rfm_segment"] == "VIP", "total_clv"].sum()
    stat(c4, "VIP share of CLV", f"{vip / total_clv:.1%}",
         f"{int(df.loc[df['rfm_segment'] == 'VIP', 'customers'].sum()):,} customers")

    st.write("")
    left, right = st.columns(2)

    by_seg = (df.groupby("rfm_segment", as_index=False)
                .agg(customers=("customers", "sum"), total_clv=("total_clv", "sum")))
    by_seg["o"] = by_seg["rfm_segment"].apply(
        lambda s: SEGMENT_ORDER.index(s) if s in SEGMENT_ORDER else 99)
    by_seg = by_seg.sort_values("o")

    with left:
        st.markdown("**Customers by segment**")
        st.plotly_chart(bar(by_seg, "rfm_segment", "customers"),
                        use_container_width=True)
    with right:
        st.markdown("**Lifetime value by segment**")
        st.plotly_chart(
            bar(by_seg, "rfm_segment", "total_clv", color=SERIES[1],
                fmt="${:,.0f}"),
            use_container_width=True)

    st.markdown("**CLV tier composition within each segment**")
    st.caption("Tiers are percentile bands recomputed every snapshot, so a "
               "customer can move between them over time.")
    pivot = (df.pivot_table(index="rfm_segment", columns="clv_tier",
                            values="customers", aggfunc="sum", fill_value=0)
               .reindex(columns=TIER_ORDER, fill_value=0)
               .reindex([s for s in SEGMENT_ORDER
                         if s in df["rfm_segment"].values]))
    fig = go.Figure()
    for i, tier in enumerate(TIER_ORDER):
        fig.add_trace(go.Bar(
            x=pivot.index, y=pivot[tier], name=tier,
            marker_color=ORDINAL_3[i], marker_line_width=2,
            marker_line_color=SURFACE,
            hovertemplate="%{x} · " + tier + "<br>%{y:,} customers<extra></extra>",
        ))
    fig.update_layout(barmode="stack")
    st.plotly_chart(_layout(fig, legend=True, ytitle="customers"),
                    use_container_width=True)

    with st.expander("Segment detail"):
        tbl = (df.groupby("rfm_segment", as_index=False)
                 .agg(customers=("customers", "sum"),
                      total_clv=("total_clv", "sum"),
                      avg_clv=("avg_clv", "mean"),
                      avg_orders=("avg_orders", "mean"),
                      avg_aov=("avg_aov", "mean")))
        st.dataframe(tbl, use_container_width=True, hide_index=True)


# --------------------------------------------------------------------------
# 2. Churn Risk
# --------------------------------------------------------------------------


def page_churn(snap: str) -> None:
    st.subheader("Churn Risk")
    st.caption("A customer is flagged at risk when their most recent order is "
               "more than 45 days before the snapshot — the threshold set in "
               "the assignment.")

    overall = q(f"""
        SELECT churn_risk_flag,
               clv_tier,
               COUNT(*)                       AS customers,
               CAST(SUM(clv_historic) AS double) AS clv
        FROM {GOLD}
        WHERE snapshot_date = '{snap}'
        GROUP BY churn_risk_flag, clv_tier
    """)

    at_risk = overall[overall["churn_risk_flag"]]
    total_cust = int(overall["customers"].sum())
    total_clv = float(overall["clv"].sum())
    risk_cust = int(at_risk["customers"].sum())
    risk_clv = float(at_risk["clv"].sum())

    c1, c2, c3, c4 = st.columns(4)
    stat(c1, "At risk", f"{risk_cust:,}", f"of {total_cust:,} customers")
    stat(c2, "Share of base", f"{risk_cust / total_cust:.1%}")
    stat(c3, "CLV at risk", money(risk_clv))
    stat(c4, "Share of CLV", f"{risk_clv / total_clv:.1%}")

    st.write("")
    left, right = st.columns(2)

    with left:
        st.markdown("**Churn rate by CLV tier**")
        rate = (overall.pivot_table(index="clv_tier", columns="churn_risk_flag",
                                    values="customers", aggfunc="sum",
                                    fill_value=0)
                       .reindex(TIER_ORDER))
        rate.columns = [str(c) for c in rate.columns]
        rate["rate"] = rate.get("True", 0) / rate.sum(axis=1) * 100
        rate = rate.reset_index()
        st.plotly_chart(
            bar(rate, "clv_tier", "rate", color=STATUS["critical"],
                fmt="{:.1f}%", ytitle="% at risk"),
            use_container_width=True)
        st.caption("High-tier churn is the expensive kind — each departure "
                   "carries far more lifetime value than a Low-tier one.")

    with right:
        st.markdown("**Days since last order**")
        buckets = q(f"""
            SELECT CASE
                     WHEN recency_days <= 7   THEN '0–7'
                     WHEN recency_days <= 30  THEN '8–30'
                     WHEN recency_days <= 45  THEN '31–45'
                     WHEN recency_days <= 90  THEN '46–90'
                     WHEN recency_days <= 180 THEN '91–180'
                     ELSE '180+'
                   END AS bucket,
                   COUNT(*) AS customers
            FROM {GOLD}
            WHERE snapshot_date = '{snap}'
            GROUP BY 1
        """)
        order = ["0–7", "8–30", "31–45", "46–90", "91–180", "180+"]
        buckets["o"] = buckets["bucket"].apply(order.index)
        buckets = buckets.sort_values("o")
        ramp = [BLUE[250], BLUE[250], BLUE[350],
                BLUE[450], BLUE[550], BLUE[650]]
        fig = go.Figure(go.Bar(
            x=buckets["bucket"], y=buckets["customers"],
            marker_color=ramp, marker_line_width=0,
            text=[f"{v:,}" for v in buckets["customers"]],
            textposition="outside", textfont=dict(color=INK_2, size=12),
            hovertemplate="%{x} days<br>%{y:,} customers<extra></extra>",
        ))
        st.plotly_chart(_layout(fig, ytitle="customers", xtitle="days"),
                        use_container_width=True)
        st.caption("The 45-day line sits between the third and fourth bars.")

    st.markdown("**Highest-value customers at risk**")
    top = q(f"""
        SELECT user_id,
               CAST(clv_historic AS double)   AS clv,
               orders_to_date,
               recency_days,
               last_order_date,
               rfm_segment,
               CAST(spend_trend_pct AS double) AS spend_trend_pct
        FROM {GOLD}
        WHERE snapshot_date = '{snap}'
          AND churn_risk_flag
          AND clv_tier = 'High'
        ORDER BY clv_historic DESC
        LIMIT 100
    """)
    st.dataframe(top, use_container_width=True, hide_index=True)
    st.caption("This is the retention call list: highest lifetime value, "
               "already past the 45-day mark.")


# --------------------------------------------------------------------------
# 3. Sales Trends & Seasonality
# --------------------------------------------------------------------------


def page_trends() -> None:
    st.subheader("Sales Trends & Seasonality")
    st.caption("Order-line revenue from the silver fact table, across the  "
               "history.")

    monthly = q(f"""
        SELECT date_format(date_trunc('month', CAST(order_date AS date)),
                           '%Y-%m')                     AS month,
               CAST(SUM(line_revenue) AS double)        AS revenue,
               COUNT(DISTINCT order_id)                 AS orders,
               COUNT(DISTINCT user_id)                  AS customers
        FROM {FACT}
        GROUP BY 1
        ORDER BY 1
    """)
    monthly["aov"] = monthly["revenue"] / monthly["orders"]

    c1, c2, c3, c4 = st.columns(4)
    stat(c1, "Revenue", money(monthly["revenue"].sum()))
    stat(c2, "Orders", f"{int(monthly['orders'].sum()):,}")
    stat(c3, "Average order", f"${monthly['revenue'].sum() / monthly['orders'].sum():,.2f}")
    stat(c4, "Months covered", f"{len(monthly)}")

    st.write("")
    st.markdown("**Revenue by month**")
    st.plotly_chart(line(monthly, "month", {"revenue": "Revenue"},
                         ytitle="revenue ($)"),
                    use_container_width=True)

    st.markdown("**Average order value by month**")
    st.caption("Kept on its own chart rather than sharing an axis with revenue "
               "— two measures on one y-scale invent a relationship that isn't "
               "in the data.")
    st.plotly_chart(line(monthly, "month", {"aov": "Average order value"},
                         ytitle="$ per order", height=260),
                    use_container_width=True)

    left, right = st.columns(2)

    with left:
        st.markdown("**By day of week**")
        dow = q(f"""
            SELECT day_of_week(CAST(order_date AS date))  AS dow,
                   CAST(SUM(line_revenue) AS double)      AS revenue,
                   COUNT(DISTINCT order_id)               AS orders
            FROM {FACT}
            GROUP BY 1
            ORDER BY 1
        """)
        names = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu",
                 5: "Fri", 6: "Sat", 7: "Sun"}
        dow["day"] = dow["dow"].map(names)
        st.plotly_chart(
            bar(dow, "day", "revenue", fmt="${:,.0f}", ytitle="revenue ($)"),
            use_container_width=True)

    with right:
        st.markdown("**By hour of day (UTC)**")
        hourly = q(f"""
            SELECT hour(order_ts_utc)                AS hr,
                   CAST(SUM(line_revenue) AS double) AS revenue
            FROM {FACT}
            GROUP BY 1
            ORDER BY 1
        """)
        fig = go.Figure(go.Bar(
            x=hourly["hr"], y=hourly["revenue"],
            marker_color=SERIES[0], marker_line_width=0,
            hovertemplate="%{x}:00<br>$%{y:,.0f}<extra></extra>",
        ))
        st.plotly_chart(_layout(fig, xtitle="hour (UTC)", ytitle="revenue ($)"),
                        use_container_width=True)
        st.caption("Timestamps are UTC, so the trading-day shape is shifted "
                   "from local time.")

    st.markdown("**Top categories over time**")
    cats = q(f"""
        WITH top AS (
            SELECT item_category
            FROM {FACT}
            WHERE item_category IS NOT NULL
            GROUP BY item_category
            ORDER BY SUM(line_revenue) DESC
            LIMIT 5
        )
        SELECT date_format(date_trunc('month', CAST(f.order_date AS date)),
                           '%Y-%m')                  AS month,
               f.item_category                       AS category,
               CAST(SUM(f.line_revenue) AS double)   AS revenue
        FROM {FACT} f
        JOIN top t ON f.item_category = t.item_category
        GROUP BY 1, 2
        ORDER BY 1
    """)
    wide = cats.pivot_table(index="month", columns="category",
                            values="revenue", fill_value=0).reset_index()
    cols = [c for c in wide.columns if c != "month"]
    st.plotly_chart(line(wide, "month", {c: c for c in cols},
                         ytitle="revenue ($)", height=400),
                    use_container_width=True)


# --------------------------------------------------------------------------
# 4. Loyalty Impact
# --------------------------------------------------------------------------


def page_loyalty(snap: str) -> None:
    st.subheader("Loyalty Impact")
    st.caption(f"Loyalty members against everyone else, at snapshot {snap}.")

    df = q(f"""
        SELECT is_loyalty,
               COUNT(*)                                 AS customers,
               CAST(SUM(clv_historic) AS double)        AS total_clv,
               CAST(AVG(clv_historic) AS double)        AS avg_clv,
               CAST(AVG(orders_to_date) AS double)      AS avg_orders,
               CAST(AVG(avg_order_value) AS double)     AS avg_aov,
               CAST(AVG(recency_days) AS double)        AS avg_recency,
               CAST(AVG(CASE WHEN churn_risk_flag THEN 1.0 ELSE 0.0 END)
                    AS double) * 100                    AS churn_rate
        FROM {GOLD}
        WHERE snapshot_date = '{snap}'
        GROUP BY is_loyalty
    """)
    df["cohort"] = df["is_loyalty"].map({True: "Loyalty", False: "Non-loyalty"})
    df = df.sort_values("is_loyalty", ascending=False)

    mem = df[df["is_loyalty"]].iloc[0] if df["is_loyalty"].any() else None
    non = df[~df["is_loyalty"]].iloc[0] if (~df["is_loyalty"]).any() else None

    c1, c2, c3, c4 = st.columns(4)
    if mem is not None and non is not None:
        stat(c1, "Loyalty members", f"{int(mem['customers']):,}",
             f"{mem['customers'] / df['customers'].sum():.1%} of base")
        stat(c2, "Avg CLV — member", f"${mem['avg_clv']:,.2f}")
        stat(c3, "Avg CLV — non-member", f"${non['avg_clv']:,.2f}")
        stat(c4, "Difference", f"{mem['avg_clv'] / non['avg_clv'] - 1:+.0%}")

    st.write("")
    st.markdown("**Member versus non-member**")
    st.caption("Four separate panels rather than one grouped chart — the "
               "measures have different units and ranges, and forcing them "
               "onto a shared scale would hide the smaller ones.")

    panels = [
        ("avg_clv", "Average CLV ($)", "${:,.0f}", SERIES[0]),
        ("avg_orders", "Average orders", "{:,.1f}", SERIES[1]),
        ("avg_aov", "Average order value ($)", "${:,.2f}", SERIES[2]),
        ("churn_rate", "Churn risk (%)", "{:,.1f}%", STATUS["critical"]),
    ]
    cols = st.columns(4)
    for col, (field, title, fmt, color) in zip(cols, panels):
        with col:
            st.markdown(f"<div style='color:{INK_2};font-size:13px'>{title}</div>",
                        unsafe_allow_html=True)
            st.plotly_chart(
                bar(df, "cohort", field, color=color, fmt=fmt, height=260),
                use_container_width=True)

    st.markdown("**Where each cohort sits in the value distribution**")
    deciles = q(f"""
        SELECT CAST(LEAST(CAST(FLOOR(clv_pct_rank * 10) AS integer), 9)
                    AS integer)              AS decile,
               is_loyalty,
               COUNT(*)                      AS customers
        FROM {GOLD}
        WHERE snapshot_date = '{snap}'
        GROUP BY 1, 2
        ORDER BY 1
    """)
    piv = deciles.pivot_table(index="decile", columns="is_loyalty",
                              values="customers", fill_value=0)
    fig = go.Figure()
    for i, flag in enumerate([True, False]):
        if flag in piv.columns:
            fig.add_trace(go.Bar(
                x=[f"D{d + 1}" for d in piv.index], y=piv[flag],
                name="Loyalty" if flag else "Non-loyalty",
                marker_color=SERIES[i], marker_line_width=2,
                marker_line_color=SURFACE,
                hovertemplate="%{x}<br>%{y:,} customers<extra></extra>",
            ))
    fig.update_layout(barmode="group")
    st.plotly_chart(_layout(fig, legend=True, xtitle="CLV decile (D10 = top)",
                            ytitle="customers"),
                    use_container_width=True)
    st.caption("Read this as association, not effect. Members self-select — "
               "frequent customers are the ones who sign up — so the gap "
               "measures who joins as much as what joining does. Separating "
               "the two needs a before-and-after on enrolment date.")

    with st.expander("Cohort detail"):
        st.dataframe(df.drop(columns=["is_loyalty"]),
                     use_container_width=True, hide_index=True)


# --------------------------------------------------------------------------
# 5. Location Performance
# --------------------------------------------------------------------------


def page_location() -> None:
    st.subheader("Location Performance")
    st.caption("Every restaurant in the silver fact table,  history.")

    df = q(f"""
        SELECT restaurant_id,
               CAST(SUM(line_revenue) AS double)  AS revenue,
               COUNT(DISTINCT order_id)           AS orders,
               COUNT(DISTINCT user_id)            AS customers,
               COUNT(*)                           AS lines
        FROM {FACT}
        GROUP BY restaurant_id
        ORDER BY revenue DESC
    """)
    df["aov"] = df["revenue"] / df["orders"]
    df["lines_per_order"] = df["lines"] / df["orders"]
    df["restaurant"] = df["restaurant_id"].astype(str)

    c1, c2, c3, c4 = st.columns(4)
    stat(c1, "Locations", f"{len(df):,}")
    stat(c2, "Revenue", money(df["revenue"].sum()))
    top5 = df.head(5)["revenue"].sum() / df["revenue"].sum()
    stat(c3, "Top 5 share", f"{top5:.1%}")
    stat(c4, "Median location", money(df["revenue"].median()))

    st.write("")
    st.markdown("**Revenue by location**")
    plot = df.sort_values("revenue")
    st.plotly_chart(
        bar(plot, "restaurant", "revenue", horizontal=True, fmt="${:,.0f}",
            height=max(360, 22 * len(plot)), xtitle="restaurant",
            ytitle="revenue ($)"),
        use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.markdown("**Order volume against basket size**")
        fig = go.Figure(go.Scatter(
            x=df["orders"], y=df["aov"], mode="markers",
            marker=dict(size=10, color=SERIES[0],
                        line=dict(width=2, color=SURFACE)),
            text=df["restaurant"],
            hovertemplate="Restaurant %{text}<br>%{x:,} orders"
                          "<br>$%{y:,.2f} per order<extra></extra>",
        ))
        st.plotly_chart(
            _layout(fig, xtitle="orders", ytitle="average order value ($)"),
            use_container_width=True)
        st.caption("A single colour: with 27 points, hues would be decoration "
                   "rather than information.")

    with right:
        st.markdown("**Average order value by location**")
        st.plotly_chart(
            bar(df.sort_values("aov", ascending=False).head(12),
                "restaurant", "aov", color=SERIES[1], fmt="${:,.2f}",
                xtitle="restaurant", ytitle="$ per order"),
            use_container_width=True)
        st.caption("Top 12 shown.")

    st.markdown("**Monthly revenue — five largest locations**")
    top_ids = ", ".join(f"'{r}'" for r in df.head(5)["restaurant_id"])
    trend = q(f"""
        SELECT date_format(date_trunc('month', CAST(order_date AS date)),
                           '%Y-%m')                 AS month,
               CAST(restaurant_id AS varchar)       AS restaurant,
               CAST(SUM(line_revenue) AS double)    AS revenue
        FROM {FACT}
        WHERE CAST(restaurant_id AS varchar) IN ({top_ids})
        GROUP BY 1, 2
        ORDER BY 1
    """)
    wide = trend.pivot_table(index="month", columns="restaurant",
                             values="revenue", fill_value=0).reset_index()
    cols = [c for c in wide.columns if c != "month"]
    st.plotly_chart(line(wide, "month", {c: f"Restaurant {c}" for c in cols},
                         ytitle="revenue ($)", height=400),
                    use_container_width=True)

    with st.expander("All locations"):
        st.dataframe(df.drop(columns=["restaurant"]),
                     use_container_width=True, hide_index=True)


# --------------------------------------------------------------------------
# 6. Discount Effectiveness
# --------------------------------------------------------------------------


def page_discounts() -> None:
    st.subheader("Discount Effectiveness")
    st.caption("The source has no discount column. A line counts as discounted "
               "when its unit price falls below the item's modal price — the "
               "price that item sells at most often on single-quantity lines.")

    overall = q(f"""
        SELECT is_discounted,
               COUNT(*)                            AS lines,
               CAST(SUM(line_revenue) AS double)   AS revenue,
               CAST(SUM(discount_amount) AS double) AS discount,
               COUNT(DISTINCT order_id)            AS orders
        FROM {FACT}
        GROUP BY is_discounted
    """)
    disc = overall[overall["is_discounted"]]
    total_rev = float(overall["revenue"].sum())
    total_disc = float(overall["discount"].sum())
    disc_lines = int(disc["lines"].sum()) if len(disc) else 0

    c1, c2, c3, c4 = st.columns(4)
    stat(c1, "Discounted lines", f"{disc_lines:,}",
         f"{disc_lines / overall['lines'].sum():.1%} of all lines")
    stat(c2, "Total discount given", money(total_disc))
    stat(c3, "Discount as % of revenue", f"{total_disc / total_rev:.2%}")
    stat(c4, "Revenue on discounted lines",
         money(float(disc["revenue"].sum()) if len(disc) else 0))

    st.write("")
    st.markdown("**Do discounted orders buy more?**")
    baskets = q(f"""
        WITH o AS (
            SELECT order_id,
                   MAX(CASE WHEN is_discounted THEN 1 ELSE 0 END) AS has_discount,
                   CAST(SUM(line_revenue) AS double)              AS order_revenue,
                   COUNT(*)                                       AS lines
            FROM {FACT}
            GROUP BY order_id
        )
        SELECT CASE WHEN has_discount = 1 THEN 'With discount'
                    ELSE 'No discount' END       AS cohort,
               COUNT(*)                          AS orders,
               CAST(AVG(order_revenue) AS double) AS avg_order_value,
               CAST(AVG(lines) AS double)         AS avg_lines
        FROM o
        GROUP BY 1
    """)
    baskets = baskets.sort_values("cohort", ascending=False)

    p1, p2, p3 = st.columns(3)
    with p1:
        st.markdown(f"<div style='color:{INK_2};font-size:13px'>Orders</div>",
                    unsafe_allow_html=True)
        st.plotly_chart(bar(baskets, "cohort", "orders", height=280),
                        use_container_width=True)
    with p2:
        st.markdown(f"<div style='color:{INK_2};font-size:13px'>"
                    f"Average order value ($)</div>", unsafe_allow_html=True)
        st.plotly_chart(bar(baskets, "cohort", "avg_order_value",
                            color=SERIES[1], fmt="${:,.2f}", height=280),
                        use_container_width=True)
    with p3:
        st.markdown(f"<div style='color:{INK_2};font-size:13px'>"
                    f"Lines per order</div>", unsafe_allow_html=True)
        st.plotly_chart(bar(baskets, "cohort", "avg_lines",
                            color=SERIES[2], fmt="{:,.2f}", height=280),
                        use_container_width=True)
    st.caption("A bigger basket on discounted orders is consistent with "
               "discounts driving volume — and equally consistent with "
               "discounts being applied to large orders in the first place. "
               "The data can't separate the two.")

    left, right = st.columns(2)
    with left:
        st.markdown("**Discount by category**")
        cat = q(f"""
            SELECT item_category,
                   CAST(SUM(discount_amount) AS double) AS discount,
                   CAST(SUM(line_revenue) AS double)    AS revenue
            FROM {FACT}
            WHERE item_category IS NOT NULL
            GROUP BY item_category
            ORDER BY discount DESC
            LIMIT 12
        """)
        st.plotly_chart(
            bar(cat.sort_values("discount"), "item_category", "discount",
                horizontal=True, fmt="${:,.0f}", color=SERIES[1],
                height=380, ytitle="discount ($)"),
            use_container_width=True)

    with right:
        st.markdown("**Discount rate over time**")
        trend = q(f"""
            SELECT date_format(date_trunc('month', CAST(order_date AS date)),
                               '%Y-%m')                        AS month,
                   CAST(SUM(discount_amount) AS double)        AS discount,
                   CAST(SUM(line_revenue) AS double)           AS revenue
            FROM {FACT}
            GROUP BY 1
            ORDER BY 1
        """)
        trend["rate"] = trend["discount"] / trend["revenue"] * 100
        st.plotly_chart(
            line(trend, "month", {"rate": "Discount as % of revenue"},
                 ytitle="%", height=380),
            use_container_width=True)

    with st.expander("Category detail"):
        st.dataframe(cat, use_container_width=True, hide_index=True)


# --------------------------------------------------------------------------
# Shell
# --------------------------------------------------------------------------


PAGES = {
    "Customer Segmentation": "gold",
    "Churn Risk": "gold",
    "Sales Trends & Seasonality": "silver",
    "Loyalty Impact": "gold",
    "Location Performance": "silver",
    "Discount Effectiveness": "silver",
}


def main() -> None:
    st.set_page_config(page_title="Global Partners — Business Insights",
                       page_icon="📊", layout="wide")

    st.sidebar.markdown("### Global Partners")
    st.sidebar.caption("Business Insights")
    choice = st.sidebar.radio("Dashboard", list(PAGES), label_visibility="collapsed")
    st.sidebar.divider()

    try:
        snap = latest_snapshot()
    except Exception as exc:  # surfaced rather than swallowed
        st.error("Could not reach Athena.")
        st.exception(exc)
        st.stop()
        return

    st.sidebar.caption(f"Snapshot **{snap}**")
    st.sidebar.caption(f"Source: `{PAGES[choice]}`")
    if st.sidebar.button("Clear cache", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.title("Global Partners — Business Insights")

    if choice == "Customer Segmentation":
        page_segmentation(snap)
    elif choice == "Churn Risk":
        page_churn(snap)
    elif choice == "Sales Trends & Seasonality":
        page_trends()
    elif choice == "Loyalty Impact":
        page_loyalty(snap)
    elif choice == "Location Performance":
        page_location()
    elif choice == "Discount Effectiveness":
        page_discounts()


if __name__ == "__main__":
    main()
