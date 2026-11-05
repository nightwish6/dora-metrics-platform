"""Streamlit Dashboard for Delivery Analytics."""

import base64
import os

import duckdb
import numpy as np
from dotenv import load_dotenv
from scipy import stats

import streamlit as st

# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------
load_dotenv()

st.set_page_config(page_title="Delivery Analytics", layout="wide", initial_sidebar_state="collapsed")

# Global font: Roboto
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;600;700&display=swap');
    * { font-family: 'Roboto', sans-serif !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = os.getenv("DUCKDB_PATH", "data/duckdb/data.duckdb")


# ---------------------------------------------------------------------------
# Data loading — load into Pandas DataFrames, then close connection
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def _load_all_data():
    """
    1. Open file in read-only mode
    2. Load all required tables into Pandas DataFrames
    3. Close file → lock released immediately
    4. Return all DataFrames (cached)
    """
    conn = duckdb.connect(DB_PATH, read_only=True)

    df = conn.execute(
        "SELECT repo_name, period, release_count FROM marts.core_deployment_frequency ORDER BY period ASC"
    ).fetchdf()
    lt_df = conn.execute(
        "SELECT repo_name, period, avg_lead_time_hours FROM marts.core_lead_time ORDER BY period ASC"
    ).fetchdf()
    cfr_df = conn.execute("SELECT repo_name, period, cfr_revert_rate FROM marts.core_cfr ORDER BY period ASC").fetchdf()
    mttr_df = conn.execute(
        "SELECT repo_name, period, avg_restore_days FROM marts.core_time_to_restore ORDER BY period ASC"
    ).fetchdf()
    avg_df = conn.execute(
        "SELECT repo_name, avg_release_count, avg_lead_time_hours, avg_cfr, avg_restore_days FROM marts.core_dora_metrics_avg"
    ).fetchdf()
    loaded_at = (
        conn.execute("SELECT MAX(loaded_at) as max_loaded_at FROM marts.core_dora_metrics_avg")
        .fetchdf()["max_loaded_at"]
        .iloc[0]
    )

    conn.close()  # file closed, lock released!

    return df, lt_df, cfr_df, mttr_df, avg_df, loaded_at


# Load data once on startup (cached for 1 hour)
df, lt_df, cfr_df, mttr_df, avg_df, loaded_at = _load_all_data()


# ---------------------------------------------------------------------------
# Helper — build chart data
# ---------------------------------------------------------------------------
def build_chart_df(metric_df, value_col):
    repo = metric_df["repo_name"].unique()[0]
    repo_df = metric_df[metric_df["repo_name"] == repo].copy()
    repo_df["period_str"] = repo_df["period"].dt.strftime("%Y-%m")
    repo_df.set_index("period_str", inplace=True)

    # Linear regression trend
    x = np.arange(len(repo_df))
    slope, intercept, _, _, _ = stats.linregress(x, repo_df[value_col])
    repo_df["trend"] = slope * x + intercept

    return repo_df, repo


# ---------------------------------------------------------------------------
# Page title and description
# ---------------------------------------------------------------------------
date_str = loaded_at.strftime("%Y-%m-%d %H:%M") if loaded_at else "N/A"
assets_path = os.path.join(os.path.dirname(__file__), "assets", "vscode-logo.png")
if os.path.exists(assets_path):
    vscode_icon = f'<img src="data:image/png;base64,{base64.b64encode(open(assets_path, "rb").read()).decode()}" width="32" height="32">'
else:
    vscode_icon = ""
st.markdown(
    f"""<div style="display:flex;justify-content:space-between;align-items:center;margin:0 0 10px 0;">
        <h1 style="font-size:30px;margin:0;padding:0;display:flex;align-items:center;gap:10px;">
            {vscode_icon} Delivery Analytics
        </h1>
        <span style="color:#999;font-size:14px;">Last updated: {date_str}</span>
    </div>""",
    unsafe_allow_html=True,
)
st.markdown(
    """
    Analysis of development performance for **Microsoft Visual Studio Code**.
    Displays DORA metrics (Deployment Frequency, Lead Time, Change Failure Rate,
    Time to Restore) alongside **Flow**, **Quality** and **Health** metrics.
    """
)

# ---------------------------------------------------------------------------
# 4 metrics in a row
# ---------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

# === Col 1: Deployment Frequency ===
with col1:
    repo_df, repo = build_chart_df(df, "release_count")
    avg_val = avg_df[avg_df["repo_name"] == repo]["avg_release_count"].iloc[0]

    col_desc, col_kpi = col1.columns([1, 1])
    with col_desc:
        st.markdown("**Deployment Frequency** — how often releases ship to production.\nOne of the key DORA metrics.")
    with col_kpi:
        st.markdown(
            f'<p style="font-size:42px;font-weight:bold;margin:0;text-align:center">{avg_val:.1f}</p>',
            unsafe_allow_html=True,
        )
        st.markdown('<p style="text-align:center;margin:0;font-size:14px">releases / month</p>', unsafe_allow_html=True)
    st.divider()
    chart1 = repo_df[["release_count", "trend"]].rename(columns={"release_count": "Release count", "trend": "Trend"})
    st.line_chart(chart1, color=["#8B5CF6", "#777777"], height=300, use_container_width=True)

# === Col 2: Lead Time ===
with col2:
    repo_lt = lt_df["repo_name"].unique()[0]
    repo_lt_df = lt_df[lt_df["repo_name"] == repo_lt]
    repo_lt_df = repo_lt_df[repo_lt_df["period"] >= "2021-02-01"]
    avg_val = avg_df[avg_df["repo_name"] == repo_lt]["avg_lead_time_hours"].iloc[0]

    col_desc, col_kpi = col2.columns([1, 1])
    with col_desc:
        st.markdown(
            "**Lead Time** — time from PR creation to merge.\nMeasures how quickly code changes flow through review."
        )
    with col_kpi:
        st.markdown(
            f'<p style="font-size:42px;font-weight:bold;margin:0;text-align:center">{avg_val:.0f}</p>',
            unsafe_allow_html=True,
        )
        st.markdown('<p style="text-align:center;margin:0;font-size:14px">avg hours</p>', unsafe_allow_html=True)
    st.divider()

    lt_chart_df, _ = build_chart_df(repo_lt_df, "avg_lead_time_hours")
    chart2 = lt_chart_df[["avg_lead_time_hours", "trend"]].rename(
        columns={"avg_lead_time_hours": "Lead time (hours)", "trend": "Trend"}
    )
    st.line_chart(chart2, color=["#00CED1", "#777777"], height=300)

# === Col 3: Change Failure Rate ===
with col3:
    repo_cfr = cfr_df["repo_name"].unique()[0]
    repo_cfr_df = cfr_df[cfr_df["repo_name"] == repo_cfr]
    avg_val = avg_df[avg_df["repo_name"] == repo_cfr]["avg_cfr"].iloc[0]

    col_desc, col_kpi = col3.columns([1, 1])
    with col_desc:
        st.markdown(
            "**Change Failure Rate** — % of releases causing failure.\nMeasures quality and stability. Lower is better."
        )
    with col_kpi:
        st.markdown(
            f'<p style="font-size:42px;font-weight:bold;margin:0;text-align:center">{avg_val:.1f}</p>',
            unsafe_allow_html=True,
        )
        st.markdown('<p style="text-align:center;margin:0;font-size:14px">%</p>', unsafe_allow_html=True)
    st.divider()

    cfr_chart_df, _ = build_chart_df(repo_cfr_df, "cfr_revert_rate")
    chart3 = cfr_chart_df[["cfr_revert_rate", "trend"]].rename(
        columns={"cfr_revert_rate": "Change failure rate", "trend": "Trend"}
    )
    st.line_chart(chart3, color=["#3B82F6", "#777777"], height=300)

# === Col 4: Time to Restore ===
with col4:
    repo_mttr = mttr_df["repo_name"].unique()[0]
    repo_mttr_df = mttr_df[mttr_df["repo_name"] == repo_mttr]
    avg_val = avg_df[avg_df["repo_name"] == repo_mttr]["avg_restore_days"].iloc[0]

    col_desc, col_kpi = col4.columns([1, 1])
    with col_desc:
        st.markdown(
            "**Time to Restore** — time to restore service after failure.\nMeasures incident response and recovery speed."
        )
    with col_kpi:
        st.markdown(
            f'<p style="font-size:42px;font-weight:bold;margin:0;text-align:center">{avg_val:.0f}</p>',
            unsafe_allow_html=True,
        )
        st.markdown('<p style="text-align:center;margin:0;font-size:14px">days</p>', unsafe_allow_html=True)
    st.divider()

    mttr_chart_df, _ = build_chart_df(repo_mttr_df, "avg_restore_days")
    chart4 = mttr_chart_df[["avg_restore_days", "trend"]].rename(
        columns={"avg_restore_days": "Time to restore (days)", "trend": "Trend"}
    )
    st.line_chart(chart4, color=["#EF4444", "#777777"], height=300)
