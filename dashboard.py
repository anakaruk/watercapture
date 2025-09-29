# dashboard.py
# Streamlit dashboard for real-time & historical plotting with CSV download.

import streamlit as st
import pandas as pd
import altair as alt

# Local helpers (safe to import now; Firestore is lazily created inside)
from firestore_loader import (
    get_active_experiment,
    list_experiments,
    load_experiment_data,
    FirestoreUnavailable,
)

st.set_page_config(page_title="Water Capture Dashboard", layout="wide")
st.title("💧 Water Capture Experiment Dashboard")

# -------- Sidebar: experiment selection --------
st.sidebar.header("Experiment Control")

try:
    active = get_active_experiment()
except FirestoreUnavailable as e:
    st.error(e.user_msg)
    st.stop()

mode = None
exp_id = None

if active:
    st.sidebar.success(f"Running Experiment: {active['id']}")
    exp_id = active["id"]
    mode = "realtime"
else:
    st.sidebar.warning("No experiment running right now.")
    try:
        exps = list_experiments(limit=200)
    except FirestoreUnavailable as e:
        st.error(e.user_msg)
        st.stop()

    st.sidebar.write(f"Total experiments: **{len(exps)}**")
    if exps:
        exp_id = st.sidebar.selectbox("Select an experiment:", [e["id"] for e in exps])
        if exp_id:
            mode = "historical"

# -------- Main area --------
def draw_chart(df: pd.DataFrame, title: str):
    if df.empty:
        st.info("No data to plot yet.")
        return

    # Ensure types
    if "experimental_runtime" in df.columns:
        # treat runtime as elapsed seconds for x-axis
        if pd.api.types.is_numeric_dtype(df["experimental_runtime"]):
            df = df.copy()
            df["runtime_hms"] = pd.to_timedelta(df["experimental_runtime"], unit="s")
            x_enc = alt.X("runtime_hms:T", title="Experimental time (hh:mm:ss)")
        else:
            x_enc = alt.X("experimental_runtime:T", title="Experimental time (hh:mm:ss)")
    else:
        x_enc = alt.X("timestamp:T", title="Timestamp")

    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=x_enc,
            y=alt.Y("weight:Q", title="Weight"),
            tooltip=[
                alt.Tooltip("weight:Q"),
                alt.Tooltip("runtime_hms:T", title="exp time", format="%H:%M:%S", undefined="ignore"),
                alt.Tooltip("experimental_runtime:T", title="exp time", format="%H:%M:%S", undefined="ignore"),
                alt.Tooltip("timestamp:T", title="timestamp", undefined="ignore"),
                alt.Tooltip("experimental_run_number:N", title="run #", undefined="ignore"),
            ],
        )
        .properties(title=title, height=420)
    )
    st.altair_chart(chart, use_container_width=True)

if mode == "realtime":
    st.subheader(f"Real-Time: Experiment {exp_id}")
    # Auto-refresh every 5 seconds for live runs
    st.autorefresh(interval=5000, key="realtime_refresh")
    try:
        df = load_experiment_data(exp_id, realtime=True)
    except FirestoreUnavailable as e:
        st.error(e.user_msg)
        st.stop()
    draw_chart(df, f"Experiment {exp_id}")
elif mode == "historical" and exp_id:
    st.subheader(f"Historical: Experiment {exp_id}")
    try:
        df = load_experiment_data(exp_id, realtime=False)
    except FirestoreUnavailable as e:
        st.error(e.user_msg)
        st.stop()
    draw_chart(df, f"Experiment {exp_id}")

    # CSV download with the exact fields you asked for
    prefer_cols = [
        "weight",
        "date",
        "time",
        "experimental_runtime",         # seconds or hh:mm:ss (depends on your uploader)
        "experimental_run_number",
        "timestamp",
    ]
    # Keep any additional uploaded fields too
    ordered_cols = [c for c in prefer_cols if c in df.columns] + [c for c in df.columns if c not in prefer_cols]
    csv_bytes = df[ordered_cols].to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", csv_bytes, file_name=f"{exp_id}_data.csv", mime="text/csv")
else:
    st.info("Pick a historical experiment in the sidebar to view data.")
