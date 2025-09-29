# dashboard.py
import streamlit as st
import pandas as pd
import altair as alt

from firestore_loader import (
    get_active_experiment,
    list_experiments,
    load_experiment_data,
    FirestoreUnavailable,
)

st.set_page_config(page_title="Water Capture Dashboard", layout="wide")
st.title("💧 Water Capture Experiment Dashboard")

st.sidebar.header("Experiment Control")

# We keep the app in historical mode (no live flag yet)
try:
    active = get_active_experiment()
except FirestoreUnavailable as e:
    st.error(e.user_msg)
    st.stop()

mode = "historical"
exp_id = None

try:
    exps = list_experiments(limit=500)
except FirestoreUnavailable as e:
    st.error(e.user_msg)
    st.stop()

st.sidebar.write(f"Total experiments: **{len(exps)}**")

if exps:
    # Pretty labels: Experiment #<sequence> (<count> points)
    labels = {
        f"Experiment #{e['sequence']} ({e['count']} points)": e["id"]
        for e in exps
    }
    # default to newest sequence
    default_idx = len(labels) - 1
    chosen = st.sidebar.selectbox("Select an experiment:", list(labels.keys()), index=default_idx)
    exp_id = labels[chosen]

def draw_chart(df: pd.DataFrame, title: str):
    if df.empty:
        st.info("No data to plot yet.")
        return

    # Choose X axis
    if "experimental_runtime" in df.columns:
        # numeric seconds → timedelta; else already HH:MM:SS
        if pd.api.types.is_numeric_dtype(df["experimental_runtime"]):
            df = df.copy()
            df["runtime_hms"] = pd.to_timedelta(df["experimental_runtime"], unit="s")
            x_enc = alt.X("runtime_hms:T", title="Experimental time (hh:mm:ss)")
        else:
            # try to ensure timedelta
            df = df.copy()
            try:
                df["runtime_hms"] = pd.to_timedelta(df["experimental_runtime"])
                x_enc = alt.X("runtime_hms:T", title="Experimental time (hh:mm:ss)")
            except Exception:
                x_enc = alt.X("experimental_runtime:T", title="Experimental time (hh:mm:ss)")
    else:
        x_enc = alt.X("time:N", title="Time")

    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=x_enc,
            y=alt.Y("weight:Q", title="Weight"),
            tooltip=[
                alt.Tooltip("weight:Q", title="weight"),
                alt.Tooltip("experimental_runtime:T", title="exp time", undefined="ignore"),
                alt.Tooltip("time:N", title="time", undefined="ignore"),
                alt.Tooltip("date:N", title="date", undefined="ignore"),
                alt.Tooltip("experimental_run_number:N", title="sequence", undefined="ignore"),
                alt.Tooltip("station:N", title="station", undefined="ignore"),
            ],
        )
        .properties(title=title, height=420)
    )
    st.altair_chart(chart, use_container_width=True)

if mode == "historical" and exp_id:
    st.subheader(f"Historical: Experiment {exp_id}")
    try:
        df = load_experiment_data(exp_id, realtime=False)
    except FirestoreUnavailable as e:
        st.error(e.user_msg)
        st.stop()

    draw_chart(df, f"Experiment {exp_id}")

    # CSV download: the exact columns you store (plus extras)
    prefer_cols = [
        "weight", "date", "time",
        "experimental_runtime",       # HH:MM:SS (or seconds → converted upstream)
        "experimental_run_number",    # == experiment_sequence
        "station",
    ]
    df_out = df.copy()
    ordered = [c for c in prefer_cols if c in df_out.columns] + [c for c in df_out.columns if c not in prefer_cols]
    st.download_button(
        "⬇️ Download CSV",
        df_out[ordered].to_csv(index=False).encode("utf-8"),
        file_name=f"{exp_id}_data.csv",
        mime="text/csv",
    )
else:
    st.info("Pick a historical experiment in the sidebar to view data.")
