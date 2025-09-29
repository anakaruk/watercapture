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

# Historical mode (no live flag yet)
try:
    _ = get_active_experiment()  # kept for future; currently returns None
except FirestoreUnavailable as e:
    st.error(e.user_msg)
    st.stop()

# ---- Load experiment list ----
try:
    exps = list_experiments(limit=500)
except FirestoreUnavailable as e:
    st.error(e.user_msg)
    st.stop()

st.sidebar.write(f"Total experiments: **{len(exps)}**")

exp_id = None
if exps:
    # Pretty labels: Experiment #<sequence> (<count> points)
    labels = [f"Experiment #{e['sequence']} ({e['count']} points)" for e in exps]
    ids    = [e["id"] for e in exps]
    default_idx = len(labels) - 1  # newest (highest sequence)
    chosen = st.sidebar.selectbox("Select an experiment:", labels, index=default_idx)
    exp_id = ids[labels.index(chosen)]
else:
    st.info("No experiments found yet.")
    st.stop()

# ---- Plot helper ----
def draw_chart(df: pd.DataFrame, title: str):
    if df.empty:
        st.info("No data to plot yet.")
        return

    # Pick X
    if "experimental_runtime" in df.columns:
        df = df.copy()
        # Convert to timedelta if not already
        if pd.api.types.is_numeric_dtype(df["experimental_runtime"]):
            df["runtime_hms"] = pd.to_timedelta(df["experimental_runtime"], unit="s")
        else:
            try:
                df["runtime_hms"] = pd.to_timedelta(df["experimental_runtime"])
            except Exception:
                # Fall back to string/time axis
                df["runtime_hms"] = pd.to_timedelta(pd.NaT)

        # If conversion succeeded for at least some rows, use temporal axis
        if df["runtime_hms"].notna().any():
            x_enc = alt.X("runtime_hms:T", title="Experimental time (hh:mm:ss)")
        else:
            x_enc = alt.X("experimental_runtime:N", title="Experimental time")
    else:
        # Fall back to 'time' display string if present
        x_enc = alt.X("time:N", title="Time")

    # Y: show weight if present
    y_field = "weight:Q" if "weight" in df.columns else alt.value(0)

    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=x_enc,
            y=alt.Y(y_field, title="Weight"),
            tooltip=[
                alt.Tooltip("weight:Q", title="weight", format=".3f", undefined="ignore"),
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

# ---- Load & render selected experiment ----
st.subheader(f"Historical: Experiment {exp_id}")
try:
    # NOTE: loader no longer accepts 'realtime'; just call it plain
    df = load_experiment_data(exp_id)
except FirestoreUnavailable as e:
    st.error(e.user_msg)
    st.stop()

draw_chart(df, f"Experiment {exp_id}")

# Preview table (first 50 rows)
if not df.empty:
    st.dataframe(df.head(50), use_container_width=True)

# ---- CSV download ----
prefer_cols = [
    "weight", "date", "time",
    "experimental_runtime",       # HH:MM:SS or seconds → converted upstream
    "experimental_run_number",    # == experiment_sequence
    "station",
]
df_out = df.copy()
ordered = [c for c in prefer_cols if c in df_out.columns] + \
          [c for c in df_out.columns if c not in prefer_cols]
st.download_button(
    "⬇️ Download CSV",
    df_out[ordered].to_csv(index=False).encode("utf-8"),
    file_name=f"{exp_id}_data.csv",
    mime="text/csv",
)
