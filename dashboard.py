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
    _ = get_active_experiment()  # currently returns None; kept for future live mode
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

    df = df.copy()

    # ---------- X axis selection ----------
    x_enc = None
    # Prefer experimental runtime if present
    if "experimental_runtime" in df.columns:
        td = pd.to_timedelta(df["experimental_runtime"], errors="coerce")
        # numeric seconds for Vega-Lite quantitative axis
        df["runtime_s"] = td.dt.total_seconds()

        def _fmt_hms(v):
            if pd.isna(v):
                return None
            v = int(v)
            h, r = divmod(v, 3600)
            m, s = divmod(r, 60)
            return f"{h:02d}:{m:02d}:{s:02d}"

        df["runtime_hms"] = df["runtime_s"].apply(_fmt_hms)
        if df["runtime_s"].notna().any():
            x_enc = alt.X("runtime_s:Q", title="Experimental time (s)")

    # If no runtime, try real timestamp
    if x_enc is None and "timestamp" in df.columns and df["timestamp"].notna().any():
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        x_enc = alt.X("timestamp:T", title="Timestamp")

    # Last resort: fall back to the raw 'time' string
    if x_enc is None:
        x_enc = alt.X("time:N", title="Time")

    # ---------- Y axis ----------
    y_field = "weight:Q" if "weight" in df.columns else alt.value(0)

    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=x_enc,
            y=alt.Y(y_field, title="Weight"),
            tooltip=[
                alt.Tooltip("weight:Q", title="weight", format=".3f", undefined="ignore"),
                alt.Tooltip("runtime_hms:N", title="exp time", undefined="ignore"),
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
    df = load_experiment_data(exp_id)  # loader no longer uses 'realtime'
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
    "experimental_runtime",       # HH:MM:SS or seconds; loader leaves as-is
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
