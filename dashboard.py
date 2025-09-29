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

# ---- Detect live experiment (≤5 min since last point) ----
try:
    live_info = get_active_experiment(live_window_s=300)  # 5 minutes
except FirestoreUnavailable as e:
    st.error(e.user_msg)
    st.stop()

# ---- Load experiment list (historical) ----
try:
    exps = list_experiments(limit=500)
except FirestoreUnavailable as e:
    st.error(e.user_msg)
    st.stop()

st.sidebar.write(f"Total experiments: **{len(exps)}**")

# ---- Mode selection ----
mode_options = []
if live_info and live_info.get("live"):
    mode_options.append(f"Live (Experiment #{live_info['sequence']})")
mode_options.append("Historical")

mode = st.sidebar.radio("Mode:", mode_options, index=0)

# ---- Historical chooser ----
exp_id_hist = None
if exps:
    labels = [f"Experiment #{e['sequence']} ({e['count']} points)" for e in exps]
    ids    = [e["id"] for e in exps]
    default_idx = len(labels) - 1
    chosen = st.sidebar.selectbox("Select an experiment:", labels, index=default_idx,
                                  disabled=(mode.startswith("Live")))
    exp_id_hist = ids[labels.index(chosen)]
else:
    st.info("No experiments found yet.")
    st.stop()

# ---- Chart helper ----
def draw_chart(df: pd.DataFrame, title: str):
    if df.empty:
        st.info("No data to plot yet.")
        return
    df = df.copy()

    # X axis selection (prefer runtime seconds)
    x_enc = None
    if "experimental_runtime" in df.columns:
        td = pd.to_timedelta(df["experimental_runtime"], errors="coerce")
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

    if x_enc is None and "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        if df["timestamp"].notna().any():
            x_enc = alt.X("timestamp:T", title="Timestamp")

    if x_enc is None:
        if "time" in df.columns:
            x_enc = alt.X("time:N", title="Time")
        else:
            df["row_index"] = range(len(df))
            x_enc = alt.X("row_index:Q", title="Index")

    # Y axis (force numeric)
    y_field_name = None
    if "weight" in df.columns:
        df["weight_num"] = pd.to_numeric(df["weight"], errors="coerce")
        if df["weight_num"].notna().any():
            y_field_name = "weight_num"
    if y_field_name is None:
        df["value"] = 0.0
        y_field_name = "value"

    # Tooltips only for existing fields
    tooltips = []
    if "weight_num" in df.columns:
        tooltips.append(alt.Tooltip("weight_num:Q", title="weight", format=".3f"))
    elif "weight" in df.columns:
        tooltips.append(alt.Tooltip("weight:N", title="weight"))
    if "runtime_hms" in df.columns:
        tooltips.append(alt.Tooltip("runtime_hms:N", title="exp time"))
    if "time" in df.columns:
        tooltips.append(alt.Tooltip("time:N", title="time"))
    if "date" in df.columns:
        tooltips.append(alt.Tooltip("date:N", title="date"))
    if "experimental_run_number" in df.columns:
        tooltips.append(alt.Tooltip("experimental_run_number:N", title="sequence"))
    if "station" in df.columns:
        tooltips.append(alt.Tooltip("station:N", title="station"))

    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=x_enc,
            y=alt.Y(f"{y_field_name}:Q", title="Weight"),
            tooltip=tooltips if tooltips else None,
        )
        .properties(title=title, height=420)
    )
    st.altair_chart(chart, use_container_width=True)

# ---- Render ----
if mode.startswith("Live") and live_info:
    live_id = live_info["id"]
    st.subheader(f"Live: Experiment {live_id}")
    # Small manual refresh (press R or click button)
    colA, colB = st.columns([1, 6])
    with colA:
        if st.button("Refresh"):
            st.experimental_rerun()
    try:
        df_live = load_experiment_data(live_id, order="asc")
    except FirestoreUnavailable as e:
        st.error(e.user_msg)
        st.stop()
    draw_chart(df_live, f"Experiment {live_id}")
    if not df_live.empty:
        st.dataframe(df_live.tail(50), use_container_width=True)
else:
    st.subheader(f"Historical: Experiment {exp_id_hist}")
    try:
        df = load_experiment_data(exp_id_hist, order="asc")
    except FirestoreUnavailable as e:
        st.error(e.user_msg)
        st.stop()
    draw_chart(df, f"Experiment {exp_id_hist}")
    if not df.empty:
        st.dataframe(df.head(50), use_container_width=True)

# ---- CSV download (both modes use current df variable) ----
current_df = df_live if (mode.startswith("Live") and live_info) else df
if current_df is not None and not current_df.empty:
    prefer_cols = [
        "weight", "date", "time",
        "experimental_runtime",
        "experimental_run_number",
        "station",
    ]
    df_out = current_df.copy()
    ordered = [c for c in prefer_cols if c in df_out.columns] + \
              [c for c in df_out.columns if c not in prefer_cols]
    st.download_button(
        "⬇️ Download CSV",
        df_out[ordered].to_csv(index=False).encode("utf-8"),
        file_name=f"{('live' if mode.startswith('Live') else exp_id_hist)}_data.csv",
        mime="text/csv",
    )
