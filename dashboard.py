# dashboard.py
# Streamlit dashboard for displaying real-time or historical experiment data
# with CSV download option.

import streamlit as st
import pandas as pd
import altair as alt

from firestore_loader import (
    get_active_experiment,
    list_experiments,
    load_experiment_data,
)


st.set_page_config(page_title="Water Capture Dashboard", layout="wide")

st.title("💧 Water Capture Experiment Dashboard")

# --- Sidebar ---
st.sidebar.header("Experiment Control")

active_exp = get_active_experiment()

if active_exp:
    st.sidebar.success(f"Running Experiment: {active_exp['id']}")
    mode = "realtime"
    exp_id = active_exp["id"]
else:
    st.sidebar.warning("No experiment running")

    experiments = list_experiments()
    st.sidebar.write(f"Total experiments: **{len(experiments)}**")

    exp_id = st.sidebar.selectbox(
        "Select experiment:", [e["id"] for e in experiments] if experiments else []
    )
    mode = "historical" if exp_id else None

# --- Main Display ---
if mode == "realtime":
    st.subheader(f"Real-Time Plotting for Experiment {exp_id}")

    # Live refresh every 5 seconds
    data = load_experiment_data(exp_id, realtime=True)

    if not data.empty:
        chart = (
            alt.Chart(data)
            .mark_line(point=True)
            .encode(
                x=alt.X("experimental_runtime:T", title="Experimental Time"),
                y=alt.Y("weight:Q", title="Weight (g)"),
            )
            .properties(height=400)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Waiting for new data...")

elif mode == "historical" and exp_id:
    st.subheader(f"Experiment {exp_id} - Historical Data")

    data = load_experiment_data(exp_id)

    if not data.empty:
        chart = (
            alt.Chart(data)
            .mark_line(point=True)
            .encode(
                x=alt.X("experimental_runtime:T", title="Experimental Time"),
                y=alt.Y("weight:Q", title="Weight (g)"),
            )
            .properties(height=400)
        )
        st.altair_chart(chart, use_container_width=True)

        # --- CSV Download ---
        csv = data.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Download CSV",
            csv,
            f"{exp_id}_data.csv",
            "text/csv",
        )
    else:
        st.warning("No data found for this experiment.")

else:
    st.info("Select a running or historical experiment to view data.")
