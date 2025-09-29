import streamlit as st
import pandas as pd
import altair as alt
from firestore_loader import get_active_experiment, list_experiments, load_experiment_data

# --- Sidebar: Experiment selection ---
st.sidebar.header("Experiment Control")

active_exp = get_active_experiment()

if active_exp:
    st.sidebar.success(f"Running Experiment: {active_exp['id']}")
    mode = "realtime"
else:
    st.sidebar.warning("No experiment running")
    all_exps = list_experiments()
    st.sidebar.write(f"Total experiments: **{len(all_exps)}**")
    selected = st.sidebar.selectbox("Select experiment:", [e["id"] for e in all_exps])
    mode = "historical"

# --- Plotting ---
if mode == "realtime":
    st.subheader("Real-Time Weight vs Experimental Time")
    data = load_experiment_data(active_exp["id"], realtime=True)  # fetch streaming updates
    chart = alt.Chart(data).mark_line().encode(
        x="experimental_time",
        y="weight"
    )
    st.altair_chart(chart, use_container_width=True)

elif mode == "historical":
    if selected:
        st.subheader(f"Experiment {selected}")
        data = load_experiment_data(selected)
        chart = alt.Chart(data).mark_line().encode(
            x="experimental_time",
            y="weight"
        )
        st.altair_chart(chart, use_container_width=True)

        # --- CSV Download ---
        csv = data.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            csv,
            f"{selected}_data.csv",
            "text/csv"
        )
