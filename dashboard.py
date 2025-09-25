import streamlit as st
import pandas as pd
from firestore_loader import load_data
from ui_display import show_table, show_plot
from data_play import process_data

# -----------------------------
# Streamlit Page Config
# -----------------------------
st.set_page_config(page_title="Watercapture Dashboard", layout="wide")

st.title("💧 Watercapture Dashboard")

# -----------------------------
# Sidebar Controls
# -----------------------------
station = st.sidebar.selectbox(
    "Select Station",
    ["station_TestUnit@HighBay", "station_AquaPars", "station_T50"]
)
limit = st.sidebar.slider("Rows to load", 100, 5000, 1000)

# -----------------------------
# Load Data from Firestore
# -----------------------------
df = load_data(station, limit=limit)

if df is not None and not df.empty:
    # Process data (calculations, derived fields)
    df_proc = process_data(df)

    st.subheader("Raw Data")
    show_table(df_proc)

    st.subheader("Trends")
    show_plot(df_proc)

    # Allow CSV export
    csv = df_proc.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download Data as CSV",
        data=csv,
        file_name=f"{station}_data.csv",
        mime="text/csv",
    )
else:
    st.warning("⚠️ No data loaded. Check Firestore connection or station name.")
