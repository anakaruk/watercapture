# ui_display.py
# Simple shared UI fragments (if you want to keep using them later)

import streamlit as st

def header():
    st.markdown(
        "<h3 style='color:#2E86C1;margin-bottom:0'>Water Capture Experiment Dashboard</h3>"
        "<p style='color:gray;margin-top:4px'>Real-time monitoring & historical data</p>",
        unsafe_allow_html=True,
    )

def footer():
    st.markdown(
        "<hr><p style='text-align:center;color:gray;font-size:12px'>© 2025 Watercapture</p>",
        unsafe_allow_html=True,
    )
