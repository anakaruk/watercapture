# ui_display.py
# UI helper functions for consistent layout/styling.

import streamlit as st

def show_header():
    st.markdown(
        """
        <h2 style="color:#2E86C1;">Water Capture Experiment Dashboard</h2>
        <p style="color:gray;">Real-time monitoring & historical experiment data</p>
        """,
        unsafe_allow_html=True,
    )


def show_footer():
    st.markdown(
        """
        <hr>
        <p style="text-align:center; color:gray; font-size:12px;">
        Water Capture Project © 2025 | ASU Research
        </p>
        """,
        unsafe_allow_html=True,
    )

