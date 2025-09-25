
import streamlit as st
import pandas as pd
from google.cloud import firestore
from google.oauth2 import service_account


def get_client():
    """
    Create a Firestore client using Streamlit Cloud secrets.
    Expects st.secrets["google_cloud"] with service account fields.
    """
    creds_dict = st.secrets["google_cloud"]
    creds = service_account.Credentials.from_service_account_info(dict(creds_dict))
    return firestore.Client(credentials=creds, project=creds_dict["project_id"])


def load_data(station: str, limit: int = 1000) -> pd.DataFrame:
    """
    Load measurement data from Firestore for a given station.
    Args:
        station (str): Station name (matches "station" field in Firestore).
        limit (int): Number of rows to fetch.
    Returns:
        DataFrame with data sorted by timestamp.
    """
    try:
        db = get_client()
        docs = (
            db.collection("measurements")
            .where("station", "==", station)
            .order_by("timestamp", direction=firestore.Query.DESCENDING)
            .limit(limit)
            .stream()
        )
        rows = [doc.to_dict() for doc in docs]
        df = pd.DataFrame(rows)

        if not df.empty and "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp")

        return df
    except Exception as e:
        st.error(f"Error loading Firestore data: {e}")
        return pd.DataFrame()
