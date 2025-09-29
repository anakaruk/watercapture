# firestore_loader.py
# Reads Firestore structure:
#   watercapture@ASU (collection)
#      └── <doc_id>
#          └── readings (subcollection)

from dataclasses import dataclass
from typing import List, Dict, Any
import pandas as pd

@dataclass
class FirestoreUnavailable(Exception):
    user_msg: str

_db = None

def _init_db():
    """Init Firestore client with explicit project from Streamlit secrets."""
    global _db
    if _db is not None:
        return _db

    try:
        from google.cloud import firestore
        from google.oauth2.service_account import Credentials
        import streamlit as st
    except Exception as e:
        raise FirestoreUnavailable(f"Missing libraries: {e}")

    try:
        sa_info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa_info)
        project_id = st.secrets.get("gcp_project") or sa_info.get("project_id")
        if not project_id:
            raise FirestoreUnavailable("No project id in secrets")

        _db = firestore.Client(project=project_id, credentials=creds)
        st.sidebar.info(f"Connected to Firestore project: {project_id}")
        return _db
    except Exception as e:
        raise FirestoreUnavailable(f"Firestore init failed: {e}")

def list_experiments(limit: int = 5) -> List[Dict[str, Any]]:
    """List unique experiment sequences across all readings subcollections."""
    db = _init_db()
    try:
        stations = db.collection("watercapture@ASU").stream()
        seen = {}

        for station_doc in stations:
            readings = station_doc.reference.collection("readings").limit(200).stream()
            for d in readings:
                rec = d.to_dict() or {}
                seq = rec.get("experiment_sequence", 0)
                key = f"exp_{seq}"
                if key not in seen:
                    seen[key] = {
                        "id": key,
                        "station": rec.get("station"),
                        "doc_id": station_doc.id,
                    }

        return list(seen.values())
    except Exception as e:
        raise FirestoreUnavailable(f"List experiments failed: {e}")

def load_experiment_data(exp_id: str, realtime: bool = False) -> pd.DataFrame:
    """Load readings for a given experiment_sequence from all docs."""
    db = _init_db()
    try:
        seq = int(exp_id.split("_")[1])
        rows = []

        stations = db.collection("watercapture@ASU").stream()
        for station_doc in stations:
            readings = station_doc.reference.collection("readings") \
                .where("experiment_sequence", "==", seq).stream()
            for snap in readings:
                d = snap.to_dict() or {}
                rows.append({
                    "weight": d.get("weight"),
                    "date": d.get("date"),
                    "time": d.get("time"),
                    "experimental_runtime": d.get("experiment_runtime"),
                    "experimental_run_number": d.get("experiment_sequence"),
                    "station": d.get("station"),
                })

        return pd.DataFrame(rows)
    except Exception as e:
        raise FirestoreUnavailable(f"Load data failed: {e}")
