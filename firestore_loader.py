# firestore_loader.py
# Helpers to interact with Firestore for experiments and data.

import pandas as pd
from google.cloud import firestore
from datetime import datetime

# Initialize Firestore client
db = firestore.Client()

def get_active_experiment():
    """Return currently active experiment metadata or None."""
    docs = db.collection("experiments").where("status", "==", "running").stream()
    active = None
    for doc in docs:
        active = {"id": doc.id, **doc.to_dict()}
        break
    return active


def list_experiments():
    """List all experiments in the database."""
    docs = db.collection("experiments").order_by("start_time", direction=firestore.Query.DESCENDING).stream()
    return [{"id": doc.id, **doc.to_dict()} for doc in docs]


def load_experiment_data(exp_id, realtime=False):
    """Load experiment data as pandas DataFrame."""
    docs = (
        db.collection("experiments")
        .document(exp_id)
        .collection("data")
        .order_by("timestamp")
        .stream()
    )

    rows = []
    for doc in docs:
        d = doc.to_dict()
        rows.append(
            {
                "weight": d.get("weight"),
                "date": d.get("date"),
                "time": d.get("time"),
                "experimental_runtime": _parse_runtime(d.get("experimental_runtime")),
                "experimental_run_number": d.get("experimental_run_number"),
                # Keep any extra fields
                **{k: v for k, v in d.items() if k not in ["weight", "date", "time", "experimental_runtime", "experimental_run_number"]},
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Ensure proper datetime for runtime plotting
    if "experimental_runtime" in df.columns:
        df["experimental_runtime"] = pd.to_datetime(df["experimental_runtime"], errors="coerce")

    return df


def _parse_runtime(val):
    """Convert hh:mm:ss string to datetime-like for plotting."""
    if isinstance(val, str):
        try:
            return datetime.strptime(val, "%H:%M:%S")
        except ValueError:
            pass
    return None
