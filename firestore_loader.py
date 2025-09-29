# firestore_loader.py
# Project: watercapture
# Searches these paths (in this order):
#   A) watercapture@ASU / (doc: "readings") / readings / <reading_doc>
#   B) watercapture@ASU / <any doc> / readings / <reading_doc>
#   C) readings / <reading_doc>   (top-level fallback)

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd

@dataclass
class FirestoreUnavailable(Exception):
    user_msg: str

_db = None

ROOT_COLLECTION = "watercapture@ASU"  # station docs live here
SPECIAL_DOC = "readings"              # special parent doc that holds a subcollection also called "readings"
SUBCOLL = "readings"                  # subcollection name under station or SPECIAL_DOC
TOPLEVEL_READINGS = "readings"        # optional top-level fallback

def _init_db():
    """Init Firestore client from Streamlit Secrets (gcp_project + gcp_service_account)."""
    global _db
    if _db is not None:
        return _db
    try:
        from google.cloud import firestore
        from google.oauth2.service_account import Credentials
        import streamlit as st
    except Exception as e:
        raise FirestoreUnavailable(
            "Missing libs. Ensure requirements.txt includes: "
            "streamlit, pandas, google-cloud-firestore, google-auth. "
            f"Details: {e}"
        )
    try:
        sa = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa)
        project_id = st.secrets.get("gcp_project") or sa.get("project_id")
        if not project_id:
            raise FirestoreUnavailable("No project id found in Streamlit secrets.")
        _db = firestore.Client(project=project_id, credentials=creds)
        try:
            st.sidebar.info(f"Connected to Firestore: {project_id}")
        except Exception:
            pass
        return _db
    except Exception as e:
        raise FirestoreUnavailable(f"Firestore init failed: {e}")

def _safe_int(v) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

def _parse_seq(exp_id: str) -> int:
    try:
        return int(str(exp_id).split("_")[-1])
    except Exception:
        raise FirestoreUnavailable(f"Bad experiment id: {exp_id}")

def _row_from_reading(d: Dict[str, Any], station_hint: Optional[str]) -> Dict[str, Any]:
    base = {
        "weight": d.get("weight"),
        "date": d.get("date"),
        "time": d.get("time"),
        "experimental_runtime": d.get("experiment_runtime"),
        "experimental_run_number": d.get("experiment_sequence"),
        "station": d.get("station") or station_hint,
    }
    # include extras without overwriting the above
    for k, v in d.items():
        if k not in base and k != "experiment_sequence":
            base[k] = v
    return base

# ---------------- Public API ----------------

def get_active_experiment() -> Optional[Dict[str, Any]]:
    # With the current flat readings structure, we don’t track a “running” flag.
    return None

def list_experiments(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Treat each unique experiment_sequence as an experiment.
    Returns: [{id:'exp_<seq>', sequence:int, count:int, station:str|None}, ...]
    """
    db = _init_db()
    seq_counts: Dict[int, int] = {}
    seq_station: Dict[int, Optional[str]] = {}

    # A) Explicit path: watercapture@ASU/readings/readings/*
    try:
        parent = db.collection(ROOT_COLLECTION).document(SPECIAL_DOC)
        for snap in parent.collection(SUBCOLL).stream():
            rec = snap.to_dict() or {}
            seq = _safe_int(rec.get("experiment_sequence"))
            if seq is None:
                continue
            seq_counts[seq] = seq_counts.get(seq, 0) + 1
            if seq not in seq_station:
                seq_station[seq] = rec.get("station") or ROOT_COLLECTION
    except Exception:
        pass

    # B) Generic: watercapture@ASU/<any doc>/readings/*
    try:
        for station_doc in db.collection(ROOT_COLLECTION).stream():
            try:
                for snap in station_doc.reference.collection(SUBCOLL).stream():
                    rec = snap.to_dict() or {}
                    seq = _safe_int(rec.get("experiment_sequence"))
                    if seq is None:
                        continue
                    seq_counts[seq] = seq_counts.get(seq, 0) + 1
                    if seq not in seq_station:
                        seq_station[seq] = rec.get("station") or station_doc.id
            except Exception:
                continue
    except Exception:
        pass

    # C) Top-level fallback: readings/*
    try:
        for snap in db.collection(TOPLEVEL_READINGS).stream():
            rec = snap.to_dict() or {}
            seq = _safe_int(rec.get("experiment_sequence"))
            if seq is None:
                continue
            seq_counts[seq] = seq_counts.get(seq, 0) + 1
            if seq not in seq_station:
                seq_station[seq] = rec.get("station")
    except Exception:
        pass

    items = [
        {"id": f"exp_{seq}", "sequence": seq, "count": seq_counts[seq], "station": seq_station.get(seq)}
        for seq in sorted(seq_counts.keys())
    ]
    if limit and len(items) > limit:
        items = items[:limit]
    return items

def load_experiment_data(exp_id: str, realtime: bool = False) -> pd.DataFrame:
    """
    Load all readings for a given experiment_sequence across all three locations.
    Returns DataFrame (weight, date, time, experimental_runtime, experimental_run_number, station, +extras).
    """
    db = _init_db()
    seq = _parse_seq(exp_id)
    rows: List[Dict[str, Any]] = []

    # A) Explicit: watercapture@ASU/readings/readings/*
    try:
        parent = db.collection(ROOT_COLLECTION).document(SPECIAL_DOC)
        for snap in parent.collection(SUBCOLL).where("experiment_sequence", "==", seq).stream():
            rows.append(_row_from_reading(snap.to_dict() or {}, station_hint=ROOT_COLLECTION))
    except Exception:
        pass

    # B) Generic: watercapture@ASU/<any doc>/readings/*
    try:
        for station_doc in db.collection(ROOT_COLLECTION).stream():
            try:
                q = (
                    station_doc.reference
                    .collection(SUBCOLL)
                    .where("experiment_sequence", "==", seq)
                    .stream()
                )
                for snap in q:
                    rows.append(_row_from_reading(snap.to_dict() or {}, station_hint=station_doc.id))
            except Exception:
                continue
    except Exception:
        pass

    # C) Top-level fallback: readings/*
    try:
        for snap in db.collection(TOPLEVEL_READINGS).where("experiment_sequence", "==", seq).stream():
            d = snap.to_dict() or {}
            rows.append(_row_from_reading(d, station_hint=d.get("station")))
    except Exception:
        pass

    df = pd.DataFrame(rows)

    # Make HH:MM:SS plottable if present
    if "experimental_runtime" in df.columns:
        try:
            df["experimental_runtime"] = pd.to_timedelta(df["experimental_runtime"])
        except Exception:
            pass

    prefer = ["weight", "date", "time", "experimental_runtime", "experimental_run_number", "station"]
    if not df.empty:
        ordered = [c for c in prefer if c in df.columns] + [c for c in df.columns if c not in prefer]
        df = df[ordered]
    return df
