# firestore_loader.py
# One-station layout:
#   watercapture@ASU / <any parent doc> / readings / <reading doc with experiment_sequence>

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd

@dataclass
class FirestoreUnavailable(Exception):
    user_msg: str

_db = None

ROOT_COLLECTION = "watercapture@ASU"   # the only (station) collection
READINGS_SUBCOLL = "readings"          # subcollection holding the readings

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
    # Accept "exp_3" or "3"
    try:
        return int(str(exp_id).split("_")[-1])
    except Exception:
        raise FirestoreUnavailable(f"Bad experiment id: {exp_id}")

def _row_from_reading(d: Dict[str, Any], station_hint: Optional[str]) -> Dict[str, Any]:
    """Normalize a reading into a consistent row."""
    base = {
        "weight": d.get("weight"),
        "date": d.get("date"),
        "time": d.get("time"),
        "experimental_runtime": d.get("experiment_runtime"),
        "experimental_run_number": d.get("experiment_sequence"),
        "station": d.get("station") or station_hint or "watercapture@ASU",
    }
    # keep any extra fields
    for k, v in d.items():
        if k not in base and k != "experiment_sequence":
            base[k] = v
    return base

# ---------------- Public API ----------------

def get_active_experiment() -> Optional[Dict[str, Any]]:
    """
    Optional heuristic: newest sequence is "active" if the newest reading is
    within the last 10 minutes. If you don't want this, return None always.
    """
    exps = list_experiments()
    if not exps:
        return None
    newest = max(exps, key=lambda e: e["sequence"])
    return None  # keep dashboard in historical mode only for now

def list_experiments(limit: int = 200) -> List[Dict[str, Any]]:
    """
    The ONLY definition of an experiment is experiment_sequence.
    Returns [{ id:'exp_<seq>', sequence:int, count:int }, ...]
    """
    import streamlit as st

    db = _init_db()
    seq_counts: Dict[int, int] = {}
    scanned = 0

    # Scan: watercapture@ASU/<parent>/readings/*
    for parent in db.collection(ROOT_COLLECTION).stream():
        try:
            for snap in parent.reference.collection(READINGS_SUBCOLL).stream():
                scanned += 1
                rec = snap.to_dict() or {}
                seq = _safe_int(rec.get("experiment_sequence"))
                if seq is None:
                    continue
                seq_counts[seq] = seq_counts.get(seq, 0) + 1
        except Exception:
            continue

    try:
        st.sidebar.caption(f"scanned readings docs: {scanned}")
    except Exception:
        pass

    items = [
        {"id": f"exp_{seq}", "sequence": seq, "count": seq_counts[seq]}
        for seq in sorted(seq_counts.keys())
    ]
    if limit and len(items) > limit:
        items = items[:limit]
    return items

def load_experiment_data(exp_id: str, realtime: bool = False) -> pd.DataFrame:
    """
    Load every reading where experiment_sequence == selected sequence.
    """
    db = _init_db()
    seq = _parse_seq(exp_id)
    rows: List[Dict[str, Any]] = []

    # Query all parents (only one station, but many parent docs are fine)
    for parent in db.collection(ROOT_COLLECTION).stream():
        try:
            q = (
                parent.reference
                .collection(READINGS_SUBCOLL)
                .where("experiment_sequence", "==", seq)
                .stream()
            )
            for snap in q:
                rows.append(_row_from_reading(snap.to_dict() or {}, station_hint=parent.id))
        except Exception:
            continue

    df = pd.DataFrame(rows)

    # Make HH:MM:SS plottable if present
    if "experimental_runtime" in df.columns:
        try:
            df["experimental_runtime"] = pd.to_timedelta(df["experimental_runtime"])
        except Exception:
            pass

    prefer = [
        "weight", "date", "time",
        "experimental_runtime", "experimental_run_number", "station",
    ]
    if not df.empty:
        ordered = [c for c in prefer if c in df.columns] + [c for c in df.columns if c not in prefer]
        df = df[ordered]
    return df
