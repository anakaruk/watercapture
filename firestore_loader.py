# firestore_loader.py
# Firestore helpers with lazy client init + friendly errors for Streamlit Cloud.

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
import pandas as pd

# ---------------- Errors ----------------
@dataclass
class FirestoreUnavailable(Exception):
    user_msg: str

# ---------------- Lazy client ----------------
_db = None

def _init_db():
    """Create a Firestore client the first time we need it.
    - Streamlit Cloud: reads service account from st.secrets["gcp_service_account"]
    - Local/dev: uses GOOGLE_APPLICATION_CREDENTIALS, or default app creds
    """
    global _db
    if _db is not None:
        return _db

    try:
        # Lazy imports so module import never fails
        from google.cloud import firestore
    except Exception:
        raise FirestoreUnavailable(
            "Firestore client library is missing. Add `google-cloud-firestore` to **requirements.txt**."
        )

    # Try Streamlit secrets first (does not error if Streamlit is not installed)
    creds = None
    try:
        import streamlit as st
        if "gcp_service_account" in st.secrets:
            from google.oauth2.service_account import Credentials
            creds = Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]))
    except Exception:
        # Not running under Streamlit or no secrets configured; fall back to ADC
        creds = None

    try:
        _db = firestore.Client(credentials=creds) if creds is not None else firestore.Client()
        return _db
    except Exception as e:
        raise FirestoreUnavailable(
            "Cannot connect to Firestore. Make sure credentials are set:\n"
            "• On Streamlit Cloud: add your service account JSON under `gcp_service_account` in **secrets**.\n"
            "• Locally: set GOOGLE_APPLICATION_CREDENTIALS to your JSON path.\n"
            f"Details: {e}"
        )

# ---------------- Public API ----------------
def get_active_experiment() -> Optional[Dict[str, Any]]:
    """Return a single running experiment or None."""
    db = _init_db()
    try:
        q = db.collection("experiments").where("status", "==", "running").limit(1).stream()
        for doc in q:
            payload = doc.to_dict() or {}
            payload["id"] = doc.id
            return payload
        return None
    except Exception as e:
        raise FirestoreUnavailable(f"Failed to query active experiment: {e}")

def list_experiments(limit: int = 500) -> List[Dict[str, Any]]:
    db = _init_db()
    try:
        q = (
            db.collection("experiments")
            .order_by("start_time", direction=_order_desc(db))
            .limit(limit)
            .stream()
        )
        return [{"id": d.id, **(d.to_dict() or {})} for d in q]
    except Exception as e:
        raise FirestoreUnavailable(f"Failed to list experiments: {e}")

def _order_desc(db):
    # small helper to avoid importing Query at module import
    from google.cloud.firestore_v1 import Query
    return Query.DESCENDING

def load_experiment_data(exp_id: str, realtime: bool = False) -> pd.DataFrame:
    """Return a DataFrame with columns:
       weight, date, time, experimental_runtime (seconds or hh:mm:ss),
       experimental_run_number, timestamp, and any extra uploaded fields.
    """
    db = _init_db()
    try:
        stream = (
            db.collection("experiments")
            .document(exp_id)
            .collection("data")
            .order_by("timestamp")
            .stream()
        )
        rows = []
        for snap in stream:
            d = snap.to_dict() or {}
            # Normalize fields
            ts = d.get("timestamp")
            if isinstance(ts, datetime):
                ts_utc = ts.astimezone(timezone.utc)
            else:
                ts_utc = None

            runtime = _normalize_runtime(
                d.get("experimental_runtime"),
                ts_utc=ts_utc,
                exp_meta=_get_exp_meta(db, exp_id),
            )

            row = {
                "weight": d.get("weight"),
                "date": d.get("date"),
                "time": d.get("time"),
                "experimental_runtime": runtime,  # numeric seconds (preferred) or hh:mm:ss parsed to datetime in dashboard
                "experimental_run_number": d.get("experimental_run_number"),
                "timestamp": ts_utc,
            }
            # include everything else too
            for k, v in d.items():
                if k not in row:
                    row[k] = v
            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # If runtime is "HH:MM:SS" strings, keep as string; if numeric seconds, keep numeric.
        # Dashboard will convert for plotting.
        return df
    except Exception as e:
        raise FirestoreUnavailable(f"Failed to load experiment data: {e}")

# ---------------- Helpers ----------------
_exp_meta_cache: Dict[str, Dict[str, Any]] = {}

def _get_exp_meta(db, exp_id: str) -> Dict[str, Any]:
    if exp_id in _exp_meta_cache:
        return _exp_meta_cache[exp_id]
    doc = db.collection("experiments").document(exp_id).get()
    meta = doc.to_dict() or {}
    _exp_meta_cache[exp_id] = meta
    return meta

def _normalize_runtime(val, ts_utc: Optional[datetime], exp_meta: Dict[str, Any]) -> Any:
    """
    Accepts several uploader styles:
    - integer/float seconds since start -> return numeric seconds
    - "HH:MM:SS" string                     -> return same string
    - missing runtime but we have timestamp & start_time -> compute seconds
    """
    # numeric seconds (already good)
    if isinstance(val, (int, float)):
        return float(val)

    # HH:MM:SS string
    if isinstance(val, str) and len(val.split(":")) == 3:
        return val

    # compute from timestamp and start_time if possible
    start = exp_meta.get("start_time")
    if isinstance(start, datetime) and isinstance(ts_utc, datetime):
        delta = ts_utc - start.astimezone(timezone.utc)
        return max(delta.total_seconds(), 0.0)

    # As a last resort, None
    return None
