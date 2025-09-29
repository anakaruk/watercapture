# firestore_loader.py
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import os
import pandas as pd

@dataclass
class FirestoreUnavailable(Exception):
    user_msg: str

_db = None
_exp_meta_cache: Dict[str, Dict[str, Any]] = {}

def _init_db():
    """Lazy-create a Firestore client with explicit project detection."""
    global _db
    if _db is not None:
        return _db

    try:
        from google.cloud import firestore
        from google.oauth2.service_account import Credentials
    except Exception:
        raise FirestoreUnavailable(
            "Firestore client library is missing. Add `google-cloud-firestore` to requirements.txt."
        )

    # Defaults
    creds = None
    project_id = (
        os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCLOUD_PROJECT")
        or None
    )

    # Streamlit Cloud: prefer secrets
    try:
        import streamlit as st
        s = st.secrets
        if "gcp_service_account" in s:
            sa = dict(s["gcp_service_account"])  # service account JSON object
            creds = Credentials.from_service_account_info(sa)
            project_id = s.get("gcp_project") or sa.get("project_id") or project_id
        elif "gcp_project" in s and not project_id:
            project_id = s.get("gcp_project")
    except Exception:
        pass  # not running under Streamlit or no secrets configured

    # Local/dev: GOOGLE_APPLICATION_CREDENTIALS / ADC
    try:
        if creds is not None:
            _db = firestore.Client(project=project_id, credentials=creds)
        else:
            # ADC; if project still unknown, let user know clearly
            _db = firestore.Client(project=project_id)
            if _db.project is None:
                raise FirestoreUnavailable(
                    "Cannot connect to Firestore. No project detected from ADC or secrets."
                )
        return _db
    except FirestoreUnavailable:
        raise
    except Exception as e:
        raise FirestoreUnavailable(
            "Cannot connect to Firestore. Make sure credentials & project are set:\n"
            "• Streamlit Cloud: put service account JSON in secrets as `gcp_service_account` "
            "and set `gcp_project` (or ensure JSON contains project_id).\n"
            "• Local: set GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT.\n"
            f"Details: {e}"
        )

def _order_desc():
    from google.cloud.firestore_v1 import Query
    return Query.DESCENDING

def get_active_experiment() -> Optional[Dict[str, Any]]:
    db = _init_db()
    try:
        q = db.collection("experiments").where("status", "==", "running").limit(1).stream()
        for doc in q:
            d = doc.to_dict() or {}
            d["id"] = doc.id
            return d
        return None
    except Exception as e:
        raise FirestoreUnavailable(f"Failed to query active experiment: {e}")

def list_experiments(limit: int = 500) -> List[Dict[str, Any]]:
    db = _init_db()
    try:
        q = db.collection("experiments").order_by("start_time", direction=_order_desc()).limit(limit).stream()
        return [{"id": d.id, **(d.to_dict() or {})} for d in q]
    except Exception as e:
        raise FirestoreUnavailable(f"Failed to list experiments: {e}")

def load_experiment_data(exp_id: str, realtime: bool = False) -> pd.DataFrame:
    db = _init_db()
    try:
        stream = (
            db.collection("experiments")
            .document(exp_id)
            .collection("data")
            .order_by("timestamp")
            .stream()
        )
        meta = _get_exp_meta(db, exp_id)
        rows = []
        for snap in stream:
            d = snap.to_dict() or {}
            ts = d.get("timestamp")
            ts_utc = ts.astimezone(timezone.utc) if isinstance(ts, datetime) else None
            runtime = _normalize_runtime(d.get("experimental_runtime"), ts_utc, meta)

            row = {
                "weight": d.get("weight"),
                "date": d.get("date"),
                "time": d.get("time"),
                "experimental_runtime": runtime,         # seconds or "HH:MM:SS"
                "experimental_run_number": d.get("experimental_run_number"),
                "timestamp": ts_utc,
            }
            for k, v in d.items():
                if k not in row:
                    row[k] = v
            rows.append(row)

        return pd.DataFrame(rows)
    except Exception as e:
        raise FirestoreUnavailable(f"Failed to load experiment data: {e}")

def _get_exp_meta(db, exp_id: str) -> Dict[str, Any]:
    if exp_id in _exp_meta_cache:
        return _exp_meta_cache[exp_id]
    doc = db.collection("experiments").document(exp_id).get()
    meta = doc.to_dict() or {}
    _exp_meta_cache[exp_id] = meta
    return meta

def _normalize_runtime(val, ts_utc: Optional[datetime], meta: Dict[str, Any]):
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str) and len(val.split(":")) == 3:
        return val
    start = meta.get("start_time")
    if isinstance(start, datetime) and isinstance(ts_utc, datetime):
        return max((ts_utc - start.astimezone(timezone.utc)).total_seconds(), 0.0)
    return None
