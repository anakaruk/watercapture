# firestore_loader.py
# Canonical path (your screenshots/spec):
#   watercapture / watercapture@ASU / readings / <reading_doc>
#
# Experiments are defined ONLY by `experiment_sequence`.
# One station. Historical-only (no live “running” detection).

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd

@dataclass
class FirestoreUnavailable(Exception):
    user_msg: str

_db = None

ROOT_COLLECTION = "watercapture"          # top-level collection
STATION_DOC    = "watercapture@ASU"       # single (fixed) station document
READINGS_SUB   = "readings"               # subcollection with reading docs


# ---------------- internal ----------------

def _init_db():
    """Create Firestore client from Streamlit Secrets (gcp_project + gcp_service_account)."""
    global _db
    if _db is not None:
        return _db
    try:
        from google.cloud import firestore
        from google.oauth2.service_account import Credentials
        import streamlit as st
    except Exception as e:
        raise FirestoreUnavailable(
            "Missing libs. Add to requirements.txt: "
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

        # Small connection hint in sidebar (best-effort)
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
    """Normalize a reading into a consistent row (keep extras)."""
    out = {
        "weight": d.get("weight"),
        "date": d.get("date"),
        "time": d.get("time"),
        "experimental_runtime": d.get("experiment_runtime"),
        "experimental_run_number": d.get("experiment_sequence"),
        "station": d.get("station") or station_hint or STATION_DOC,
    }
    for k, v in d.items():
        if k not in out and k != "experiment_sequence":
            out[k] = v
    return out


# ---------------- public API (used by dashboard.py) ----------------

def get_active_experiment() -> Optional[Dict[str, Any]]:
    """No live/run flag in this dataset; keep dashboard in historical mode."""
    return None


def list_experiments(limit: int = 200) -> List[Dict[str, Any]]:
    """
    Experiments = unique experiment_sequence values inside:
      watercapture / watercapture@ASU / readings / *
    Returns: [{id:'exp_<seq>', sequence:int, count:int}, ...]
    """
    import streamlit as st
    db = _init_db()

    seq_counts: Dict[int, int] = {}
    scanned = 0

    # Resolve and check parent doc
    parent = db.collection(ROOT_COLLECTION).document(STATION_DOC)
    try:
        ps = parent.get()
        if not ps.exists:
            msg = f"Doc not found: {ROOT_COLLECTION}/{STATION_DOC}"
            st.sidebar.error(msg)
            raise FirestoreUnavailable(msg)
    except Exception as e:
        st.sidebar.error(f"Path resolve error: {e}")
        raise FirestoreUnavailable(f"Path resolve error: {e}")

    # Scan the readings subcollection
    try:
        for snap in parent.collection(READINGS_SUB).stream():
            scanned += 1
            rec = snap.to_dict() or {}
            seq = _safe_int(rec.get("experiment_sequence"))
            if seq is None:
                continue
            seq_counts[seq] = seq_counts.get(seq, 0) + 1
    except Exception as e:
        st.sidebar.error(f"Failed to stream 'readings': {e}")
        raise FirestoreUnavailable(f"Failed to stream readings: {e}")

    # Debug line so we can verify we're seeing docs
    try:
        st.sidebar.caption(
            f"scanned readings docs: {scanned} ({ROOT_COLLECTION}/{STATION_DOC}/{READINGS_SUB})"
        )
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
    Load every reading where experiment_sequence == selected sequence from:
      watercapture / watercapture@ASU / readings
    """
    import streamlit as st
    db = _init_db()
    seq = _parse_seq(exp_id)
    rows: List[Dict[str, Any]] = []

    # Resolve and check parent doc
    parent = db.collection(ROOT_COLLECTION).document(STATION_DOC)
    try:
        ps = parent.get()
        if not ps.exists:
            msg = f"Doc not found: {ROOT_COLLECTION}/{STATION_DOC}"
            st.sidebar.error(msg)
            raise FirestoreUnavailable(msg)
    except Exception as e:
        st.sidebar.error(f"Path resolve error: {e}")
        raise FirestoreUnavailable(f"Path resolve error: {e}")

    # Query by experiment_sequence
    try:
        q = parent.collection(READINGS_SUB).where("experiment_sequence", "==", seq).stream()
        cnt = 0
        for snap in q:
            cnt += 1
            rows.append(_row_from_reading(snap.to_dict() or {}, station_hint=STATION_DOC))
        try:
            st.sidebar.caption(f"loaded rows for seq {seq}: {cnt}")
        except Exception:
            pass
    except Exception as e:
        st.sidebar.error(f"Query failed for seq {seq}: {e}")
        raise FirestoreUnavailable(f"Query failed: {e}")

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
