# firestore_loader.py
# For project "watercapture"
# Data layout supported:
#   1) Preferred (your current):  watercapture@ASU/<station_doc>/readings/<reading_doc>
#   2) Fallback (optional):       readings/<reading_doc>

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd

# ===== Public error type ======================================================
@dataclass
class FirestoreUnavailable(Exception):
    user_msg: str

# ===== Config (edit here if your root collection name changes) ===============
STATIONS_COLLECTION = "watercapture@ASU"   # root collection holding station docs
READINGS_SUBCOLLECTION = "readings"        # subcollection inside each station doc
TOPLEVEL_READINGS = "readings"             # optional fallback top-level collection

# ===== Internal globals =======================================================
_db = None
_project_id_cached: Optional[str] = None


# ===== Firestore init (lazy) =================================================
def _init_db():
    """
    Create a Firestore client from Streamlit secrets.
    Requires Streamlit secrets to contain:
      gcp_project = "watercapture"
      [gcp_service_account]  # full JSON fields
    """
    global _db, _project_id_cached
    if _db is not None:
        return _db

    try:
        # Lazy imports so importing this module never crashes
        from google.cloud import firestore
        from google.oauth2.service_account import Credentials
        import streamlit as st
    except Exception as e:
        raise FirestoreUnavailable(
            "Missing libraries. Ensure requirements include: "
            "`google-cloud-firestore`, `google-auth`, `streamlit`, `pandas`. "
            f"Details: {e}"
        )

    try:
        sa_info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa_info)
        project_id = st.secrets.get("gcp_project") or sa_info.get("project_id")
        if not project_id:
            raise FirestoreUnavailable("No project id found in Streamlit secrets.")

        _project_id_cached = project_id
        _db = firestore.Client(project=project_id, credentials=creds)

        # Small status in sidebar helps sanity-check
        try:
            st.sidebar.info(f"Connected to Firestore: {project_id}")
        except Exception:
            pass  # sidebar not available during tests

        return _db
    except FirestoreUnavailable:
        raise
    except Exception as e:
        raise FirestoreUnavailable(f"Firestore init failed: {e}")


# ===== Public API expected by dashboard.py ===================================
def get_active_experiment() -> Optional[Dict[str, Any]]:
    """
    With the current flat readings structure we don't have an 'active run' doc.
    Return None so dashboard switches to historical mode.
    """
    return None


def list_experiments(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Return unique experiment sequences discovered from readings.
    Looks inside STATIONS_COLLECTION/*/READINGS_SUBCOLLECTION first.
    If that path doesn't exist, falls back to TOPLEVEL_READINGS.
    """
    db = _init_db()
    try:
        seen: Dict[int, Dict[str, Any]] = {}

        # ---- Path A: station docs with readings subcollections
        stations_iter = db.collection(STATIONS_COLLECTION).stream()
        any_station = False
        for station_doc in stations_iter:
            any_station = True
            station_id = station_doc.id
            readings_iter = (
                station_doc.reference
                .collection(READINGS_SUBCOLLECTION)
                .limit(500)
                .stream()
            )
            for snap in readings_iter:
                rec = snap.to_dict() or {}
                seq = _safe_int(rec.get("experiment_sequence"))
                if seq is None:
                    continue
                if seq not in seen:
                    seen[seq] = {
                        "id": f"exp_{seq}",
                        "station": rec.get("station") or station_id,
                        "station_doc": station_id,
                    }

        # ---- Path B: fallback to top-level 'readings'
        if not any_station:
            for snap in db.collection(TOPLEVEL_READINGS).limit(500).stream():
                rec = snap.to_dict() or {}
                seq = _safe_int(rec.get("experiment_sequence"))
                if seq is None:
                    continue
                if seq not in seen:
                    seen[seq] = {
                        "id": f"exp_{seq}",
                        "station": rec.get("station"),
                        "station_doc": None,
                    }

        # Sort by sequence id, newest last (change if you prefer)
        out = [seen[k] for k in sorted(seen.keys())]
        if limit and len(out) > limit:
            out = out[:limit]
        return out
    except Exception as e:
        raise FirestoreUnavailable(f"List experiments failed: {e}")


def load_experiment_data(exp_id: str, realtime: bool = False) -> pd.DataFrame:
    """
    Load all readings for a given experiment_sequence across all stations.

    Returns DataFrame with (at least):
      weight, date, time, experimental_runtime, experimental_run_number, station
    """
    db = _init_db()
    try:
        seq = _parse_seq(exp_id)
        rows = []

        # ---- Path A: station docs with readings subcollections
        stations_iter = db.collection(STATIONS_COLLECTION).stream()
        have_station = False
        for station_doc in stations_iter:
            have_station = True
            station_id = station_doc.id
            q = (
                station_doc.reference
                .collection(READINGS_SUBCOLLECTION)
                .where("experiment_sequence", "==", seq)
                .stream()
            )
            for snap in q:
                d = snap.to_dict() or {}
                rows.append(_row_from_reading(d, station_id))

        # ---- Path B: fallback to top-level 'readings'
        if not have_station:
            q = db.collection(TOPLEVEL_READINGS).where("experiment_sequence", "==", seq).stream()
            for snap in q:
                d = snap.to_dict() or {}
                rows.append(_row_from_reading(d, d.get("station")))

        df = pd.DataFrame(rows)
        # Keep column order nice if present
        prefer = [
            "weight", "date", "time",
            "experimental_runtime", "experimental_run_number",
            "station",
        ]
        if not df.empty:
            ordered = [c for c in prefer if c in df.columns] + [c for c in df.columns if c not in prefer]
            df = df[ordered]
        return df
    except Exception as e:
        raise FirestoreUnavailable(f"Load data failed: {e}")


# ===== Small helpers ==========================================================
def _safe_int(v) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

def _parse_seq(exp_id: str) -> int:
    # Accept "exp_2" or "2"
    try:
        return int(exp_id.split("_")[-1])
    except Exception:
        raise FirestoreUnavailable(f"Bad experiment id: {exp_id}")

def _row_from_reading(d: Dict[str, Any], station_id: Optional[str]) -> Dict[str, Any]:
    return {
        "weight": d.get("weight"),
        "date": d.get("date"),
        "time": d.get("time"),
        "experimental_runtime": d.get("experiment_runtime"),
        "experimental_run_number": d.get("experiment_sequence"),
        "station": d.get("station") or station_id,
        # include any extra fields automatically
        **{k: v for k, v in d.items() if k not in {
            "weight", "date", "time", "experiment_runtime",
            "experiment_sequence", "station"
        }},
    }
