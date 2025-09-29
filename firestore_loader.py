# firestore_loader.py
# Path:
#   watercapture / watercapture@ASU / readings / <reading_doc>
#
# Quiet (no sidebar noise) + live experiment detection.

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Iterable, Tuple
import pandas as pd
import streamlit as st

DEFAULT_ROOT = "watercapture"
DEFAULT_DOC  = "watercapture@ASU"
SUBCOL       = "readings"

# Toggle sidebar debug via secrets if you want: debug_sidebar = true
DEBUG_SIDEBAR = bool(st.secrets.get("debug_sidebar", False))

def _sb_caption(msg: str):
    if DEBUG_SIDEBAR:
        try:
            st.sidebar.caption(msg)
        except Exception:
            pass

def _sb_info(msg: str):
    if DEBUG_SIDEBAR:
        try:
            st.sidebar.info(msg)
        except Exception:
            pass

def _sb_error(msg: str):
    if DEBUG_SIDEBAR:
        try:
            st.sidebar.error(msg)
        except Exception:
            pass

@dataclass
class FirestoreUnavailable(Exception):
    user_msg: str

@st.cache_resource(show_spinner=False)
def _init_db():
    try:
        from google.cloud import firestore
        from google.oauth2.service_account import Credentials
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
        db = firestore.Client(project=project_id, credentials=creds)
        _sb_info(f"Connected to Firestore: {project_id}")
        return db
    except KeyError:
        raise FirestoreUnavailable("Missing `gcp_service_account` in Streamlit secrets.")
    except Exception as e:
        raise FirestoreUnavailable(f"Firestore init failed: {e}")

@st.cache_data(ttl=60, show_spinner=False)
def _resolve_parent_path() -> Tuple[str, str]:
    db = _init_db()
    root = st.secrets.get("root_collection", DEFAULT_ROOT)
    doc  = st.secrets.get("station_doc", DEFAULT_DOC)

    try:
        _ = db.collection(root).document(doc).collection(SUBCOL).limit(1).get()
        _sb_caption(f"Using path: {root}/{doc}/{SUBCOL}")
        return (root, doc)
    except Exception:
        pass

    try:
        col = db.collection(root)
        for dref in col.list_documents(page_size=200):
            try:
                _ = dref.collection(SUBCOL).limit(1).get()
                _sb_caption(f"Using path: {root}/{dref.id}/{SUBCOL}")
                return (root, dref.id)
            except Exception:
                continue
        raise FirestoreUnavailable(
            f"No document with '{SUBCOL}' subcollection found under '{root}'."
        )
    except Exception as e:
        raise FirestoreUnavailable(f"Path resolve error: {e}")

def _safe_int(v) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

def _parse_seq(exp_id: str | int) -> int:
    try:
        return int(str(exp_id).split("_")[-1])
    except Exception:
        raise FirestoreUnavailable(f"Bad experiment id: {exp_id}")

def _combine_date_time(date_val, time_val) -> Optional[pd.Timestamp]:
    if pd.isna(date_val) and pd.isna(time_val):
        return None
    try:
        if pd.notna(date_val) and pd.notna(time_val):
            return pd.to_datetime(f"{date_val} {time_val}", errors="coerce")
        if pd.notna(date_val):
            return pd.to_datetime(date_val, errors="coerce")
        if pd.notna(time_val):
            today = pd.Timestamp.today().normalize()
            t = pd.to_datetime(str(time_val), errors="coerce")
            if pd.isna(t):
                return None
            return pd.Timestamp(today.year, today.month, today.day,
                                t.hour, t.minute, t.second, t.microsecond)
    except Exception:
        return None
    return None

def _row_from_reading(d: Dict[str, Any], station_hint: Optional[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "weight": d.get("weight"),
        "date": d.get("date"),
        "time": d.get("time"),
        "experimental_runtime": d.get("experiment_runtime"),
        "experimental_run_number": d.get("experiment_sequence"),
        "station": d.get("station") or station_hint,
    }
    for k, v in d.items():
        if k not in out:
            out[k] = v
    if "timestamp" not in out or out.get("timestamp") in (None, ""):
        out["timestamp"] = _combine_date_time(out.get("date"), out.get("time"))
    else:
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    return out

def _order_columns(df: pd.DataFrame) -> pd.DataFrame:
    prefer = [
        "timestamp",
        "weight", "date", "time",
        "experimental_runtime",
        "experimental_run_number",
        "station",
    ]
    exist = [c for c in prefer if c in df.columns]
    rest  = [c for c in df.columns if c not in exist]
    return df[exist + rest]

def get_active_experiment(live_window_s: int = 300) -> Optional[Dict[str, Any]]:
    """
    Heuristic 'live' detector:
    - Find latest experiment_sequence present.
    - Load its rows, compute latest timestamp (from date+time or 'timestamp').
    - If latest timestamp is within live_window_s seconds of now -> treat as live.
    Returns: {'id': 'exp_<seq>', 'sequence': seq, 'live': True} or None.
    """
    db = _init_db()
    root, doc = _resolve_parent_path()

    # Gather sequences
    seqs: List[int] = []
    try:
        for snap in db.collection(root).document(doc).collection(SUBCOL).stream():
            seq = _safe_int((snap.to_dict() or {}).get("experiment_sequence"))
            if seq is not None:
                seqs.append(seq)
    except Exception as e:
        _sb_error(f"Failed to scan for live: {e}")
        return None
    if not seqs:
        return None

    latest_seq = max(seqs)

    # Fetch rows for latest seq and compute latest timestamp
    try:
        q = db.collection(root).document(doc).collection(SUBCOL) \
              .where("experiment_sequence", "==", latest_seq)
        snaps = list(q.stream())
        ts_vals: List[pd.Timestamp] = []
        for s in snaps:
            row = _row_from_reading(s.to_dict() or {}, station_hint=doc)
            ts_vals.append(pd.to_datetime(row.get("timestamp"), errors="coerce"))
        if not ts_vals:
            return None
        latest_ts = pd.Series(ts_vals).max()
        if pd.isna(latest_ts):
            return None
        # Compare with now
        now = pd.Timestamp.utcnow()
        live = (now - latest_ts.tz_localize(None)).total_seconds() <= live_window_s
        if live:
            return {"id": f"exp_{latest_seq}", "sequence": latest_seq, "live": True}
        return None
    except Exception:
        return None

@st.cache_data(ttl=60, show_spinner=False)
def list_experiments(limit: int = 200) -> List[Dict[str, Any]]:
    db = _init_db()
    root, doc = _resolve_parent_path()

    seq_counts: Dict[int, int] = {}
    scanned = 0
    try:
        for snap in db.collection(root).document(doc).collection(SUBCOL).stream():
            scanned += 1
            rec = snap.to_dict() or {}
            seq = _safe_int(rec.get("experiment_sequence"))
            if seq is None:
                continue
            seq_counts[seq] = seq_counts.get(seq, 0) + 1
        _sb_caption(f"scanned readings: {scanned}  ({root}/{doc}/{SUBCOL})")
    except Exception as e:
        _sb_error(f"Failed to stream readings: {e}")
        raise FirestoreUnavailable(f"Failed to stream readings: {e}")

    items = [
        {"id": f"exp_{seq}", "sequence": seq, "count": seq_counts[seq]}
        for seq in sorted(seq_counts.keys())
    ]
    if limit and len(items) > limit:
        items = items[:limit]
    return items

@st.cache_data(ttl=10, show_spinner=False)  # short TTL helps Live refresh
def load_experiment_data(
    exp_id: str | int,
    *,
    fields: Optional[Iterable[str]] = None,
    order: str = "asc",
    limit: Optional[int] = None,
    realtime: bool = False,   # tolerated for backwards compatibility
    **_ignored,
) -> pd.DataFrame:
    db = _init_db()
    root, doc = _resolve_parent_path()
    seq = _parse_seq(exp_id)

    rows: List[Dict[str, Any]] = []
    try:
        q = db.collection(root).document(doc).collection(SUBCOL) \
              .where("experiment_sequence", "==", seq)
        snaps = list(q.stream())
        cnt = 0
        for s in snaps:
            cnt += 1
            row = _row_from_reading(s.to_dict() or {}, station_hint=doc)
            if fields is not None:
                keep = set(fields) | {
                    "timestamp",
                    "weight", "date", "time",
                    "experimental_runtime", "experimental_run_number",
                    "station",
                }
                row = {k: v for k, v in row.items() if k in keep}
            rows.append(row)
        _sb_caption(f"loaded rows for seq {seq}: {cnt}")
    except Exception as e:
        _sb_error(f"Query failed for seq {seq}: {e}")
        raise FirestoreUnavailable(f"Query failed: {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    if "experimental_runtime" in df.columns:
        try:
            df["experimental_runtime"] = pd.to_timedelta(df["experimental_runtime"])
        except Exception:
            pass

    if "timestamp" in df.columns:
        df = df.sort_values("timestamp", ascending=(order == "asc"))
    else:
        df = df.sort_index(ascending=(order == "asc"))

    if isinstance(limit, int) and limit > 0:
        df = df.head(limit) if order == "asc" else df.tail(limit)

    return _order_columns(df).reset_index(drop=True)

def load_latest_experiment(
    *,
    fields: Optional[Iterable[str]] = None,
    order: str = "asc",
    limit: Optional[int] = None,
) -> pd.DataFrame:
    exps = list_experiments()
    if not exps:
        return pd.DataFrame()
    latest = exps[-1]["id"]  # sequences sorted ascending
    return load_experiment_data(latest, fields=fields, order=order, limit=limit)
