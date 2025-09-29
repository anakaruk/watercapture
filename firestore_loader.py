# firestore_loader.py
# Project: watercapture
# Canonical Firestore path:
#   watercapture / watercapture@ASU / readings / <reading_doc>
#
# Experiments are defined ONLY by `experiment_sequence`.
# One fixed station. Historical-only (no live-run detection).

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Iterable
from datetime import datetime
import pandas as pd

import streamlit as st

# ---------- Config: fixed path ----------
ROOT_COLLECTION = "watercapture"          # top-level collection
STATION_DOC     = "watercapture@ASU"      # single (fixed) station document
READINGS_SUB    = "readings"              # subcollection with reading docs


# ---------- Errors ----------
@dataclass
class FirestoreUnavailable(Exception):
    user_msg: str


# ---------- Firestore client ----------
@st.cache_resource(show_spinner=False)
def _init_db():
    """
    Create Firestore client from Streamlit Secrets (gcp_service_account).
    Requires: google-cloud-firestore, google-auth.
    """
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

        # Best-effort sidebar hint
        try:
            st.sidebar.info(f"Connected to Firestore: {project_id}")
        except Exception:
            pass

        return db
    except KeyError:
        raise FirestoreUnavailable(
            "Missing `gcp_service_account` in Streamlit secrets."
        )
    except Exception as e:
        raise FirestoreUnavailable(f"Firestore init failed: {e}")


# ---------- Helpers ----------
def _safe_int(v) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None


def _parse_seq(exp_id: str | int) -> int:
    """
    Accepts: 'exp_3', '3', 3 -> 3
    """
    try:
        return int(str(exp_id).split("_")[-1])
    except Exception:
        raise FirestoreUnavailable(f"Bad experiment id: {exp_id}")


def _combine_date_time(date_val: Any, time_val: Any) -> Optional[pd.Timestamp]:
    """
    Make a timezone-naive Timestamp from separate 'date' & 'time' fields if present.
    If already ISO strings, pandas will handle.
    """
    if pd.isna(date_val) and pd.isna(time_val):
        return None
    try:
        if pd.notna(date_val) and pd.notna(time_val):
            # Common formats:
            #   date: '2025-09-24' or '09/24/2025'
            #   time: '14:23:15' or '14:23:15.123'
            return pd.to_datetime(f"{date_val} {time_val}", errors="coerce")
        elif pd.notna(date_val):
            return pd.to_datetime(date_val, errors="coerce")
        elif pd.notna(time_val):
            # Use today's date with provided time; still useful for plotting sequences
            today = pd.Timestamp.today().normalize()
            t = pd.to_datetime(str(time_val), errors="coerce")
            if pd.isna(t):
                return None
            return pd.Timestamp(
                year=today.year, month=today.month, day=today.day,
                hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond
            )
    except Exception:
        return None
    return None


def _row_from_reading(d: Dict[str, Any], station_hint: Optional[str]) -> Dict[str, Any]:
    """
    Normalize a reading into a consistent row (keep extras).
    Adds a synthetic 'timestamp' column from date+time if possible.
    """
    out: Dict[str, Any] = {
        "weight": d.get("weight"),
        "date": d.get("date"),
        "time": d.get("time"),
        "experimental_runtime": d.get("experiment_runtime"),
        "experimental_run_number": d.get("experiment_sequence"),
        "station": d.get("station") or station_hint or STATION_DOC,
    }
    # Preserve all other fields
    for k, v in d.items():
        if k not in out:
            out[k] = v

    # Construct timestamp if not present
    if "timestamp" not in out or out.get("timestamp") in (None, ""):
        ts = _combine_date_time(out.get("date"), out.get("time"))
        out["timestamp"] = ts
    else:
        # Coerce to pandas Timestamp if present
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
    existing = [c for c in prefer if c in df.columns]
    rest = [c for c in df.columns if c not in existing]
    return df[existing + rest]


# ---------- Public API ----------
def get_active_experiment() -> Optional[Dict[str, Any]]:
    """No live/run flag in this dataset; keep dashboard in historical mode."""
    return None


@st.cache_data(ttl=60, show_spinner=False)
def list_experiments(limit: int = 200) -> List[Dict[str, Any]]:
    """
    Experiments = unique experiment_sequence values inside:
      watercapture / watercapture@ASU / readings / *
    Returns: [{id:'exp_<seq>', sequence:int, count:int}, ...]
    """
    db = _init_db()

    seq_counts: Dict[int, int] = {}
    scanned = 0

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

    try:
        st.sidebar.caption(
            f"scanned readings: {scanned}  ({ROOT_COLLECTION}/{STATION_DOC}/{READINGS_SUB})"
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


@st.cache_data(ttl=120, show_spinner=False)
def load_experiment_data(
    exp_id: str | int,
    *,
    fields: Optional[Iterable[str]] = None,
    order: str = "asc",
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Load every reading where experiment_sequence == selected sequence from:
      watercapture / watercapture@ASU / readings

    Args:
        exp_id: 'exp_3', '3', or 3
        fields: optional iterable of field names to project (we always keep core columns)
        order: 'asc' | 'desc' (sorted by synthetic/real 'timestamp' if present, else by index)
        limit: optional row cap

    Returns:
        Pandas DataFrame with normalized columns + all original fields.
    """
    db = _init_db()
    seq = _parse_seq(exp_id)
    rows: List[Dict[str, Any]] = []

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
        q = parent.collection(READINGS_SUB).where("experiment_sequence", "==", seq)

        # NOTE: Firestore requires order_by on a field you filter for range/ordering.
        # We don't have a native 'timestamp' field guaranteed, so we stream and sort client-side.
        # If your docs DO include a Firestore 'timestamp', you can uncomment to server-side order:
        # q = q.order_by("timestamp", direction=firestore.Query.ASCENDING)

        snaps = list(q.stream())
        cnt = 0
        for snap in snaps:
            cnt += 1
            d = snap.to_dict() or {}
            row = _row_from_reading(d, station_hint=STATION_DOC)

            # Project fields if requested (but always keep core columns)
            if fields is not None:
                keep = set(fields) | {
                    "timestamp",
                    "weight", "date", "time",
                    "experimental_runtime", "experimental_run_number",
                    "station",
                }
                row = {k: v for k, v in row.items() if k in keep}

            rows.append(row)

        try:
            st.sidebar.caption(f"loaded rows for seq {seq}: {cnt}")
        except Exception:
            pass
    except Exception as e:
        st.sidebar.error(f"Query failed for seq {seq}: {e}")
        raise FirestoreUnavailable(f"Query failed: {e}")

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # Parse experimental_runtime to Timedelta for plotting
    if "experimental_runtime" in df.columns:
        try:
            df["experimental_runtime"] = pd.to_timedelta(df["experimental_runtime"])
        except Exception:
            pass

    # Sort
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp", ascending=(order == "asc"))
    else:
        df = df.sort_index(ascending=(order == "asc"))

    # Limit
    if isinstance(limit, int) and limit > 0:
        df = df.head(limit) if order == "asc" else df.tail(limit)

    return _order_columns(df).reset_index(drop=True)


# ---------- Convenience ----------
def load_latest_experiment(
    *,
    fields: Optional[Iterable[str]] = None,
    order: str = "asc",
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Pick the experiment with the highest sequence number and load it.
    """
    exps = list_experiments()
    if not exps:
        return pd.DataFrame()
    latest = exps[-1]["id"]  # sorted ascending by sequence
    return load_experiment_data(latest, fields=fields, order=order, limit=limit)
