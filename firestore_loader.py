# firestore_loader.py
# Project: watercapture
# Auto-detect Firestore layout and load by experiment_sequence.
#
# Supports BOTH:
#   (A) watercapture / watercapture@ASU / readings / *
#   (B) watercapture@ASU / <any-doc> / readings / *
#
# You can force a layout via secrets:
#   root_collection = "watercapture@ASU"  # or "watercapture"
#   station_doc     = "watercapture@ASU"  # used only for layout (A)

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Iterable, Tuple
import pandas as pd
import streamlit as st

# ---------- Errors ----------
@dataclass
class FirestoreUnavailable(Exception):
    user_msg: str

# ---------- Firestore client ----------
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
        try:
            st.sidebar.info(f"Connected to Firestore: {project_id}")
        except Exception:
            pass
        return db
    except KeyError:
        raise FirestoreUnavailable("Missing `gcp_service_account` in Streamlit secrets.")
    except Exception as e:
        raise FirestoreUnavailable(f"Firestore init failed: {e}")

# ---------- Layout detection ----------
@st.cache_data(ttl=60, show_spinner=False)
def _resolve_parent_path() -> Tuple[str, str]:
    """
    Returns (root_collection, station_doc) and guarantees that
    <root_collection>/<station_doc>/readings exists (non-empty or at least accessible).

    Tries, in order:
      A) root="watercapture", doc="watercapture@ASU"
      B) root="watercapture@ASU", doc="<first doc that has readings>"
    Can be forced via secrets: root_collection, station_doc.
    """
    db = _init_db()

    # 1) If user provided explicit path in secrets, try that first.
    root_override = st.secrets.get("root_collection")
    station_override = st.secrets.get("station_doc")
    if root_override and station_override:
        parent = db.collection(root_override).document(station_override)
        if parent.get().exists:
            return (root_override, station_override)
        else:
            st.sidebar.error(f"Override path not found: {root_override}/{station_override}")

    # 2) Try layout (A): watercapture / watercapture@ASU
    rootA, docA = "watercapture", st.secrets.get("station_doc") or "watercapture@ASU"
    parentA = db.collection(rootA).document(docA)
    try:
        snapA = parentA.get()
        if snapA.exists:
            # If readings subcollection is reachable, accept.
            _ = parentA.collection("readings").limit(1).get()
            st.sidebar.caption(f"Using path: {rootA}/{docA}/readings")
            return (rootA, docA)
    except Exception:
        pass  # fall through to (B)

    # 3) Try layout (B): watercapture@ASU / <any-doc>
    rootB = "watercapture@ASU"
    if root_override:
        rootB = root_override  # allow forcing collection name only

    try:
        col_ref = db.collection(rootB)
        # choose first doc that has 'readings' subcollection (or the only doc)
        candidates = list(col_ref.list_documents(page_size=100))
        found = None
        for doc_ref in candidates:
            try:
                # Touch doc; must exist
                if doc_ref.get().exists:
                    # Make sure readings subcollection is reachable
                    _ = doc_ref.collection("readings").limit(1).get()
                    found = doc_ref.id
                    break
            except Exception:
                continue
        if found:
            st.sidebar.caption(f"Using path: {rootB}/{found}/readings")
            return (rootB, found)
        else:
            raise FirestoreUnavailable(
                f"No usable doc with 'readings' under collection '{rootB}'."
            )
    except Exception as e:
        raise FirestoreUnavailable(
            f"Path resolve error: could not find a valid parent. Details: {e}"
        )

# ---------- Helpers ----------
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
        elif pd.notna(date_val):
            return pd.to_datetime(date_val, errors="coerce")
        elif pd.notna(time_val):
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
    rest = [c for c in df.columns if c not in exist]
    return df[exist + rest]

# ---------- Public API ----------
def get_active_experiment() -> Optional[Dict[str, Any]]:
    """Historical-only dataset (no live flag)."""
    return None

@st.cache_data(ttl=60, show_spinner=False)
def list_experiments(limit: int = 200) -> List[Dict[str, Any]]:
    db = _init_db()
    root, doc = _resolve_parent_path()

    seq_counts: Dict[int, int] = {}
    scanned = 0
    try:
        for snap in db.collection(root).document(doc).collection("readings").stream():
            scanned += 1
            rec = snap.to_dict() or {}
            seq = _safe_int(rec.get("experiment_sequence"))
            if seq is None:
                continue
            seq_counts[seq] = seq_counts.get(seq, 0) + 1
        st.sidebar.caption(f"scanned readings: {scanned}  ({root}/{doc}/readings)")
    except Exception as e:
        st.sidebar.error(f"Failed to stream readings: {e}")
        raise FirestoreUnavailable(f"Failed to stream readings: {e}")

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
    db = _init_db()
    root, doc = _resolve_parent_path()
    seq = _parse_seq(exp_id)

    rows: List[Dict[str, Any]] = []
    try:
        q = db.collection(root).document(doc).collection("readings").where(
            "experiment_sequence", "==", seq
        )
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
        st.sidebar.caption(f"loaded rows for seq {seq}: {cnt}")
    except Exception as e:
        st.sidebar.error(f"Query failed for seq {seq}: {e}")
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
    latest = exps[-1]["id"]  # sequences are sorted ascending
    return load_experiment_data(latest, fields=fields, order=order, limit=limit)
