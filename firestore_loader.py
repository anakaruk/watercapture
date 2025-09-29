# firestore_loader.py (adapted to your current Firestore structure)
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

from google.cloud import firestore

@dataclass
class FirestoreUnavailable(Exception):
    user_msg: str

_db = None

def _init_db():
    global _db
    if _db is None:
        try:
            _db = firestore.Client()
        except Exception as e:
            raise FirestoreUnavailable(f"Firestore init failed: {e}")
    return _db

def get_active_experiment():
    # Not applicable for your current structure
    return None

def list_experiments(limit: int = 5) -> List[Dict[str, Any]]:
    """Fake experiment list: return unique stations or sequences."""
    db = _init_db()
    docs = db.collection("readings").limit(200).stream()
    seen = {}
    for d in docs:
        rec = d.to_dict()
        seq = rec.get("experiment_sequence", 0)
        key = f"exp_{seq}"
        if key not in seen:
            seen[key] = {"id": key, "station": rec.get("station")}
    return list(seen.values())

def load_experiment_data(exp_id: str, realtime: bool = False) -> pd.DataFrame:
    """Load all readings for a given experiment_sequence."""
    db = _init_db()
    try:
        # exp_id is like "exp_2"
        seq = int(exp_id.split("_")[1])
        docs = db.collection("readings").where("experiment_sequence", "==", seq).stream()
        rows = []
        for snap in docs:
            d = snap.to_dict()
            rows.append({
                "weight": d.get("weight"),
                "date": d.get("date"),
                "time": d.get("time"),
                "experimental_runtime": d.get("experiment_runtime"),
                "experimental_run_number": d.get("experiment_sequence"),
                "station": d.get("station"),
            })
        return pd.DataFrame(rows)
    except Exception as e:
        raise FirestoreUnavailable(f"Failed to load readings: {e}")
