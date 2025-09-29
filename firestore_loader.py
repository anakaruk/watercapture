# ---- NEW/UPDATED: use experiment_sequence as experiments ----

def list_experiments(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Return experiments as unique experiment_sequence values.
    Each item: { id: "exp_<seq>", sequence: <int>, count: <int>, station: <str|None> }
    """
    db = _init_db()
    seq_counts: Dict[int, int] = {}
    seq_station: Dict[int, str] = {}

    # scan all station docs under watercapture@ASU
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

    # (optional) also look at a top-level readings collection if you ever use it
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
    Load all readings for a given experiment_sequence across stations.
    Returns DataFrame with at least:
      weight, date, time, experimental_runtime, experimental_run_number, station
    """
    db = _init_db()
    seq = _parse_seq(exp_id)
    rows = []

    # scan station documents
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

    # fallback top-level readings
    try:
        for snap in db.collection(TOPLEVEL_READINGS).where("experiment_sequence", "==", seq).stream():
            d = snap.to_dict() or {}
            rows.append(_row_from_reading(d, station_hint=d.get("station")))
    except Exception:
        pass

    df = pd.DataFrame(rows)

    # Make runtime plottable (HH:MM:SS -> timedelta)
    if "experimental_runtime" in df.columns:
        try:
            df["experimental_runtime"] = pd.to_timedelta(df["experimental_runtime"])
        except Exception:
            pass

    prefer = [
        "weight", "date", "time",
        "experimental_runtime", "experimental_run_number", "station"
    ]
    if not df.empty:
        ordered = [c for c in prefer if c in df.columns] + [c for c in df.columns if c not in prefer]
        df = df[ordered]
    return df


def get_active_experiment() -> Optional[Dict[str, Any]]:
    """
    Heuristic: newest sequence is 'active' if its newest reading is within the last 10 minutes.
    (We only have date+time strings, so we parse them.)
    """
    import datetime as _dt

    exps = list_experiments(limit=1000)
    if not exps:
        return None

    # newest sequence by number
    newest = max(exps, key=lambda e: e["sequence"])
    seq = newest["sequence"]

    df = load_experiment_data(f"exp_{seq}")
    if df.empty:
        return None

    # build a datetime from date+time strings if available
    ts = None
    if "date" in df.columns and "time" in df.columns:
        try:
            ts = pd.to_datetime(df["date"] + " " + df["time"]).max()
        except Exception:
            ts = None

    if ts is not None:
        now = pd.Timestamp.utcnow()
        # consider 'active' if we have a reading in the last 10 minutes
        if (now - ts.tz_localize(None)) <= pd.Timedelta(minutes=10):
            return {"id": f"exp_{seq}", "sequence": seq}

    return None
