def _init_db():
    """Create Firestore client using service account JSON from secrets."""
    global _db
    if _db is not None:
        return _db

    try:
        from google.cloud import firestore
        from google.oauth2.service_account import Credentials
        import streamlit as st
    except Exception as e:
        raise FirestoreUnavailable(
            f"Firestore library import failed: {e}. "
            "Add `google-cloud-firestore` and `google-auth` to requirements.txt."
        )

    try:
        sa_info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa_info)
        project_id = st.secrets.get("gcp_project") or sa_info.get("project_id")
        if not project_id:
            raise FirestoreUnavailable("No project_id found in secrets.")

        _db = firestore.Client(project=project_id, credentials=creds)

        # --- Debug: confirm connection ---
        st.sidebar.success(f"Connected to Firestore project: {project_id}")

        return _db
    except Exception as e:
        raise FirestoreUnavailable(f"Firestore init failed: {e}")
