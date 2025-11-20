# streamlit_admin.py
import os
import streamlit as st
import requests
import pandas as pd
from typing import Optional


# -----------------------
# CONFIG - safe secrets fallback
# -----------------------
try:
    ADMIN_BASE = st.secrets.get("ADMIN_BASE", "") or "http://10.147.19.93:5000"
    DEFAULT_ADMIN_TOKEN = st.secrets.get("ADMIN_TOKEN", "") or ""
except Exception:
    ADMIN_BASE = os.environ.get("ADMIN_BASE", "http://10.147.19.93:5000")
    DEFAULT_ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

TIMEOUT = 6  # seconds

# --- Smart auto-refresh helpers (paste near top of file) ---
if "refresh_key" not in st.session_state:
    st.session_state["refresh_key"] = 0

def trigger_refresh():
    """Increment key to trigger UI rerun."""
    st.session_state["refresh_key"] += 1

# Watcher: if key changed, rerun once and reset
if st.session_state.get("refresh_key", 0) > 0:
    st.session_state["refresh_key"] = 0
    st.rerun()


# Safe rerun helper — works whether or not streamlit has experimental_rerun
# Safe rerun helper — supports new Streamlit API (no deprecation warnings)
def safe_rerun():
    try:
        st.experimental_rerun()
    except Exception:
        try:
            import random
            # Use new query_params API (preferred)
            st.query_params["_reload"] = str(random.random())
        except Exception:
            try:
                st.session_state['_reload_key'] = st.session_state.get('_reload_key', 0) + 1
            except Exception:
                pass




# -----------------------
# Helpers: HTTP wrappers returning structured results
# -----------------------
def build_headers(token: Optional[str]):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Admin-Token"] = token
    return headers

def get_json(path: str, token: Optional[str] = None):
    """
    GET ADMIN_BASE + path
    Returns: { ok: True, data: <json> } or { ok: False, error: str, status_code: int or None, text: str or None }
    """
    url = f"{ADMIN_BASE}{path}"
    try:
        r = requests.get(url, headers=build_headers(token), timeout=TIMEOUT)
        try:
            text = r.text
        except Exception:
            text = None
        if 200 <= r.status_code < 300:
            try:
                return {"ok": True, "data": r.json(), "status_code": r.status_code, "text": text}
            except Exception:
                # non-json body but success
                return {"ok": True, "data": None, "status_code": r.status_code, "text": text}
        else:
            return {"ok": False, "error": f"HTTP {r.status_code}", "status_code": r.status_code, "text": text}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "error": str(e), "status_code": None, "text": None}

def post_json(path: str, token: Optional[str] = None, payload: Optional[dict] = None):
    """
    POST ADMIN_BASE + path with optional JSON payload.
    Returns: { ok: True, resp: <requests.Response> } or { ok: False, error: str, status_code: int or None, text: str or None }
    """
    url = f"{ADMIN_BASE}{path}"
    try:
        r = requests.post(url, json=payload, headers=build_headers(token), timeout=TIMEOUT)
        try:
            text = r.text
        except Exception:
            text = None
        if 200 <= r.status_code < 300:
            return {"ok": True, "resp": r, "status_code": r.status_code, "text": text}
        else:
            return {"ok": False, "error": f"HTTP {r.status_code}", "status_code": r.status_code, "text": text}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "error": str(e), "status_code": None, "text": None}

# Convenience small wrappers that return .get('data') on success or show error
def fetch_pending_topics(token: Optional[str]):
    return get_json("/topics/pending", token)

def fetch_all_topics(token: Optional[str]):
    return get_json("/topics", token)

def fetch_subscriptions(token: Optional[str]):
    return get_json("/subscriptions", token)

# -----------------------
# UI: Streamlit layout + styles
# -----------------------
st.set_page_config(page_title="Admin Dashboard", layout="wide")

st.markdown("""
<style>
.section {
  background: #f9f9f9;
  border-radius: 12px;
  padding: 18px;
  margin-bottom: 18px;
  box-shadow: 0 6px 18px rgba(0,0,0,0.05);
}
.topic-name { font-weight: 600; font-size: 1.05rem; color: #222; }
.status-badge { font-weight: 600; padding: 6px 10px; border-radius: 8px; background: #eee; font-size: 0.9rem; }
.approve-btn { padding:6px 12px; border-radius:8px; color:white; background:linear-gradient(135deg,#667eea 0%,#764ba2 100%); border:none; }
.reject-btn { padding:6px 12px; border-radius:8px; color:white; background:linear-gradient(135deg,#ff4b2b 0%,#ff416c 100%); border:none; }
.revoke-btn { padding:6px 12px; border-radius:8px; color:black; background:linear-gradient(135deg,#ffb347 0%,#ffcc33 100%); border:none; }
.small-note { font-size:0.9rem; color:#666; }
</style>
""", unsafe_allow_html=True)

st.title("📊 Admin Dashboard — Kafka Topic Control")

# Sidebar settings
with st.sidebar:
    st.header("⚙️ Settings")
    admin_token = st.text_input("Admin Token (X-Admin-Token)", value=DEFAULT_ADMIN_TOKEN, type="password")
    base = st.text_input("Admin Base URL", value=ADMIN_BASE)
    if base != ADMIN_BASE:
        ADMIN_BASE = base
    refresh_interval = st.slider("Auto-refresh (seconds)", min_value=0, max_value=60, value=0, step=5)
    st.markdown("<div class='small-note'>Tip: use correct ADMIN_BASE (ZeroTier IP) and ADMIN_TOKEN if server requires auth.</div>", unsafe_allow_html=True)
    if st.button("Force Refresh"):
        trigger_refresh()


# -----------------------
# Pending topics (Approve + Reject per-row)
# -----------------------
st.markdown("<div class='section'><h2>Pending Topic Requests</h2>", unsafe_allow_html=True)
pending_res = fetch_pending_topics(admin_token)
if not pending_res["ok"]:
    st.error(f"Failed to fetch pending topics: {pending_res.get('error')} (status={pending_res.get('status_code')}) Text: {pending_res.get('text')}")
    pending = []
else:
    pending = pending_res.get("data") or []

if not pending:
    st.info("No pending topic creation requests.")
else:
    for p in pending:
        # columns: name | status | actions
        c_name, c_status, c_action = st.columns([4, 1, 3])
        c_name.markdown(f"<div class='topic-name'>{p.get('name')}</div>", unsafe_allow_html=True)
        c_status.markdown(f"<div class='status-badge'>{p.get('status','pending')}</div>", unsafe_allow_html=True)

        # actions: two small columns inside action column
        a_col, b_col = c_action.columns(2)

        if a_col.button("✅ Approve", key=f"approve_{p.get('name')}"):
            res = post_json(f"/topics/{p.get('name')}/approve", admin_token)
            if not res["ok"]:
                st.error(f"Approve failed: {res.get('error')} status={res.get('status_code')} text={res.get('text')}")
            else:
                st.success(f"✅ Approved {p.get('name')}")
                trigger_refresh()


        if b_col.button("❌ Reject", key=f"reject_{p.get('name')}"):
            res = post_json(f"/topics/{p.get('name')}/reject", admin_token)
            if not res["ok"]:
                st.error(f"Reject failed: {res.get('error')} status={res.get('status_code')} text={res.get('text')}")
            else:
                st.warning(f"❌ Rejected {p.get('name')}")
                trigger_refresh()


st.markdown("</div>", unsafe_allow_html=True)

# -----------------------
# Current topics (only approved/active)
# -----------------------
st.markdown("<div class='section'><h2>Current Topics</h2>", unsafe_allow_html=True)
topics_res = fetch_all_topics(admin_token)
if not topics_res["ok"]:
    st.error(f"Failed to fetch topics: {topics_res.get('error')} status={topics_res.get('status_code')} text={topics_res.get('text')}")
    topics = []
else:
    topics = topics_res.get("data") or []

# Filter: only show topics that are not 'pending' (i.e., approved or active)
filtered_topics = []
for t in topics:
    st_status = (t.get('status') or '').lower()
    if st_status in ('pending', 'rejected'):
        continue
    filtered_topics.append(t)

if not filtered_topics:
    st.info("No approved/active topics found.")
else:
    for t in filtered_topics:
        c_name, c_status, c_action = st.columns([4, 1, 3])
        c_name.markdown(f"<div class='topic-name'>{t.get('name')}</div>", unsafe_allow_html=True)
        status_val = (t.get('status') or "").lower()
        if status_val == "active":
            badge_html = f"<div class='status-badge' style='background:#c8e6c9'>{t.get('status')}</div>"
        elif status_val == "approved":
            badge_html = f"<div class='status-badge' style='background:#bbdefb'>{t.get('status')}</div>"
        else:
            badge_html = f"<div class='status-badge'>{t.get('status')}</div>"
        c_status.markdown(badge_html, unsafe_allow_html=True)

        if c_action.button("🔁 Revoke", key=f"revoke_{t.get('name')}"):
            res = post_json(f"/topics/{t.get('name')}/revoke", admin_token, {"status": "pending"})
            if not res["ok"]:
                st.error(f"Revoke failed: {res.get('error')} status={res.get('status_code')} text={res.get('text')}")
            else:
                st.warning(f"🔁 Revoked {t.get('name')} → pending")
                trigger_refresh()

st.markdown("</div>", unsafe_allow_html=True)


# -----------------------
# Consumer <-> Topic mappings
# -----------------------
st.markdown("<div class='section'><h2>Consumer ↔ Topic Mappings</h2>", unsafe_allow_html=True)
subs_res = fetch_subscriptions(admin_token)
if not subs_res["ok"]:
    st.info("Subscriptions endpoint not available or returned error.")
    if subs_res.get("error"):
        st.error(f"Subscriptions fetch error: {subs_res.get('error')} status={subs_res.get('status_code')} text={subs_res.get('text')}")
else:
    subs = subs_res.get("data") or []
    if subs:
        df = pd.DataFrame(subs)
        st.dataframe(df)
    else:
        st.info("No subscriptions found.")
st.markdown("</div>", unsafe_allow_html=True)

# -----------------------
# Auto-refresh support
# -----------------------
if refresh_interval > 0:
    import time
    time.sleep(refresh_interval)
    trigger_refresh()


