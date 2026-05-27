# streamlit_admin.py
import os
import sys
import streamlit as st
import requests
import pandas as pd
from typing import Optional

# ── Import centralized config ────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import ADMIN_URL, ADMIN_TOKEN as DEFAULT_ADMIN_TOKEN_VALUE

# -----------------------
# CONFIG
# -----------------------
try:
    ADMIN_BASE = st.secrets.get("ADMIN_BASE", "") or ADMIN_URL
    DEFAULT_ADMIN_TOKEN = st.secrets.get("ADMIN_TOKEN", "") or DEFAULT_ADMIN_TOKEN_VALUE
except Exception:
    ADMIN_BASE = os.environ.get("ADMIN_BASE", ADMIN_URL)
    DEFAULT_ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", DEFAULT_ADMIN_TOKEN_VALUE)

TIMEOUT = 6  # seconds

# --- Smart auto-refresh helpers ---
if "refresh_key" not in st.session_state:
    st.session_state["refresh_key"] = 0

def trigger_refresh():
    st.session_state["refresh_key"] += 1

if st.session_state.get("refresh_key", 0) > 0:
    st.session_state["refresh_key"] = 0
    st.rerun()

def safe_rerun():
    try:
        st.rerun()
    except Exception:
        pass


# -----------------------
# Helpers: HTTP wrappers
# -----------------------
def build_headers(token: Optional[str]):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Admin-Token"] = token
    return headers

def get_json(path: str, token: Optional[str] = None):
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
                return {"ok": True, "data": None, "status_code": r.status_code, "text": text}
        else:
            return {"ok": False, "error": f"HTTP {r.status_code}", "status_code": r.status_code, "text": text}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "error": str(e), "status_code": None, "text": None}

def post_json(path: str, token: Optional[str] = None, payload: Optional[dict] = None):
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

def fetch_pending_topics(token):
    return get_json("/topics/pending", token)

def fetch_all_topics(token):
    return get_json("/topics", token)

def fetch_subscriptions(token):
    return get_json("/subscriptions", token)


# -----------------------
# UI Setup
# -----------------------
st.set_page_config(
    page_title="Admin Dashboard — Fraud Detection",
    page_icon="🛡️",
    layout="wide"
)

# ── Theme-aware CSS ───────────────────────────────────────────────────
st.markdown("""
<style>
    /* Status badges */
    .badge-active {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
        background: #1b5e20;
        color: #a5d6a7;
    }
    .badge-approved {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
        background: #0d47a1;
        color: #90caf9;
    }
    .badge-pending {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
        background: #e65100;
        color: #ffcc80;
    }
    /* Metric cards */
    div[data-testid="stMetric"] {
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 12px;
        padding: 12px;
        background: rgba(255,255,255,0.03);
    }
    div[data-testid="stMetric"] label {
        font-size: 0.9rem !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }
</style>
""", unsafe_allow_html=True)

st.title("🛡️ Admin Dashboard — Kafka Topic & Fraud Control")

# Sidebar settings
with st.sidebar:
    st.header("⚙️ Settings")
    admin_token = st.text_input("Admin Token", value=DEFAULT_ADMIN_TOKEN, type="password")
    base = st.text_input("Admin Base URL", value=ADMIN_BASE)
    if base != ADMIN_BASE:
        ADMIN_BASE = base
    refresh_interval = st.slider("Auto-refresh (seconds)", min_value=0, max_value=60, value=0, step=5)
    st.caption("💡 Use correct ADMIN_BASE and ADMIN_TOKEN if server requires auth.")
    if st.button("🔄 Force Refresh"):
        trigger_refresh()

# ── Tab-based layout ──────────────────────────────────────────────────
tab_topics, tab_fraud, tab_model = st.tabs(["📋 Topic Management", "🚨 Fraud Monitoring", "📈 Model Performance"])


# ═══════════════════════════════════════════════════════════════════════
# TAB 1: Topic Management
# ═══════════════════════════════════════════════════════════════════════
with tab_topics:

    # ── Pending Topics ─────────────────────────────────────────────────
    st.subheader("⏳ Pending Topic Requests")
    pending_res = fetch_pending_topics(admin_token)
    if not pending_res["ok"]:
        st.error(f"Failed to fetch pending topics: {pending_res.get('error')}")
        pending = []
    else:
        pending = pending_res.get("data") or []

    if not pending:
        st.info("No pending topic creation requests.")
    else:
        for p in pending:
            col_name, col_action = st.columns([5, 3])
            with col_name:
                st.markdown(f"**{p.get('name')}** — {p.get('description', '')}")
                st.markdown(f'<span class="badge-pending">pending</span>', unsafe_allow_html=True)
            with col_action:
                c1, c2 = st.columns(2)
                if c1.button("✅ Approve", key=f"approve_{p.get('name')}"):
                    res = post_json(f"/topics/{p.get('name')}/approve", admin_token)
                    if res["ok"]:
                        st.success(f"Approved: {p.get('name')}")
                        trigger_refresh()
                    else:
                        st.error(f"Failed: {res.get('error')}")
                if c2.button("❌ Reject", key=f"reject_{p.get('name')}"):
                    res = post_json(f"/topics/{p.get('name')}/reject", admin_token)
                    if res["ok"]:
                        st.warning(f"Rejected: {p.get('name')}")
                        trigger_refresh()
                    else:
                        st.error(f"Failed: {res.get('error')}")

    st.divider()

    # ── Current Topics ─────────────────────────────────────────────────
    st.subheader("📌 Active & Approved Topics")
    topics_res = fetch_all_topics(admin_token)
    if not topics_res["ok"]:
        st.error(f"Failed to fetch topics: {topics_res.get('error')}")
        topics = []
    else:
        topics = topics_res.get("data") or []

    filtered_topics = [t for t in topics if (t.get('status') or '').lower() not in ('pending', 'rejected')]

    if not filtered_topics:
        st.info("No approved/active topics found.")
    else:
        for t in filtered_topics:
            col_name, col_status, col_action = st.columns([4, 2, 2])
            with col_name:
                st.markdown(f"**{t.get('name')}**")
            with col_status:
                status = (t.get('status') or '').lower()
                if status == 'active':
                    st.markdown('<span class="badge-active">● active</span>', unsafe_allow_html=True)
                elif status == 'approved':
                    st.markdown('<span class="badge-approved">● approved</span>', unsafe_allow_html=True)
                else:
                    st.text(t.get('status', ''))
            with col_action:
                if st.button("🔁 Revoke", key=f"revoke_{t.get('name')}"):
                    res = post_json(f"/topics/{t.get('name')}/revoke", admin_token, {"status": "pending"})
                    if res["ok"]:
                        st.warning(f"Revoked: {t.get('name')} → pending")
                        trigger_refresh()
                    else:
                        st.error(f"Failed: {res.get('error')}")

    st.divider()

    # ── Consumer Subscriptions ─────────────────────────────────────────
    st.subheader("🔗 Consumer ↔ Topic Subscriptions")
    subs_res = fetch_subscriptions(admin_token)
    if not subs_res["ok"]:
        st.info("No subscription data available.")
    else:
        subs = subs_res.get("data") or []
        if subs:
            df = pd.DataFrame(subs)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No subscriptions found.")


# ═══════════════════════════════════════════════════════════════════════
# TAB 2: Fraud Monitoring
# ═══════════════════════════════════════════════════════════════════════
with tab_fraud:

    st.subheader("🚨 Fraud Detection Overview")

    stats_res = get_json("/fraud/stats", admin_token)
    if stats_res["ok"] and stats_res.get("data"):
        stats = stats_res["data"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📊 Total Transactions", f"{stats.get('total_transactions', 0):,}")
        c2.metric("🔴 Fraud Detected", f"{stats.get('total_fraud_detected', 0):,}")
        c3.metric("✅ Confirmed Fraud", f"{stats.get('total_confirmed_fraud', 0):,}")
        c4.metric("⚠️ False Positives", f"{stats.get('total_false_positives', 0):,}")

        if stats.get('updated_at'):
            st.caption(f"Last updated: {stats['updated_at']}")
    else:
        st.info("No fraud stats available yet. Start the pipeline to begin processing.")

    st.divider()

    # ── Recent Fraud Alerts ────────────────────────────────────────────
    st.subheader("📋 Recent Fraud Alerts")
    alerts_res = get_json("/fraud/alerts?limit=20", admin_token)
    if alerts_res["ok"] and alerts_res.get("data"):
        alerts = alerts_res["data"]
        if alerts:
            df_alerts = pd.DataFrame(alerts)
            display_cols = [c for c in ['id', 'transaction_id', 'user_id', 'amount', 'fraud_score', 'is_fraud', 'feedback', 'created_at'] if c in df_alerts.columns]
            st.dataframe(df_alerts[display_cols], use_container_width=True, height=400)
        else:
            st.info("No fraud alerts yet. Alerts will appear once the model server is running.")
    else:
        st.info("No fraud alerts available. Deploy models and start the inference pipeline first.")

    st.divider()

    # ── Kafka Broker Health ────────────────────────────────────────────
    st.subheader("💚 Kafka Broker Health")
    broker_res = get_json("/broker/health", admin_token)
    if broker_res["ok"] and broker_res.get("data"):
        data = broker_res["data"]
        if data.get("status") == "ok":
            st.success(f"Kafka is healthy — {data.get('topic_count', 0)} topic(s) in cluster")
        else:
            st.error(f"Kafka issue: {data.get('message', 'unknown')}")
    else:
        st.warning("Cannot reach Kafka via admin service.")


# ═══════════════════════════════════════════════════════════════════════
# TAB 3: Model Performance
# ═══════════════════════════════════════════════════════════════════════
with tab_model:

    st.subheader("📈 Model Performance Metrics")

    metrics_res = get_json("/model/metrics", admin_token)
    if metrics_res["ok"] and metrics_res.get("data"):
        metrics = metrics_res["data"]
        if metrics:
            # Show latest metrics as cards
            latest = metrics[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Precision", f"{latest.get('precision', 0):.4f}" if latest.get('precision') else "—")
            c2.metric("Recall", f"{latest.get('recall', 0):.4f}" if latest.get('recall') else "—")
            c3.metric("F1 Score", f"{latest.get('f1', 0):.4f}" if latest.get('f1') else "—")
            c4.metric("AUPRC", f"{latest.get('auprc', 0):.4f}" if latest.get('auprc') else "—")

            st.divider()

            # Show history table
            st.subheader("📊 Metrics History")
            df_metrics = pd.DataFrame(metrics)
            st.dataframe(df_metrics, use_container_width=True)
        else:
            st.info("No model metrics recorded yet. Train and deploy models first.")
    else:
        st.info("No model metrics available. Train models (Phase 2) and start the model server (Phase 4).")

    st.divider()

    # ── Service Status ─────────────────────────────────────────────────
    st.subheader("🔌 Service Status")
    col1, col2, col3 = st.columns(3)

    with col1:
        try:
            r = requests.get(f"{ADMIN_BASE}/health", timeout=2)
            if r.status_code == 200:
                st.success("Admin Service ✅")
            else:
                st.error("Admin Service ❌")
        except Exception:
            st.error("Admin Service ❌")

    with col2:
        try:
            r = requests.get("http://localhost:8000/health", timeout=2)
            if r.status_code == 200:
                data = r.json()
                loaded = sum(1 for v in data.get('models', {}).values() if v)
                st.success(f"Model Server ✅ ({loaded} models)")
            else:
                st.warning("Model Server ⚠️")
        except Exception:
            st.warning("Model Server — not running")

    with col3:
        broker_res = get_json("/broker/health", admin_token)
        if broker_res["ok"] and broker_res.get("data", {}).get("status") == "ok":
            st.success("Kafka Broker ✅")
        else:
            st.error("Kafka Broker ❌")


# -----------------------
# Auto-refresh
# -----------------------
if refresh_interval > 0:
    import time
    time.sleep(refresh_interval)
    trigger_refresh()
