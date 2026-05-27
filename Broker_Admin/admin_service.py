#!/usr/bin/env python3
"""
admin_service.py

Flask + SQLite admin service that:
- stores topic requests and status (pending/approved/active)
- stores consumer registrations and consumer-topic subscriptions
- exposes endpoints for admin actions and for producers/consumers to poll
- provides fraud detection monitoring endpoints

Run: python admin_service.py
"""

import os
import sys
import datetime
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, Column, Integer, String, Boolean, MetaData, Table, DateTime, ForeignKey, Float, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
import traceback

from flask_cors import CORS
import json
from kafka import KafkaAdminClient

# ── Import centralized config ────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import (
    KAFKA_BROKER, ADMIN_HOST, ADMIN_PORT, ADMIN_TOKEN,
    DB_PATH, DB_DIR
)

app = Flask(__name__)
from functools import wraps

def require_admin(f):
    """Decorator to protect admin-only endpoints with a simple token."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        token = request.headers.get("X-Admin-Token")
        if token != ADMIN_TOKEN:
            return jsonify({"error": "unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper


@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-Admin-Token'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, DELETE'
    return response

@app.route('/<path:path>', methods=['OPTIONS'])
def handle_options(path):
    return ('', 204)


# ── Database Setup ────────────────────────────────────────────────────
# Ensure DB directory exists
DB_DIR.mkdir(parents=True, exist_ok=True)

engine = create_engine(f'sqlite:///{DB_PATH}', echo=False, connect_args={"check_same_thread": False})
metadata = MetaData()

# Topics table: status can be 'pending', 'approved', 'active'
topics_table = Table('topics', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, unique=True, nullable=False),
    Column('description', String, default=''),
    Column('status', String, default='pending')  # 'pending', 'approved', 'active'
)

# Consumer subscriptions table
consumer_subscriptions_table = Table('consumer_subscriptions', metadata,
    Column('id', Integer, primary_key=True),
    Column('consumer_id', String, nullable=False),
    Column('topic_name', String, nullable=False),
    Column('subscribed_at', DateTime, default=datetime.datetime.utcnow)
)

# Messages table (for audit/UI)
messages_table = Table('messages', metadata,
    Column('id', Integer, primary_key=True),
    Column('topic', String, nullable=False),
    Column('payload', String, nullable=False),   # JSON string
    Column('ts', DateTime, default=datetime.datetime.utcnow)
)

# ── Fraud Detection Tables ────────────────────────────────────────────
fraud_alerts_table = Table('fraud_alerts', metadata,
    Column('id', Integer, primary_key=True),
    Column('transaction_id', String, nullable=False),
    Column('user_id', String),
    Column('amount', Float),
    Column('fraud_score', Float),
    Column('is_fraud', Boolean, default=False),
    Column('model_scores', Text),        # JSON: {"isolation_forest": 0.8, "xgboost": 0.9, "autoencoder": 0.7}
    Column('explanation', Text),          # JSON: SHAP top features
    Column('feedback', String),           # 'confirmed_fraud', 'false_positive', or None
    Column('created_at', DateTime, default=datetime.datetime.utcnow)
)

fraud_stats_table = Table('fraud_stats', metadata,
    Column('id', Integer, primary_key=True),
    Column('total_transactions', Integer, default=0),
    Column('total_fraud_detected', Integer, default=0),
    Column('total_false_positives', Integer, default=0),
    Column('total_confirmed_fraud', Integer, default=0),
    Column('updated_at', DateTime, default=datetime.datetime.utcnow)
)

model_metrics_table = Table('model_metrics', metadata,
    Column('id', Integer, primary_key=True),
    Column('model_name', String, nullable=False),
    Column('precision_val', Float),
    Column('recall_val', Float),
    Column('f1_val', Float),
    Column('auprc', Float),
    Column('avg_latency_ms', Float),
    Column('recorded_at', DateTime, default=datetime.datetime.utcnow)
)

metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Initialize fraud stats row if not exists
def _init_fraud_stats():
    s = Session()
    row = s.query(fraud_stats_table).first()
    if not row:
        s.execute(fraud_stats_table.insert().values(
            total_transactions=0, total_fraud_detected=0,
            total_false_positives=0, total_confirmed_fraud=0,
            updated_at=datetime.datetime.utcnow()
        ))
        s.commit()
    s.close()

_init_fraud_stats()


def topic_row_to_dict(r):
    return {
        'id': r.id,
        'name': r.name,
        'description': r.description,
        'status': r.status
    }

def consumer_row_to_dict(r):
    return {
        'id': r.id,
        'name': r.name,
        'description': r.description,
        'created_at': r.created_at.isoformat() if r.created_at else None
    }

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

#
# Topic endpoints
#
@app.route('/topics', methods=['GET'])
def list_topics():
    """Return all topics (any status)."""
    s = Session()
    rows = s.query(topics_table).all()
    result = [topic_row_to_dict(r) for r in rows]
    s.close()
    return jsonify(result)

@app.route('/topics/pending', methods=['GET'])
def list_pending_topics():
    """Return only topics where status == 'pending'."""
    s = Session()
    rows = s.query(topics_table).filter_by(status='pending').all()
    result = [{'id': r.id, 'name': r.name, 'description': r.description} for r in rows]
    s.close()
    return jsonify(result)

@app.route('/topics/active', methods=['GET'])
def list_active_topics():
    """Return only topics where status == 'active'."""
    s = Session()
    rows = s.query(topics_table).filter_by(status='active').all()
    result = [{'id': r.id, 'name': r.name, 'description': r.description} for r in rows]
    s.close()
    return jsonify(result)

@app.route('/topics/approved', methods=['GET'])
def list_approved_topics():
    """Return only topics where status == 'approved'."""
    s = Session()
    rows = s.query(topics_table).filter_by(status='approved').all()
    result = [{'id': r.id, 'name': r.name, 'description': r.description} for r in rows]
    s.close()
    return jsonify(result)

@app.route('/topics', methods=['POST'])
def create_topic_request():
    """
    Create a pending topic request.
    payload: {"name":"topic-name","description":"..."}
    """
    data = request.json or {}
    name = data.get('name')
    desc = data.get('description', '')
    if not name or not isinstance(name, str) or name.strip() == '':
        return jsonify({'error': 'name required'}), 400
    name = name.strip()
    s = Session()
    try:
        ins = topics_table.insert().values(name=name, description=desc, status='pending')
        s.execute(ins)
        s.commit()
    except IntegrityError:
        s.rollback()
        s.close()
        return jsonify({'error': 'topic already exists'}), 400
    s.close()
    return jsonify({'status': 'created', 'name': name}), 201

@app.route('/topics/<string:name>/approve', methods=['POST'])
def approve_topic(name):
    """Set topic status to 'approved'."""
    s = Session()
    row = s.query(topics_table).filter_by(name=name).first()
    if not row:
        s.close()
        return jsonify({'error': 'not found'}), 404
    upd = topics_table.update().where(topics_table.c.name == name).values(status='approved')
    s.execute(upd)
    s.commit()
    s.close()
    return jsonify({'status': 'approved', 'name': name})

# --- Reject (DELETE) endpoint: removes topic + subscriptions + messages ---
@app.route('/topics/<string:name>/reject', methods=['POST', 'DELETE'])
@require_admin
def reject_topic(name):
    """
    Reject a pending topic request by deleting it from the DB.
    Also delete subscriptions referencing the topic,
    and optionally messages for that topic in messages table.
    """
    s = Session()
    # ensure topic exists
    row = s.query(topics_table).filter_by(name=name).first()
    if not row:
        s.close()
        return jsonify({'error': 'not found'}), 404

    # Only allow delete if it's pending
    if (row.status or '').lower() != 'pending':
        s.close()
        return jsonify({'error': 'invalid_status', 'message': 'only pending topics can be rejected'}), 400

    # Delete subscriptions referencing the topic
    try:
        if 'consumer_subscriptions' in metadata.tables:
            del_q = consumer_subscriptions_table.delete().where(consumer_subscriptions_table.c.topic_name == name)
            s.execute(del_q)
    except Exception:
        pass

    # Delete messages
    try:
        if 'messages' in metadata.tables:
            del_msgs = messages_table.delete().where(messages_table.c.topic == name)
            s.execute(del_msgs)
    except Exception:
        pass

    # Finally delete the topic row
    s.execute(topics_table.delete().where(topics_table.c.name == name))
    s.commit()
    s.close()

    return jsonify({'status': 'deleted', 'name': name})


@app.route('/topics/<string:name>/activate', methods=['POST'])
def activate_topic(name):
    """
    Activate a topic (set status = 'active') in the Admin DB.
    """
    try:
        s = Session()
        row = s.query(topics_table).filter_by(name=name).first()

        if not row:
            s.close()
            return jsonify({'error': f'Topic {name} not found'}), 404

        # Update status
        upd = topics_table.update().where(topics_table.c.name == name).values(status='active')
        s.execute(upd)
        s.commit()
        s.close()

        print(f"[Admin] Activated topic: {name}")
        return jsonify({'status': 'active', 'name': name}), 200

    except Exception as e:
        print(f"[Admin] Error activating topic {name}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/topics/<string:name>/revoke', methods=['POST'])
@require_admin
def revoke_topic(name):
    s = Session()
    try:
        # 1) ensure topic exists
        row = s.query(topics_table).filter_by(name=name).first()
        if not row:
            s.close()
            return jsonify({'error': 'not found'}), 404

        prev_status = (row.status or '').lower()

        # 2) update status -> pending
        s.execute(topics_table.update().where(topics_table.c.name == name).values(status='pending'))
        s.commit()

        # 3) delete subscriptions
        rows_deleted = None
        try:
            with engine.begin() as conn:
                delete_stmt = text("DELETE FROM consumer_subscriptions WHERE topic_name = :t")
                result = conn.execute(delete_stmt, {"t": name})
                try:
                    rows_deleted = result.rowcount
                except Exception:
                    rows_deleted = None
        except Exception as e:
            err_msg = str(e)
            s.close()
            return jsonify({'error': 'failed_delete_subscriptions', 'message': err_msg}), 500

        s.commit()
        s.close()
        return jsonify({
            'status': 'revoked',
            'name': name,
            'prev_status': prev_status,
            'rows_deleted': rows_deleted
        }), 200

    except Exception as e:
        s.rollback()
        tb = str(e)
        s.close()
        return jsonify({'error': 'internal_error', 'message': tb}), 500


@app.route('/topics/<string:name>/status', methods=['GET'])
def get_topic_status(name):
    s = Session()
    row = s.query(topics_table).filter_by(name=name).first()
    if not row:
        s.close()
        return jsonify({'error': 'not found'}), 404
    result = topic_row_to_dict(row)
    s.close()
    return jsonify(result)
    
@app.route('/topics/<string:name>/ingest', methods=['POST'])
def ingest_message(name):
    """
    Ingest a message copy from a producer (for central audit/UI).
    Body: {"payload": <json-serializable-object>}
    """
    data = request.json or {}
    payload = data.get('payload')
    if payload is None:
        return jsonify({'error': 'payload required'}), 400
    try:
        s = Session()
        # store payload as JSON string
        s.execute(messages_table.insert().values(topic=name, payload=json.dumps(payload), ts=datetime.datetime.utcnow()))
        s.commit()
        s.close()
        return jsonify({'status': 'ingested', 'topic': name}), 201
    except Exception as e:
        try:
            s.rollback()
            s.close()
        except Exception:
            pass
        return jsonify({'error': 'failed', 'message': str(e)}), 500

@app.route('/topics/<string:name>/recent', methods=['GET'])
def recent_messages(name):
    """Return last 100 messages for <name>."""
    try:
        s = Session()
        rows = s.query(messages_table).filter_by(topic=name).order_by(messages_table.c.id.desc()).limit(100).all()
        res = []
        for r in rows:
            try:
                payload = json.loads(r.payload)
            except Exception:
                payload = r.payload
            res.append({'id': r.id, 'topic': r.topic, 'payload': payload, 'ts': r.ts.isoformat() if r.ts else None})
        s.close()
        return jsonify(res)
    except Exception as e:
        return jsonify({'error': 'failed', 'message': str(e)}), 500
        
@app.route('/broker/health', methods=['GET'])
def broker_health():
    """
    Quick health check of Kafka broker connection.
    Returns topic list count if reachable.
    """
    try:
        admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER], client_id="admin_health", request_timeout_ms=2000)
        topics = admin.list_topics()
        admin.close()
        return jsonify({'status': 'ok', 'topic_count': len(topics)})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


#
# Consumer & subscription endpoints
#
@app.route('/subscribe', methods=['POST'])
def subscribe_consumer():
    """
    Subscribe a consumer to a topic.
    Body: {"consumer_id":"consumer-1", "topic_name":"topic-cpu"}
    """
    data = request.json or {}
    consumer_id = data.get('consumer_id')
    topic_name = data.get('topic_name')

    if not consumer_id or not topic_name:
        return jsonify({'error': 'consumer_id and topic_name required'}), 400

    s = Session()
    # Ensure topic exists
    topic_row = s.query(topics_table).filter_by(name=topic_name).first()
    if not topic_row:
        s.close()
        return jsonify({'error': 'topic not found'}), 404

    ins = consumer_subscriptions_table.insert().values(
        consumer_id=consumer_id,
        topic_name=topic_name,
        subscribed_at=datetime.datetime.utcnow()
    )
    s.execute(ins)
    s.commit()
    s.close()
    return jsonify({'status': 'subscribed', 'consumer_id': consumer_id, 'topic_name': topic_name})

@app.route('/unsubscribe', methods=['POST'])
def unsubscribe_consumer():
    """
    Unsubscribe a consumer from a topic.
    Body: {"consumer_id":"consumer-1", "topic_name":"topic-cpu"}
    """
    data = request.json or {}
    consumer_id = data.get('consumer_id')
    topic_name = data.get('topic_name')

    if not consumer_id or not topic_name:
        return jsonify({'error': 'consumer_id and topic_name required'}), 400

    s = Session()
    delete_q = consumer_subscriptions_table.delete().where(
        (consumer_subscriptions_table.c.consumer_id == consumer_id) &
        (consumer_subscriptions_table.c.topic_name == topic_name)
    )
    s.execute(delete_q)
    s.commit()
    s.close()
    return jsonify({'status': 'unsubscribed', 'consumer_id': consumer_id, 'topic_name': topic_name})

@app.route('/subscriptions', methods=['GET'])
def list_all_subscriptions():
    """
    List all consumer-topic subscriptions.
    """
    s = Session()
    rows = s.query(consumer_subscriptions_table).all()
    result = [
        {
            'id': r.id,
            'consumer_id': r.consumer_id,
            'topic_name': r.topic_name,
            'subscribed_at': r.subscribed_at.isoformat() if r.subscribed_at else None
        }
        for r in rows
    ]
    s.close()
    return jsonify(result)

@app.route('/subscriptions/<string:consumer_id>', methods=['GET'])
def list_subscriptions_for_consumer(consumer_id):
    s = Session()
    rows = s.query(consumer_subscriptions_table).filter_by(consumer_id=consumer_id).all()
    result = [{'topic_name': r.topic_name, 'subscribed_at': r.subscribed_at.isoformat()} for r in rows]
    s.close()
    return jsonify(result)
    

@app.route('/topics/rejected', methods=['GET'])
def list_rejected_topics():
    s = Session()
    rows = s.query(topics_table).filter_by(status='rejected').all()
    result = [{'id': r.id, 'name': r.name, 'description': r.description} for r in rows]
    s.close()
    return jsonify(result)


# ══════════════════════════════════════════════════════════════════════
# FRAUD DETECTION ENDPOINTS
# ══════════════════════════════════════════════════════════════════════

@app.route('/fraud/stats', methods=['GET'])
def get_fraud_stats():
    """Return overall fraud detection statistics."""
    s = Session()
    row = s.query(fraud_stats_table).first()
    if not row:
        s.close()
        return jsonify({
            'total_transactions': 0,
            'total_fraud_detected': 0,
            'total_false_positives': 0,
            'total_confirmed_fraud': 0
        })
    result = {
        'total_transactions': row.total_transactions,
        'total_fraud_detected': row.total_fraud_detected,
        'total_false_positives': row.total_false_positives,
        'total_confirmed_fraud': row.total_confirmed_fraud,
        'updated_at': row.updated_at.isoformat() if row.updated_at else None
    }
    s.close()
    return jsonify(result)

@app.route('/fraud/stats', methods=['POST'])
def update_fraud_stats():
    """Increment fraud stats counters. Body: {"transactions": 1, "fraud_detected": 0}"""
    data = request.json or {}
    s = Session()
    row = s.query(fraud_stats_table).first()
    if row:
        updates = {'updated_at': datetime.datetime.utcnow()}
        if 'transactions' in data:
            updates['total_transactions'] = row.total_transactions + data['transactions']
        if 'fraud_detected' in data:
            updates['total_fraud_detected'] = row.total_fraud_detected + data['fraud_detected']
        if 'false_positives' in data:
            updates['total_false_positives'] = row.total_false_positives + data['false_positives']
        if 'confirmed_fraud' in data:
            updates['total_confirmed_fraud'] = row.total_confirmed_fraud + data['confirmed_fraud']
        s.execute(fraud_stats_table.update().where(fraud_stats_table.c.id == row.id).values(**updates))
    s.commit()
    s.close()
    return jsonify({'status': 'updated'})

@app.route('/fraud/alerts', methods=['GET'])
def get_fraud_alerts():
    """Return recent fraud alerts (last 100) with SHAP explanations."""
    limit = request.args.get('limit', 100, type=int)
    s = Session()
    rows = s.query(fraud_alerts_table).order_by(fraud_alerts_table.c.id.desc()).limit(limit).all()
    result = []
    for r in rows:
        alert = {
            'id': r.id,
            'transaction_id': r.transaction_id,
            'user_id': r.user_id,
            'amount': r.amount,
            'fraud_score': r.fraud_score,
            'is_fraud': r.is_fraud,
            'feedback': r.feedback,
            'created_at': r.created_at.isoformat() if r.created_at else None
        }
        try:
            alert['model_scores'] = json.loads(r.model_scores) if r.model_scores else None
        except Exception:
            alert['model_scores'] = None
        try:
            alert['explanation'] = json.loads(r.explanation) if r.explanation else None
        except Exception:
            alert['explanation'] = None
        result.append(alert)
    s.close()
    return jsonify(result)

@app.route('/fraud/alerts', methods=['POST'])
def create_fraud_alert():
    """Store a new fraud alert. Called by model server."""
    data = request.json or {}
    s = Session()
    s.execute(fraud_alerts_table.insert().values(
        transaction_id=data.get('transaction_id', ''),
        user_id=data.get('user_id'),
        amount=data.get('amount'),
        fraud_score=data.get('fraud_score'),
        is_fraud=data.get('is_fraud', False),
        model_scores=json.dumps(data.get('model_scores', {})),
        explanation=json.dumps(data.get('explanation', {})),
        feedback=None,
        created_at=datetime.datetime.utcnow()
    ))
    s.commit()
    s.close()
    return jsonify({'status': 'alert_created'}), 201

@app.route('/fraud/feedback', methods=['POST'])
def submit_fraud_feedback():
    """
    Human feedback on a fraud alert.
    Body: {"alert_id": 42, "feedback": "confirmed_fraud"|"false_positive"}
    """
    data = request.json or {}
    alert_id = data.get('alert_id')
    feedback = data.get('feedback')
    
    if not alert_id or feedback not in ('confirmed_fraud', 'false_positive'):
        return jsonify({'error': 'alert_id and feedback (confirmed_fraud/false_positive) required'}), 400

    s = Session()
    row = s.query(fraud_alerts_table).filter_by(id=alert_id).first()
    if not row:
        s.close()
        return jsonify({'error': 'alert not found'}), 404
    
    s.execute(fraud_alerts_table.update().where(
        fraud_alerts_table.c.id == alert_id
    ).values(feedback=feedback))
    
    # Update stats
    stats_row = s.query(fraud_stats_table).first()
    if stats_row:
        if feedback == 'confirmed_fraud':
            s.execute(fraud_stats_table.update().where(
                fraud_stats_table.c.id == stats_row.id
            ).values(total_confirmed_fraud=stats_row.total_confirmed_fraud + 1))
        elif feedback == 'false_positive':
            s.execute(fraud_stats_table.update().where(
                fraud_stats_table.c.id == stats_row.id
            ).values(total_false_positives=stats_row.total_false_positives + 1))
    
    s.commit()
    s.close()
    return jsonify({'status': 'feedback_recorded', 'alert_id': alert_id, 'feedback': feedback})

@app.route('/model/metrics', methods=['GET'])
def get_model_metrics():
    """Return latest model performance metrics."""
    s = Session()
    rows = s.query(model_metrics_table).order_by(model_metrics_table.c.id.desc()).limit(10).all()
    result = [
        {
            'model_name': r.model_name,
            'precision': r.precision_val,
            'recall': r.recall_val,
            'f1': r.f1_val,
            'auprc': r.auprc,
            'avg_latency_ms': r.avg_latency_ms,
            'recorded_at': r.recorded_at.isoformat() if r.recorded_at else None
        }
        for r in rows
    ]
    s.close()
    return jsonify(result)

@app.route('/model/metrics', methods=['POST'])
def record_model_metrics():
    """Record model performance metrics. Called by model server."""
    data = request.json or {}
    s = Session()
    s.execute(model_metrics_table.insert().values(
        model_name=data.get('model_name', 'ensemble'),
        precision_val=data.get('precision'),
        recall_val=data.get('recall'),
        f1_val=data.get('f1'),
        auprc=data.get('auprc'),
        avg_latency_ms=data.get('avg_latency_ms'),
        recorded_at=datetime.datetime.utcnow()
    ))
    s.commit()
    s.close()
    return jsonify({'status': 'metrics_recorded'}), 201


#
# Run
#
if __name__ == '__main__':
    print(f"Admin service starting. DB: {DB_PATH}")
    print(f"Kafka broker: {KAFKA_BROKER}")
    print(f"Listening on {ADMIN_HOST}:{ADMIN_PORT}")
    app.run(host=ADMIN_HOST, port=ADMIN_PORT, debug=False)
