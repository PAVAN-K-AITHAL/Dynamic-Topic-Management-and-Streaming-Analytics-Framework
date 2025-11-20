#!/usr/bin/env python3
"""
admin_service.py

Flask + SQLite admin service that:
- stores topic requests and status (pending/approved/active)
- stores consumer registrations and consumer-topic subscriptions
- exposes endpoints for admin actions and for producers/consumers to poll

Run: python3 admin_service.py
"""

import os
import datetime
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, Column, Integer, String, Boolean, MetaData, Table, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from flask import request, jsonify
from sqlalchemy import text
import traceback

from flask_cors import CORS
import json 
from kafka import KafkaAdminClient


app = Flask(__name__)
from functools import wraps

# Simple admin token (you can change this)
ADMIN_TOKEN = "demo123"  # change to your team token if needed

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
    response.headers['Access-Control-Allow-Origin'] = '*'  # or 'http://localhost:3000'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    return response

@app.route('/<path:path>', methods=['OPTIONS'])
def handle_options(path):
    return ('', 204)


# CONFIG - change DB_PATH if needed
DB_PATH = 'pes1ug23cs429/173_Project2_BD/Broker_Admin/topics.db'
HOST = '0.0.0.0'
PORT = 5000

# Ensure DB dir exists
db_dir = os.path.dirname(DB_PATH)
if db_dir and not os.path.exists(db_dir):
    os.makedirs(db_dir, exist_ok=True)

# SQLAlchemy core setup (lightweight)
engine = create_engine(f'sqlite:///{DB_PATH}', echo=False, connect_args={"check_same_thread": False})
metadata = MetaData()

# Topics table: status can be 'pending', 'approved', 'active'
topics_table = Table('topics', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, unique=True, nullable=False),
    Column('description', String, default=''),
    Column('status', String, default='pending')  # 'pending', 'approved', 'active'
)

# Remove consumers_table and subscriptions_table completely

# Add this single new table instead:
consumer_subscriptions_table = Table('consumer_subscriptions', metadata,
    Column('id', Integer, primary_key=True),
    Column('consumer_id', String, nullable=False),
    Column('topic_name', String, nullable=False),
    Column('subscribed_at', DateTime, default=datetime.datetime.utcnow)
)


# --- add at top with other Table defs ---
 # add to top imports if not already

messages_table = Table('messages', metadata,
    Column('id', Integer, primary_key=True),
    Column('topic', String, nullable=False),
    Column('payload', String, nullable=False),   # JSON string
    Column('ts', DateTime, default=datetime.datetime.utcnow)
)
# ------------------------------


metadata.create_all(engine)
Session = sessionmaker(bind=engine)



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
    # --- requires existing require_admin decorator and audit_action() helper ---

# --- Reject (DELETE) endpoint: removes topic + subscriptions + messages ---
@app.route('/topics/<string:name>/reject', methods=['POST', 'DELETE'])
@require_admin
def reject_topic(name):
    """
    Reject a pending topic request by deleting it from the DB.
    Also delete subscriptions referencing the topic (consumer_subscriptions or subscriptions),
    and optionally messages for that topic in messages table.
    """
    s = Session()
    # ensure topic exists
    row = s.query(topics_table).filter_by(name=name).first()
    if not row:
        s.close()
        return jsonify({'error': 'not found'}), 404

    # Only allow delete if it's pending (safe guard). If you want to allow rejecting others, remove the check.
    if (row.status or '').lower() != 'pending':
        s.close()
        return jsonify({'error': 'invalid_status', 'message': 'only pending topics can be rejected (or delete manually)'}), 400

    # Delete subscriptions referencing the topic in consumer_subscriptions if exists
    try:
        # Try both possible subscription table names to be safe
        if 'consumer_subscriptions' in metadata.tables:
            del_q = consumer_subscriptions_table.delete().where(consumer_subscriptions_table.c.topic_name == name)
            s.execute(del_q)
        elif 'subscriptions' in metadata.tables:
            del_q = subscriptions_table.delete().where(subscriptions_table.c.topic_name == name)
            s.execute(del_q)
    except Exception:
        # ignore but log if you have logging
        pass

    # Delete messages if you have a messages table and messages.topic exists
    try:
        if 'messages' in metadata.tables:
            del_msgs = metadata.tables['messages'].delete().where(metadata.tables['messages'].c.topic_name == name)
            s.execute(del_msgs)
    except Exception:
        pass

    # Finally delete the topic row
    s.execute(topics_table.delete().where(topics_table.c.name == name))
    s.commit()
    s.close()

    # optional audit
    try:
        who = request.headers.get('X-Admin-Actor', 'unknown')
        audit_action('reject_delete', topic=name, who=who, details=f"deleted topic {name}")
    except Exception:
        pass

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

# admin_service.py  -- REPLACE your old revoke implementation with this



# admin_service.py - fixed revoke using SQLAlchemy 1.4+/2.0 style API
from flask import request, jsonify
from sqlalchemy import text

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

        # 2) update status -> pending using session (SQLAlchemy ORM/core)
        s.execute(topics_table.update().where(topics_table.c.name == name).values(status='pending'))
        s.commit()

        # 3) delete subscriptions using modern engine pattern
        rows_deleted = None
        try:
            # Use a transaction context (begin) for execute and commit
            with engine.begin() as conn:
                delete_stmt = text("DELETE FROM consumer_subscriptions WHERE topic_name = :t")
                result = conn.execute(delete_stmt, {"t": name})
                # Some DBAPIs set rowcount reliably; capture if available
                try:
                    rows_deleted = result.rowcount
                except Exception:
                    rows_deleted = None
        except Exception as e:
            # Rollback already handled by engine.begin() on exception; return the SQL error
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
        admin = KafkaAdminClient(bootstrap_servers=["10.147.19.93:9092"], client_id="admin_health", request_timeout_ms=2000)
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


#
# Run
#
if __name__ == '__main__':
    print("Admin service starting. DB:", DB_PATH)
    app.run(host=HOST, port=PORT, debug=False)

