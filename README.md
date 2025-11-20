# Kafka Producer Node

## Project Structure

```
producer/
│
├── producer_main.py          # Main producer entry point
├── ingest_thread.py          # Handles data ingestion
├── publisher_thread.py       # Publishes messages to Kafka
├── topic_watcher.py          # Monitors topic creation/updates
│
├── data/                     # Data directory
│   ├── dataset.csv           # Dataset file
│   ├── dataset.config.json   # Configuration for dataset
│   ├── <more_datasets>.csv   # Additional datasets
│   └── <more_datasets>.config.json  # Additional configs
│
└── __pycache__/             # Python cache (auto-generated)
```

## Prerequisites

- Python 3.7+
- Kafka broker running
- Admin service running (Flask API)

## Installation

### 1. Update System Packages

```bash
sudo apt update
```

### 2. Install Python and pip

```bash
sudo apt install python3 python3-pip -y
```

### 3. Install Python Dependencies

```bash
pip3 install kafka-python requests
```

## Running the Producer

### Navigate to Producer Directory

```bash
cd ~/producer
```

### Start the Producer

```bash
python3 producer_main.py
```

# Broker-Admin Node


## Project Structure

```
Broker_Admin/
│
├── admin_service.py      # Flask admin service
│   
│
└── streamlit_admin.py        # Streamlit dashboard UI
```

## Prerequisites

- Kafka and ZooKeeper installed (typically in `/opt/kafka`)
- Python 3.7+
- Virtual environments:
  - `~/venv/` for admin service
  - `~/env_streamlit/` for Streamlit
- SQLite3 (for database viewing)

## Setup Instructions

### Step 1: Start ZooKeeper

```bash
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Step 2: Start Kafka Broker

```bash
cd /opt/kafka
bin/kafka-server-start.sh config/server.properties
```

### Step 3: Run the Admin Service

```bash
source ~/venv/bin/activate
python3 /home/youruser/admin_service.py
```

**Note**: Replace `/home/youruser/` with your actual path to `admin_service.py`

### Step 4: Run the Streamlit Dashboard

```bash
source ~/env_streamlit/bin/activate 2>/dev/null || true
streamlit run streamlit_admin.py --server.port 8501 --server.address 0.0.0.0
```

The Streamlit dashboard will be available at `http://localhost:8501`

## Viewing SQLite Database

To view the SQLite database:

```bash
sqlite3 /home/pes1ug23cs429/topics.db
```

### Useful SQLite Commands

```sql
-- View all tables
.tables

-- View topics
SELECT * FROM topics;

-- View consumer subscriptions
SELECT * FROM consumer_subscriptions;

-- View messages
SELECT * FROM messages LIMIT 10;

-- Exit
.quit
```



# Kafka Consumer Node

## Project Structure

```
Consumer/
├── config.py                 # Configuration (broker URL, admin URL)
├── run_consumer.py          # Main consumer runner
├── kafka_consumer.py        # Kafka consumer class
├── topic_poller.py          # Polls admin for active topics
├── subscription_poller.py   # Polls admin for subscriptions
├── requirements.txt         # Python dependencies
├── .gitignore              # Git ignore rules
├── logs/                   # Auto-generated log files (per consumer)
│   ├── consumer-1/
│   ├── consumer-2/
│   └── consumer-3/
└── frontend/               # React frontend
    ├── src/
    │   ├── App.jsx         # Main component
    │   ├── App.css         # Styles
    │   ├── api.js          # API service
    │   └── main.jsx        # Entry point
    ├── package.json
    └── vite.config.js
```

## Prerequisites

- Python 3.7+
- Node.js 16+ and npm
- Kafka broker running
- Admin service running (Flask API)

## Installation

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Install Frontend Dependencies

```bash
cd frontend
npm install
```

## Configuration

Edit `config.py` to set:
- `BROKER_URL`: Kafka broker address
- `ADMIN_BASE_URL`: Admin service URL
- `GROUP_ID`: Default consumer group ID
- `POLL_INTERVAL`: Polling interval in seconds
- `CONSUMER_NAME`: Default consumer name

## Running the Consumer

### Single Consumer (Default)

```bash
python3 run_consumer.py
```

### Multiple Consumers with Environment Variables

**Consumer 1 (Group A - Load Balanced):**
```bash
CONSUMER_NAME=consumer-1 GROUP_ID=dynamic-consumer-group python3 run_consumer.py
```

**Consumer 2 (Group A - Load Balanced):**
```bash
CONSUMER_NAME=consumer-2 GROUP_ID=dynamic-consumer-group python3 run_consumer.py
```

**Consumer 3 (Group B - Independent):**
```bash
CONSUMER_NAME=consumer-3 GROUP_ID=independent-consumer-group python3 run_consumer.py
```


## Running the Frontend

```bash
cd frontend
npm start
```

T

## Stopping Consumers

```bash
# Stop all consumers
pkill -f run_consumer.py

# Or use Ctrl+C in the terminal where consumer is running
```


