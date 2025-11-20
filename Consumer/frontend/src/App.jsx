import React, { useState, useEffect } from 'react';
import {
  registerConsumer,
  fetchActiveTopics,
  fetchSubscriptions,
  subscribeToTopic,
  unsubscribeFromTopic,
  POLL_INTERVAL,
} from './api';
import './App.css';

function App() {
  const [selectedConsumer, setSelectedConsumer] = useState('consumer-1');
  const [activeTopics, setActiveTopics] = useState([]);
  const [subscribedTopics, setSubscribedTopics] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [actionLoading, setActionLoading] = useState({});

  // Available consumers list (you can extend this or fetch from API)
  const availableConsumers = ['consumer-1', 'consumer-2', 'consumer-3'];

  useEffect(() => {
    // Load data when consumer changes
    loadData();
    // Start polling for active topics
    const pollInterval = setInterval(() => {
      fetchActiveTopics().then(topics => {
        setActiveTopics(topics);
      });
      // Also refresh subscriptions to keep them in sync
      fetchSubscriptions(selectedConsumer).then(subs => {
        setSubscribedTopics(subs);
      });
    }, POLL_INTERVAL);

    return () => clearInterval(pollInterval);
  }, [selectedConsumer]);

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [topics, subscriptions] = await Promise.all([
        fetchActiveTopics(),
        fetchSubscriptions(selectedConsumer),
      ]);
      setActiveTopics(topics);
      setSubscribedTopics(subscriptions);
    } catch (err) {
      setError('Failed to load data. Please check if the backend is running.');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleConsumerChange = (event) => {
    setSelectedConsumer(event.target.value);
    setSubscribedTopics([]); // Clear subscriptions while switching
  };

  const handleSubscribe = async (topic) => {
    if (!activeTopics.includes(topic)) {
      setError(`Topic "${topic}" is not in active topics.`);
      return;
    }

    setActionLoading({ ...actionLoading, [topic]: true });
    try {
      const success = await subscribeToTopic(selectedConsumer, topic);
      if (success) {
        // Refresh subscriptions from server to get the latest state
        const updatedSubs = await fetchSubscriptions(selectedConsumer);
        setSubscribedTopics(updatedSubs);
        setError(null);
      } else {
        setError(`Failed to subscribe to "${topic}".`);
      }
    } catch (err) {
      setError(`Error subscribing to "${topic}": ${err.message}`);
    } finally {
      setActionLoading({ ...actionLoading, [topic]: false });
    }
  };

  const handleUnsubscribe = async (topic) => {
    setActionLoading({ ...actionLoading, [topic]: true });
    try {
      const success = await unsubscribeFromTopic(selectedConsumer, topic);
      if (success) {
        // Refresh subscriptions from server to get the latest state
        const updatedSubs = await fetchSubscriptions(selectedConsumer);
        setSubscribedTopics(updatedSubs);
        setError(null);
      } else {
        setError(`Failed to unsubscribe from "${topic}".`);
      }
    } catch (err) {
      setError(`Error unsubscribing from "${topic}": ${err.message}`);
    } finally {
      setActionLoading({ ...actionLoading, [topic]: false });
    }
  };

  const isSubscribed = (topic) => subscribedTopics.includes(topic);
  
  // Filter out subscribed topics from active topics display
  const availableTopics = activeTopics.filter(topic => !isSubscribed(topic));

  return (
    <div className="app">
      <div className="container">
        <header className="header">
          <h1>Kafka Consumer - Topic Manager</h1>
          <p className="subtitle">Manage your topic subscriptions</p>
          <div className="consumer-selector">
            <label htmlFor="consumer-select">Select Consumer: </label>
            <select
              id="consumer-select"
              value={selectedConsumer}
              onChange={handleConsumerChange}
              className="consumer-dropdown"
            >
              {availableConsumers.map(consumer => (
                <option key={consumer} value={consumer}>
                  {consumer}
                </option>
              ))}
            </select>
          </div>
        </header>

        {error && (
          <div className="error-message">
            {error}
            <button onClick={() => setError(null)} className="close-btn">×</button>
          </div>
        )}

        <div className="content">
          <section className="section">
            <h2>Available Topics</h2>
            {loading ? (
              <div className="loading">Loading topics...</div>
            ) : availableTopics.length === 0 ? (
              <div className="empty-state">No available topics to subscribe</div>
            ) : (
              <div className="topics-grid">
                {availableTopics.map((topic) => (
                  <div
                    key={topic}
                    className="topic-card"
                  >
                    <div className="topic-name">{topic}</div>
                    <div className="topic-actions">
                      <button
                        onClick={() => handleSubscribe(topic)}
                        disabled={actionLoading[topic]}
                        className="btn btn-subscribe"
                      >
                        {actionLoading[topic] ? 'Subscribing...' : 'Subscribe'}
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </section>

          <section className="section">
            <h2>{selectedConsumer} - Subscriptions ({subscribedTopics.length})</h2>
            {subscribedTopics.length === 0 ? (
              <div className="empty-state">You are not subscribed to any topics</div>
            ) : (
              <div className="subscriptions-list">
                {subscribedTopics.map((topic) => (
                  <div key={topic} className="subscription-item">
                    <span className="subscription-topic">{topic}</span>
                    <button
                      onClick={() => handleUnsubscribe(topic)}
                      disabled={actionLoading[topic]}
                      className="btn btn-unsubscribe-small"
                    >
                      {actionLoading[topic] ? '...' : 'Unsubscribe'}
                    </button>
                  </div>
                ))}
              </div>
            )}
          </section>
        </div>

        <footer className="footer">
          <button onClick={loadData} className="btn btn-refresh">
            Refresh
          </button>
          <p className="footer-text">
            Topics are automatically refreshed every {POLL_INTERVAL / 1000} seconds
          </p>
        </footer>
      </div>
    </div>
  );
}

export default App;


