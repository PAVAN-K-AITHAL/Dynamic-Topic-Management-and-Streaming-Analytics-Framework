// API service for backend communication
const ADMIN_BASE_URL = "http://localhost:5000";
const POLL_INTERVAL = 5000; // 5 seconds

// Register consumer on initialization (no longer needed with new API, but keeping for compatibility)
export const registerConsumer = async (consumerName) => {
  // Consumer registration is implicit in the new API
  return true;
};

// Fetch active topics
export const fetchActiveTopics = async () => {
  try {
    const response = await fetch(`${ADMIN_BASE_URL}/topics/active`);
    if (response.ok) {
      const data = await response.json();
      if (!Array.isArray(data)) {
        console.warn('Unexpected active topics payload:', data);
        return [];
      }

      const topics = data
        .map((t) => t?.name ?? t?.topic ?? t?.topic_name ?? null)
        .filter(Boolean);

      if (topics.length === 0 && data.length > 0) {
        console.warn('Active topics payload missing name/topic fields:', data);
      }

      return topics;
    }
    return [];
  } catch (error) {
    console.error('Error fetching active topics:', error);
    return [];
  }
};

// Fetch current subscriptions
export const fetchSubscriptions = async (consumerName) => {
  try {
    const response = await fetch(`${ADMIN_BASE_URL}/subscriptions/${consumerName}`);
    if (response.ok) {
      const data = await response.json();
      return data.map(s => s.topic_name);
    }
    return [];
  } catch (error) {
    console.error('Error fetching subscriptions:', error);
    return [];
  }
};

// Subscribe to a topic
export const subscribeToTopic = async (consumerName, topic) => {
  try {
    const response = await fetch(`${ADMIN_BASE_URL}/subscribe`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ 
        consumer_id: consumerName,
        topic_name: topic 
      }),
    });
    return response.ok;
  } catch (error) {
    console.error('Error subscribing to topic:', error);
    return false;
  }
};

// Unsubscribe from a topic
export const unsubscribeFromTopic = async (consumerName, topic) => {
  try {
    const response = await fetch(`${ADMIN_BASE_URL}/unsubscribe`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ 
        consumer_id: consumerName,
        topic_name: topic 
      }),
    });
    return response.ok;
  } catch (error) {
    console.error('Error unsubscribing from topic:', error);
    return false;
  }
};

export { POLL_INTERVAL };


