# Kafka Consumer Frontend

A React-based frontend application for managing Kafka topic subscriptions.

## Features

- View all active topics from the admin service
- Subscribe to topics with a single click
- Unsubscribe from subscribed topics
- Real-time updates (polls every 5 seconds)
- View current subscriptions

## Setup

1. Install dependencies:
```bash
npm install
```

2. Start the development server:
```bash
npm run dev
```

The application will open at `http://localhost:3000`

## Build for Production

```bash
npm run build
```

The built files will be in the `dist` directory.

## Configuration

The API endpoints are configured in `src/api.js`. The default configuration uses:
- Admin Base URL: `http://10.147.19.93:5000`
- Consumer Name: `consumer-1`
- Poll Interval: 5 seconds

You can modify these values in `src/api.js` if needed.


