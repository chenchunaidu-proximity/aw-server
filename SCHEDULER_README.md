# Data Scheduler

The ActivityWatch server now includes a data scheduler that automatically fetches data from the database, sends it to a backend API, and deletes it at regular intervals. The scheduler requires authentication token and API URL configuration to function.

## Features

- **Automatic Data Processing**: Fetches all events from all buckets every 10 minutes (configurable)
- **Backend API Integration**: Sends collected data to a configurable backend API endpoint
- **Authentication**: Uses stored authentication token for secure API communication
- **Data Cleanup**: Deletes all processed data from the database (except the last event in each bucket)
- **Merging Preservation**: Preserves the last event in each bucket for future merging operations
- **Configurable**: Can be enabled/disabled and interval can be adjusted
- **Thread-Safe**: Runs in a separate daemon thread
- **Success-Based Deletion**: Only deletes local data if API call succeeds

## Authentication Setup

The scheduler requires authentication token and backend API URL to be configured before it can send data. There are several ways to set this up:

### Method 1: URL Scheme (Recommended)

Use the ActivityWatch URL scheme to automatically configure both token and URL:

**From Web Application:**
```javascript
const token = "your-auth-token";
const apiUrl = encodeURIComponent("http://localhost:4000/activities");
window.location.href = `activitywatch://token?token=${token}&url=${apiUrl}`;
```

**From Command Line:**
```bash
# macOS
open "activitywatch://token?token=your-token-here&url=http://localhost:4000/activities"

# Linux
xdg-open "activitywatch://token?token=your-token-here&url=http://localhost:4000/activities"
```

### Method 2: Direct API Call

Store token and URL directly via the ActivityWatch API:

```bash
curl -X POST http://localhost:5600/api/0/token \
  -H "Content-Type: application/json" \
  -d '{
    "token": "your-auth-token-here",
    "url": "http://localhost:4000/activities"
  }'
```

### Method 3: URL Scheme API

Process a URL scheme via the API:

```bash
curl -X POST http://localhost:5600/api/0/url-scheme \
  -H "Content-Type: application/json" \
  -d '{
    "url": "activitywatch://token?token=your-token-here&url=http://localhost:4000/activities"
  }'
```

### Verification

Check if authentication is configured:

```bash
curl http://localhost:5600/api/0/token
```

Expected response:
```json
{
  "token": "your-auth-token-here",
  "url": "http://localhost:4000/activities"
}
```

## Configuration

### Configuration File

The scheduler can be configured in the ActivityWatch server configuration file:

```toml
[server]
host = "localhost"
port = "5600"
storage = "peewee"
cors_origins = ""
scheduler_interval_minutes = "10"  # How often to run (in minutes)
```

### Command Line Arguments

You can also control the scheduler via command line arguments:

```bash
# Start with default settings (10 minutes, enabled)
aw-server

# Change the interval to 5 minutes
aw-server --scheduler-interval 5

# Disable the scheduler
aw-server --scheduler-disabled

# Enable the scheduler (if disabled in config)
aw-server --scheduler-enabled

# Combine options
aw-server --scheduler-interval 15 --scheduler-enabled
```

## Data Flow

The scheduler follows this process every 10 minutes:

1. **Check Authentication**: Verifies that token and URL are configured
2. **Collect Data**: Gathers all events from all buckets (except last event in each)
3. **Send to API**: Posts data to the configured backend endpoint
4. **Conditional Deletion**: Only deletes local data if API call succeeds

### API Request Format

The scheduler sends data to the backend API in this format:

**Request:**
```http
POST http://localhost:4000/activities
Authorization: Bearer your-auth-token
Content-Type: application/json

[
  {
    "id": 12345,
    "timestamp": "2024-01-15T10:25:00.000000",
    "duration": 300.0,
    "data": {
      "app": "Chrome",
      "title": "Google - Chrome"
    },
    "bucket_id": "aw-watcher-window_username"
  },
  {
    "id": 12346,
    "timestamp": "2024-01-15T10:30:00.000000",
    "duration": 0.0,
    "data": {
      "app": "Terminal",
      "title": "bash"
    },
    "bucket_id": "aw-watcher-window_username"
  }
]
```

**Expected Response:**
```http
HTTP/1.1 201 Created
Content-Type: application/json

{
  "message": "Events processed successfully",
  "count": 2
}
```

## Logging

The scheduler logs its activities to the ActivityWatch server log:

- `INFO`: Scheduler start/stop, API calls, and processing completion
- `DEBUG`: Detailed processing information and data collection
- `WARNING`: Missing authentication, failed API calls, or failed deletions
- `ERROR`: Processing errors and network issues

### Example Log Messages

```
INFO: Data scheduler started with 10 minute intervals
INFO: Starting scheduled data processing...
WARNING: No authentication token and URL found, skipping API call
INFO: Sending 15 events to backend API at http://localhost:4000/activities
INFO: Successfully sent events to backend API
INFO: Scheduled processing completed: 15 events processed, 15 events deleted
ERROR: Failed to send events to backend API: Connection refused
WARNING: API call failed, events not deleted
```

## Safety Features

- **Error Handling**: Continues running even if individual buckets fail to process
- **Graceful Shutdown**: Properly stops when the server shuts down
- **Thread Safety**: Uses locks to prevent concurrent access issues
- **Data Validation**: Checks for event IDs before attempting deletion

## Use Cases

- **Data Archiving**: Process and archive data before deletion
- **Data Analysis**: Regular analysis of collected data
- **Storage Management**: Prevent database from growing too large
- **Compliance**: Regular data cleanup for privacy/security requirements

## Troubleshooting

### Common Issues

**"No authentication token and URL found, skipping API call"**
- Configure authentication using one of the methods in the Authentication Setup section
- Verify configuration with: `curl http://localhost:5600/api/0/token`

**"Failed to send events to backend API: Connection refused"**
- Check if the backend API server is running
- Verify the URL is correct in the stored configuration
- Check firewall and network connectivity

**"Backend API returned status 401"**
- Verify the authentication token is valid
- Check if the token has expired
- Ensure the backend API accepts the token format

**"Events not deleted from local storage"**
- This is expected behavior when API calls fail
- Events will be retried in the next cycle (10 minutes)
- Check the API response status (should be 201 for success)

### Debug Steps

1. **Check Authentication Status:**
   ```bash
   curl http://localhost:5600/api/0/token
   ```

2. **Test API Endpoint Manually:**
   ```bash
   curl -X POST http://localhost:4000/activities \
     -H "Authorization: Bearer your-token" \
     -H "Content-Type: application/json" \
     -d '[]'
   ```

3. **Check ActivityWatch Logs:**
   ```bash
   tail -f ~/.local/share/activitywatch/aw-server/logs/aw-server.log
   ```

4. **Test URL Scheme Processing:**
   ```bash
   curl -X POST http://localhost:5600/api/0/url-scheme \
     -H "Content-Type: application/json" \
     -d '{"url": "activitywatch://token?token=test&url=http://localhost:4000/activities"}'
   ```

## Notes

- The scheduler runs as a daemon thread, so it will stop when the main server process stops
- **Authentication is required** - the scheduler will not send data without a valid token and URL
- **Data is only deleted after successful API calls** - failed API calls preserve local data
- **The last event in each bucket is preserved for future merging operations**
- **No retry logic** - failed API calls are logged but not retried until the next cycle
- The scheduler processes all buckets and all events in each bucket (except the last event)
- Consider the impact on performance when setting very short intervals
- Backend API must respond with status code 201 for successful processing
