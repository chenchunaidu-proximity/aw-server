# Data Scheduler

The ActivityWatch server now includes a data scheduler that automatically fetches data from the database, prints it, and deletes it at regular intervals.

## Features

- **Automatic Data Processing**: Fetches all events from all buckets every 10 minutes (configurable)
- **Data Printing**: Prints all fetched data in a readable format to the console
- **Data Cleanup**: Deletes all processed data from the database (except the last event in each bucket)
- **Merging Preservation**: Preserves the last event in each bucket for future merging operations
- **Configurable**: Can be enabled/disabled and interval can be adjusted
- **Thread-Safe**: Runs in a separate daemon thread

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

## Output Format

The scheduler prints data in the following format:

```
============================================================
BUCKET: aw-watcher-window_username
TIMESTAMP: 2024-01-15T10:30:00.123456
EVENT COUNT: 5 (last event preserved for merging)
============================================================

Event 1:
  ID: 12345
  Timestamp: 2024-01-15T10:25:00.000000
  Duration: 300.0
  Data: {'app': 'Chrome', 'title': 'Google - Chrome'}

Event 2:
  ID: 12346
  Timestamp: 2024-01-15T10:30:00.000000
  Duration: 0.0
  Data: {'app': 'Terminal', 'title': 'bash'}

============================================================
```

## Logging

The scheduler logs its activities to the ActivityWatch server log:

- `INFO`: Scheduler start/stop and processing completion
- `DEBUG`: Detailed processing information
- `WARNING`: Failed deletions or missing event IDs
- `ERROR`: Processing errors

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

## Notes

- The scheduler runs as a daemon thread, so it will stop when the main server process stops
- All data is permanently deleted after processing - make sure this is what you want
- **The last event in each bucket is preserved for future merging operations**
- The scheduler processes all buckets and all events in each bucket (except the last event)
- Consider the impact on performance when setting very short intervals
