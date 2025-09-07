import logging
import threading
import time
import requests
from datetime import datetime
from typing import Dict, List

from aw_core.models import Event

logger = logging.getLogger(__name__)


class DataScheduler:
    """
    A scheduler that fetches data from the database, sends it to backend API, and deletes it every 10 minutes.
    Events are only deleted if the API call is successful.
    """
    
    def __init__(self, api_instance, interval_minutes: int = 10):
        """
        Initialize the scheduler.
        
        Args:
            api_instance: The ServerAPI instance to access the database
            interval_minutes: How often to run the scheduler (default: 10 minutes)
        """
        self.api = api_instance
        # FIXME: add * 60
        self.interval_seconds = interval_minutes
        self.running = False
        self.thread = None
        
    def start(self):
        """Start the scheduler in a separate thread."""
        if self.running:
            logger.warning("Scheduler is already running")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()
        logger.info(f"Data scheduler started with {self.interval_seconds//60} minute intervals")
        
    def stop(self):
        """Stop the scheduler."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Data scheduler stopped")
        
    def _run_scheduler(self):
        """Main scheduler loop."""
        while self.running:
            try:
                self._process_data()
                # Wait for the next interval
                time.sleep(self.interval_seconds)
            except Exception as e:
                logger.error(f"Error in scheduler: {e}")
                # Continue running even if there's an error
                time.sleep(self.interval_seconds)
                
    def _process_data(self):
        """Fetch data from all buckets, send to backend API, and delete it."""
        try:
            logger.info("Starting scheduled data processing...")
            
            # Get stored token
            token = self.api.get_token()
            if not token:
                logger.warning("No authentication token found, skipping API call")
                return
            
            # Get all buckets
            buckets = self.api.get_buckets()
            if not buckets:
                logger.info("No buckets found")
                return
                
            total_events_processed = 0
            total_events_deleted = 0
            
            # Collect all events from all buckets
            all_events = []
            for bucket_id in buckets.keys():
                try:
                    events_processed, events_data = self._collect_bucket_events(bucket_id)
                    total_events_processed += events_processed
                    all_events.extend(events_data)
                except Exception as e:
                    logger.error(f"Error collecting events from bucket {bucket_id}: {e}")
            
            if not all_events:
                logger.info("No events to send")
                return
            
            # Send events to backend API
            success = self._send_events_to_api(all_events, token)
            
            if success:
                # Only delete events if API call was successful
                for bucket_id in buckets.keys():
                    try:
                        events_deleted = self._delete_bucket_events(bucket_id)
                        total_events_deleted += events_deleted
                    except Exception as e:
                        logger.error(f"Error deleting events from bucket {bucket_id}: {e}")
                        
                logger.info(f"Scheduled processing completed: {total_events_processed} events processed, {total_events_deleted} events deleted (last events preserved for merging)")
            else:
                logger.warning("API call failed, events not deleted")
            
        except Exception as e:
            logger.error(f"Error in data processing: {e}")
            
    def _collect_bucket_events(self, bucket_id: str) -> tuple[int, List[Dict]]:
        """
        Collect events from a single bucket for API sending.
        Ignores the last event as it will be used for merging later.
        
        Args:
            bucket_id: The ID of the bucket to process
            
        Returns:
            Tuple of (events_processed, events_data)
        """
        try:
            # Get all events from the bucket (limit=-1 means no limit)
            events = self.api.get_events(bucket_id, limit=-1)
            
            if not events:
                logger.debug(f"Bucket {bucket_id}: No events found")
                return 0, []
                
            # Skip the last event as it will be used for merging later
            if len(events) <= 1:
                logger.debug(f"Bucket {bucket_id}: Only one event found, skipping to preserve for merging")
                return 0, []
                
            # Remove the last event from processing
            events_to_process = events[:-1]
            
            logger.info(f"Bucket {bucket_id}: Collecting {len(events_to_process)} events (keeping last event for merging)")
            
            # Add bucket_id to each event for API
            events_with_bucket = []
            for event in events_to_process:
                event_data = event.copy()
                event_data['bucket_id'] = bucket_id
                events_with_bucket.append(event_data)
            
            return len(events_to_process), events_with_bucket
            
        except Exception as e:
            logger.error(f"Error collecting events from bucket {bucket_id}: {e}")
            return 0, []

    def _send_events_to_api(self, events: List[Dict], token: str) -> bool:
        """
        Send events to the backend API.
        
        Args:
            events: List of events to send
            token: Authentication token
            
        Returns:
            True if successful, False otherwise
        """
        try:
            url = "http://localhost:4000/activities"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            payload = events
            
            logger.info(f"Sending {len(events)} events to backend API at {url}")
            
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 201:
                logger.info("Successfully sent events to backend API")
                return True
            else:
                logger.error(f"Backend API returned status {response.status_code}: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send events to backend API: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending events to API: {e}")
            return False

    def _delete_bucket_events(self, bucket_id: str) -> int:
        """
        Delete events from a bucket (excluding the last event).
        
        Args:
            bucket_id: The bucket ID
            
        Returns:
            Number of events successfully deleted
        """
        try:
            # Get all events from the bucket
            events = self.api.get_events(bucket_id, limit=-1)
            
            if not events or len(events) <= 1:
                return 0
                
            # Remove the last event from deletion
            events_to_delete = events[:-1]
            
            deleted_count = 0
            for event in events_to_delete:
                try:
                    event_id = event.get('id')
                    if event_id is not None:
                        success = self.api.delete_event(bucket_id, event_id)
                        if success:
                            deleted_count += 1
                        else:
                            logger.warning(f"Failed to delete event {event_id} from bucket {bucket_id}")
                    else:
                        logger.warning(f"Event has no ID, cannot delete: {event}")
                except Exception as e:
                    logger.error(f"Error deleting event {event.get('id', 'unknown')} from bucket {bucket_id}: {e}")
                    
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error deleting events from bucket {bucket_id}: {e}")
            return 0


# Global scheduler instance
_scheduler_instance = None


def start_scheduler(api_instance, interval_minutes: int = 10):
    """
    Start the global scheduler instance.
    
    Args:
        api_instance: The ServerAPI instance
        interval_minutes: How often to run the scheduler (default: 10 minutes)
    """
    global _scheduler_instance
    
    if _scheduler_instance is not None:
        logger.warning("Scheduler is already initialized")
        return
        
    _scheduler_instance = DataScheduler(api_instance, interval_minutes)
    _scheduler_instance.start()
    

def stop_scheduler():
    """Stop the global scheduler instance."""
    global _scheduler_instance
    
    if _scheduler_instance is not None:
        _scheduler_instance.stop()
        _scheduler_instance = None
