import logging
import threading
import time
import requests
from datetime import datetime
from typing import Dict, List

from aw_core.models import Event
from .utils import chunks, retry_api_call

logger = logging.getLogger(__name__)

BATCH_SIZE = 100


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
        self.interval_seconds = interval_minutes * 60
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
        
    def stop(self):
        """Stop the scheduler."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        
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
            # Get stored token and URL
            token_data = self.api.get_token_data()
            if not token_data:
                return
            
            token, api_url = token_data
            
            # Get all buckets
            buckets = self.api.get_buckets()
            if not buckets:
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
                return
            
            # Send events to backend API (deletion handled internally)
            self._send_events_to_api(all_events, token, api_url)
            
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
                return 0, []
                
            # Skip the last event as it will be used for merging later
            if len(events) <= 1:
                return 0, []
                
            # Remove the last event from processing
            events_to_process = events[:-1]
            
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

    def _send_events_to_api(self, events: List[Dict], token: str, api_url: str) -> bool:
        """Send events to the backend API in batches of max 100 events."""
        
        successfully_sent = []
        
        for batch in chunks(events, BATCH_SIZE):
            if self._send_single_batch(batch, token, api_url):
                successfully_sent.extend(batch)
            else:
                break
        
        # Delete successfully sent events
        if successfully_sent:
            self._delete_successfully_sent_events(successfully_sent)
        
        return len(successfully_sent) == len(events)

    def _send_single_batch(self, batch: List[Dict], token: str, api_url: str) -> bool:
        """Send a single batch of events with retry logic."""
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        
        def make_request():
            response = requests.post(api_url, json=batch, headers=headers, timeout=30)
            if 200 <= response.status_code < 300:
                return True
            elif 400 <= response.status_code < 500:
                return False  # Don't retry client errors
            else:
                raise requests.RequestException(f"Server error {response.status_code}")
        
        return retry_api_call(make_request, max_attempts=3, base_delay=0.5)

    def _delete_successfully_sent_events(self, events: List[Dict]) -> None:
        """Delete events that were successfully sent to API."""
        for event in events:
            try:
                self.api.delete_event(event['bucket_id'], event['id'])
            except Exception:
                pass  # Log but don't fail the entire operation

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
                except Exception:
                    pass
                    
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
