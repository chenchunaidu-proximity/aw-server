import logging
import threading
import time
import requests
from datetime import datetime
from typing import Dict, List

from aw_core.models import Event
from aw_datastore.storages.peewee import chunks
from .utils import retry_api_call

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
            logger.warning("===>> Scheduler is already running")
            return
            
        logger.info(f"===>> Starting DataScheduler with {self.interval_seconds/60:.1f} minute interval")
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
        logger.info("===>> DataScheduler main loop started")
        cycle_count = 0
        while self.running:
            try:
                cycle_count += 1
                logger.info(f"===>> Starting scheduler cycle #{cycle_count}")
                self._process_data()
                logger.info(f"===>> Completed scheduler cycle #{cycle_count}, waiting {self.interval_seconds/60:.1f} minutes")
                # Wait for the next interval
                time.sleep(self.interval_seconds)
            except Exception as e:
                logger.error(f"===>> Error in scheduler cycle #{cycle_count}: {e}")
                # Continue running even if there's an error
                time.sleep(self.interval_seconds)
                
    def _process_data(self):
        """Fetch data from all buckets, send to backend API, and delete it."""
        try:
            logger.info("===>> Starting data processing cycle")
            
            # Get stored token and URL from JSON storage
            from aw_datastore.storages.token_manager import TokenManager
            token_manager = TokenManager(testing=False)
            token_data = token_manager.get_token_data()
            if not token_data:
                logger.info("===>> No authentication token found, skipping data processing")
                return
            
            token, api_url = token_data
            logger.info(f"===>> Authentication configured, API URL: {api_url}")
            
            # Get all buckets
            buckets = self.api.get_buckets()
            if not buckets:
                logger.info("===>> No buckets found, skipping data processing")
                return
                
            logger.info(f"===>> Found {len(buckets)} buckets to process")
            total_events_processed = 0
            total_events_deleted = 0
            
            # Collect all events from all buckets
            all_events = []
            for bucket_id in buckets.keys():
                try:
                    events_processed, events_data = self._collect_bucket_events(bucket_id)
                    total_events_processed += events_processed
                    all_events.extend(events_data)
                    if events_processed > 0:
                        logger.info(f"===>> Collected {events_processed} events from bucket {bucket_id}")
                except Exception as e:
                    logger.error(f"===>> Error collecting events from bucket {bucket_id}: {e}")
            
            if not all_events:
                logger.info("===>> No events to process, skipping API call")
                return
            
            logger.info(f"===>> Total events collected: {len(all_events)}")
            
            # Send events to backend API (deletion handled internally)
            success = self._send_events_to_api(all_events, token, api_url)
            if not success:
                logger.error(f"===>> Failed to process {len(all_events)} events")
            
        except Exception as e:
            logger.error(f"===>> Error in data processing: {e}")
            logger.error(f"===>> Error type: {type(e).__name__}")
            import traceback
            logger.error(f"===>> Full traceback: {traceback.format_exc()}")
            
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
        
        logger.info(f"===>> Starting API call to {api_url} with {len(events)} events in batches of {BATCH_SIZE}")
        successfully_sent = []
        total_batches = (len(events) + BATCH_SIZE - 1) // BATCH_SIZE
        batch_count = 0
        
        for batch in chunks(events, BATCH_SIZE):
            batch_count += 1
            logger.info(f"===>> Sending batch {batch_count}/{total_batches} ({len(batch)} events)")
            
            if self._send_single_batch(batch, token, api_url):
                successfully_sent.extend(batch)
            else:
                logger.error(f"===>> Batch {batch_count}/{total_batches} failed, stopping")
                break
        
        # Delete successfully sent events
        if successfully_sent:
            self._delete_successfully_sent_events(successfully_sent)
        
        return len(successfully_sent) == len(events)

    def _send_single_batch(self, batch: List[Dict], token: str, api_url: str) -> bool:
        """Send a single batch of events with retry logic."""
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        
        def make_request():
            logger.info(f"===>> Making HTTP POST request to {api_url}")
            start_time = time.time()
            
            try:
                response = requests.post(api_url, json=batch, headers=headers, timeout=30)
                response_time = time.time() - start_time
                
                # Check if response is None (shouldn't happen but let's be safe)
                if response is None:
                    logger.error("===>> API call returned None response")
                    raise requests.RequestException("API call returned None response")
                
                logger.info(f"===>> API response: {response.status_code} in {response_time:.2f}s")
                
                if 200 <= response.status_code < 300:
                    logger.info(f"===>> API call successful: {response.status_code}")
                    return True
                elif response.status_code == 401:
                    logger.error(f"===>> Authentication failed (401) - Token expired or invalid")
                    logger.error(f"===>> User needs to re-authenticate via samay:// URL scheme")
                    logger.error(f"===>> Response: {response.text}")
                    return False
                elif 400 <= response.status_code < 500:
                    logger.error(f"===>> API client error: {response.status_code} - {response.text}")
                    return False  # Don't retry client errors
                else:
                    logger.error(f"===>> API server error: {response.status_code} - {response.text}")
                    raise requests.RequestException(f"Server error {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                response_time = time.time() - start_time
                logger.error(f"===>> API request failed after {response_time:.2f}s: {e}")
                raise
            except Exception as e:
                response_time = time.time() - start_time
                logger.error(f"===>> Unexpected error during API request after {response_time:.2f}s: {e}")
                raise requests.RequestException(f"Unexpected error: {e}")
        
        try:
            return retry_api_call(make_request, max_attempts=3, base_delay=0.5)
        except Exception as e:
            logger.error(f"===>> API call failed after retries: {e}")
            return False

    def _delete_successfully_sent_events(self, events: List[Dict]) -> None:
        """Delete events that were sent to API."""
        deleted_count = 0
        failed_count = 0
        
        logger.info(f"===>> Deleting {len(events)} successfully sent events")
        
        for event in events:
            try:
                bucket_id = event.get('bucket_id')
                event_id = event.get('id')
                
                if not bucket_id or event_id is None:
                    logger.warning(f"===>> Skipping event with missing bucket_id or id")
                    failed_count += 1
                    continue
                
                success = self.api.delete_event(bucket_id, event_id)
                if success:
                    deleted_count += 1
                else:
                    failed_count += 1
                    logger.warning(f"===>> Failed to delete event {event_id} from bucket {bucket_id}")
            except Exception as e:
                failed_count += 1
                logger.warning(f"===>> Exception deleting event {event.get('id')} from bucket {event.get('bucket_id')}: {e}")
        
        logger.info(f"===>> Event deletion completed: {deleted_count} deleted, {failed_count} failed")

    def _delete_bucket_events(self, bucket_id: str) -> int:
        """
        Delete events from a bucket (excluding the last event).
        
        Args:
            bucket_id: The bucket ID
            
        Returns:
            Number of events deleted
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
        logger.warning("===>> Scheduler is already initialized")
        return
        
    logger.info(f"===>> Initializing global DataScheduler with {interval_minutes} minute interval")
    _scheduler_instance = DataScheduler(api_instance, interval_minutes)
    _scheduler_instance.start()
    

def stop_scheduler():
    """Stop the global scheduler instance."""
    global _scheduler_instance
    
    if _scheduler_instance is not None:
        logger.info("===>> Stopping global DataScheduler")
        _scheduler_instance.stop()
        _scheduler_instance = None
    else:
        logger.info("===>> No scheduler instance to stop")
