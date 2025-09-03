import logging
import threading
import time
from datetime import datetime
from typing import Dict, List

from aw_core.models import Event

logger = logging.getLogger(__name__)


class DataScheduler:
    """
    A scheduler that fetches data from the database, prints it, and deletes it every 10 minutes.
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
        """Fetch data from all buckets, print it, and delete it."""
        try:
            logger.info("Starting scheduled data processing...")
            
            # Get all buckets
            buckets = self.api.get_buckets()
            if not buckets:
                logger.info("No buckets found")
                return
                
            total_events_processed = 0
            total_events_deleted = 0
            
            for bucket_id in buckets.keys():
                try:
                    events_processed, events_deleted = self._process_bucket(bucket_id)
                    total_events_processed += events_processed
                    total_events_deleted += events_deleted
                except Exception as e:
                    logger.error(f"Error processing bucket {bucket_id}: {e}")
                    
            logger.info(f"Scheduled processing completed: {total_events_processed} events processed, {total_events_deleted} events deleted (last events preserved for merging)")
            
        except Exception as e:
            logger.error(f"Error in data processing: {e}")
            
    def _process_bucket(self, bucket_id: str) -> tuple[int, int]:
        """
        Process a single bucket: fetch events, print them, and delete them.
        Ignores the last event as it will be used for merging later.
        
        Args:
            bucket_id: The ID of the bucket to process
            
        Returns:
            Tuple of (events_processed, events_deleted)
        """
        try:
            # Get all events from the bucket (limit=-1 means no limit)
            events = self.api.get_events(bucket_id, limit=-1)
            
            if not events:
                logger.debug(f"Bucket {bucket_id}: No events found")
                return 0, 0
                
            # Skip the last event as it will be used for merging later
            if len(events) <= 1:
                logger.debug(f"Bucket {bucket_id}: Only one event found, skipping to preserve for merging")
                return 0, 0
                
            # Remove the last event from processing
            events_to_process = events[:-1]
            last_event = events[-1]
            
            logger.info(f"Bucket {bucket_id}: Processing {len(events_to_process)} events (keeping last event for merging)")
            
            # Print the events (excluding the last one)
            self._print_events(bucket_id, events_to_process)
            
            # Delete the events (excluding the last one)
            deleted_count = self._delete_events(bucket_id, events_to_process)
            
            logger.info(f"Bucket {bucket_id}: {len(events_to_process)} events processed, {deleted_count} events deleted (last event preserved)")
            return len(events_to_process), deleted_count
            
        except Exception as e:
            logger.error(f"Error processing bucket {bucket_id}: {e}")
            return 0, 0
            
    def _print_events(self, bucket_id: str, events: List[Dict]):
        """Print events in a readable format."""
        print(f"\n{'='*60}")
        print(f"BUCKET: {bucket_id}")
        print(f"TIMESTAMP: {datetime.now().isoformat()}")
        print(f"EVENT COUNT: {len(events)} (last event preserved for merging)")
        print(f"{'='*60}")
        
        for i, event in enumerate(events, 1):
            print(f"\nEvent {i}:")
            print(f"  ID: {event.get('id', 'N/A')}")
            print(f"  Timestamp: {event.get('timestamp', 'N/A')}")
            print(f"  Duration: {event.get('duration', 'N/A')}")
            print(f"  Data: {event.get('data', {})}")
            
        print(f"\n{'='*60}\n")
        
    def _delete_events(self, bucket_id: str, events: List[Dict]) -> int:
        """
        Delete events from the bucket.
        
        Args:
            bucket_id: The bucket ID
            events: List of events to delete
            
        Returns:
            Number of events successfully deleted
        """
        deleted_count = 0
        
        for event in events:
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
