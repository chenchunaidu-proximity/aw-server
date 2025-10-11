import time
import random
import logging

logger = logging.getLogger(__name__)


def chunks(lst, n):
    """Split a list into chunks of size n."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def retry_api_call(func, *args, max_attempts=3, base_delay=0.5, **kwargs):
    for attempt in range(max_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Don't retry client errors (4xx) - these won't succeed on retry
            if hasattr(e, 'response') and 400 <= e.response.status_code < 500:
                logger.warning(f"Client error {e.response.status_code}, not retrying: {type(e).__name__}")
                raise e
            
            # Don't retry on the last attempt
            if attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                logger.info(f"Attempt {attempt + 1}/{max_attempts} failed, retrying in {delay:.2f}s: {type(e).__name__}")
                time.sleep(delay)
            else:
                logger.error(f"All {max_attempts} attempts failed: {type(e).__name__}: {e}")
                raise e
