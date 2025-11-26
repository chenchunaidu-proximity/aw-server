import time
import random
import logging

logger = logging.getLogger(__name__)


def retry_api_call(func, *args, max_attempts=3, base_delay=0.5, **kwargs):
    for attempt in range(max_attempts):
        try:
            result = func(*args, **kwargs)
            
            # Check if function returned None (which indicates failure)
            if result is None:
                raise Exception("Function returned None")
                
            return result
            
        except Exception as e:
            # Don't retry client errors (4xx) - these won't succeed on retry
            if hasattr(e, 'response') and e.response is not None and 400 <= e.response.status_code < 500:
                raise e
            
            # Don't retry on the last attempt
            if attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                time.sleep(delay)
            else:
                logger.error(f"All {max_attempts} attempts failed: {type(e).__name__}: {e}")
                raise e
