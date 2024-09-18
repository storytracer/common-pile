from time import sleep
from pyrate_limiter import Limiter, RequestRate, Duration, BucketFullException
from requests_cache import CachedSession, NEVER_EXPIRE
from licensed_pile import logs

# Cached session for resumability
session = CachedSession('data/cache', expire_after=NEVER_EXPIRE, backend='filesystem')  # Cache never expires, delete folder to clear

# Define rate limits: 1000 requests per hour and 1 request every 3 seconds
rate_limit_seconds = RequestRate(1, Duration.SECOND * 3)
rate_limit_hour = RequestRate(1000, Duration.HOUR)
limiter = Limiter(rate_limit_seconds, rate_limit_hour)

def api_query(endpoint, headers, params):
    logger = logs.get_logger("usgpo")
    
    # Make the request using the cached session
    while True:
        try:
            # Make the request, check cache
            response = session.get(endpoint, headers=headers, params=params)

            # If the response is not from cache, enforce rate limits
            if not response.from_cache:
                # Enforce rate limits if not a cache hit
                limiter.try_acquire('govinfo')

                if response.status_code == 429:
                    logger.info("Received 429 rate-limit, waiting and retrying...")
                    # Sleep for the retry-after time if provided by the server, otherwise sleep for an hour
                    retry_after = int(response.headers.get('Retry-After', 3600))
                    sleep(retry_after)
                    continue  # Retry the request after sleeping
            
            return response

        except BucketFullException as e:
            # If rate limit exceeded, wait until it's safe to retry
            wait_time = round(e.meta_info['remaining_time'])  # Remaining time in seconds
            #logger.info(f"Rate limit hit, sleeping for {wait_time} seconds")
            sleep(wait_time)

        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise