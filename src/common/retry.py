import logging
import time
from functools import wraps
from typing import Callable , Type

def retry(
    retries: int = 3,
    delay: int = 5,
    retry_on: tuple[Type[Exception], ...] = (Exception,),
    logger: logging.Logger | None = None,
    ):

    """
    Retry a function when specified exceptions occur.

    Args:
        retries: Maximum number of attempts.
        delay: Delay (seconds) between retries.
        retry_on: Tuple of exception types that should trigger a retry.
        logger: Optional logger for retry messages.
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):

            attempt = 1
            while attempt <= retries:
                try:
                    return func(*args, **kwargs)
                except retry_on as exc:

                    if attempt == retries:
                        if logger:
                            logger.error("Function '%s' failed after %d attempts",
                                    func.__name__,
                                    retries,
                                    )
                        raise
                    if logger:
                        logger.warning(
                            "Attempt %d/%d failed for '%s': %s. Retrying in %d seconds...",
                            attempt,
                            retries,
                            func.__name__,
                            str(exc),
                            delay,
                        )
                    time.sleep(delay)
                    attempt += 1
        return wrapper
    return decorator
