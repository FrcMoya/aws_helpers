import random

def exponential_backoff_with_jitter(
    attempt: int,
    base: float = 0.25,
    cap: float = 5.0,
) -> float:
    """
    Exponential backoff with full jitter.

    Returns the number of seconds to sleep.
    """
    max_delay = min(cap, base * (2 ** attempt))
    return random.uniform(0, max_delay)
