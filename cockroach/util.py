import logging
import random
import time

try:
    import enum  # >= py34
except ImportError:
    import enum34 as enum  # < py34

# _RETRY_JITTER specifies random jitter to add to backoff
# durations. Specified as a percentage of the backoff.
_RETRY_JITTER = 0.15


class RetryStatus(enum.Enum):
    # BREAK indicates the retry loop is finished and should return
    # the result of the retry worker function.
    BREAK = 1
    # RESET indicates that the retry loop should be reset with
    # no backoff for an immediate retry.
    RESET = 2
    # CONTINUE indicates that the retry loop should continue with
    # another iteration of backoff / retry.
    CONTINUE = 3


class RetryMaxAttemptsError(Exception):
    """RetryMaxAttemptsError indicates max attempts were exceeded."""
    def __init__(self, max_attempts):
        self.max_attempts = max_attempts

    def __str__(self):
        return "maximum number of attempts exceeded (%d)" % self.max_attempts


class RetryOptions(object):
    """RetryOptions provides control of retry loop logic via the
    `retry_with_backoff` function.
    """
    def __init__(self, tag, backoff, max_backoff, constant, max_attempts=0, log_level=logging.INFO):
        self.tag = tag  # Tag for helpful logging of backoffs
        self.backoff = backoff  # Default retry backoff interval
        self.max_backoff = max_backoff  # Maximum retry backoff interval
        self.constant = constant  # Default backoff constant
        self.max_attempts = max_attempts  # Maximum number of attempts (0 for infinite)
        self.log_level = log_level


def retry_with_backoff(opts, fn):
    """retry_with_backoff implements retry with exponential backoff using
    the supplied options as parameters. When fn returns
    `RetryStatus.CONTINUE` and the number of retry attempts haven't
    been exhausted, fn is retried. When fn returns
    `RetryStatus.BREAK`, retry ends. As a special case, if fn returns
    `RetryStatus.RESET`, the backoff and retry count are reset to
    starting values and the next retry occurs immediately. Returns an
    error if the maximum number of retries is exceeded or if the fn
    returns an error.
    """
    backoff = opts.backoff.total_seconds()
    count = 0
    while True:
        count += 1
        status = RetryStatus.CONTINUE
        try:
            status = fn()
        except Exception:
            logging.log(opts.log_level, "%s failed an iteration", opts.tag, exc_info=True)
            raise
        if status is RetryStatus.BREAK:
            return
        elif status is RetryStatus.RESET:
            backoff = opts.backoff.total_seconds()
            wait = 0
            count = 0
            logging.log(opts.log_level, "%s failed; retrying immediately", opts.tag)
        else:
            if opts.max_attempts > 0 and count >= opts.max_attempts:
                raise RetryMaxAttemptsError(opts.max_attempts)
            logging.log(opts.log_level, "%s failed; retrying in %s", opts.tag, backoff)
            wait = backoff + backoff * _RETRY_JITTER * random.random()
            if backoff > opts.max_backoff.total_seconds():
                backoff = opts.max_backoff.total_seconds()
        # Wait before retry.
        time.sleep(wait)
