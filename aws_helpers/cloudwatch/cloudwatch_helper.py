# MIT License
# Copyright (c) 2026 Fran Moya

import datetime
import logging
import time

from aws_helpers.utils.retry import exponential_backoff_with_jitter


class CloudWatchLogsBatchWriter:
    """
    A helper class to batch and send records to CloudWatch Logs,
    respecting CloudWatch Logs limits and handling retries on failures.
    """

    def __init__(self, logs_client, log_group_name, log_stream_name, logger=None):
        self.logs_client = logs_client
        self.log_group_name = log_group_name
        self.log_stream_name = log_stream_name
        self.logger = logger or logging.getLogger(__name__)

        # AWS CloudWatch Logs limits
        self._limit_bytes_per_batch = 1048576  # 1 MB
        self._limit_bytes_per_event = 1048576  # 1 MB
        self._limit_num_log_events = 10000

        # Retry configuration
        self._max_tries = 10
        self._tries_to_wait = 5

        # Internal state
        self._events_to_send = []
        self._total_bytes_to_send = 0
        self._total_bytes_sent = 0
        self._total_batches_count = 0
        self._total_events_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        Ensure any remaining records are sent when exiting the context manager.
        """
        try:
            self._send_last_batch()
        except Exception:
            self.logger.exception("Failed to flush last batch on context exit")
        
        if exc_type is not None:
            self.logger.error("Unhandled exception in context manager", exc_info=(exc_type, exc_value, exc_traceback))
    
    def put_log_event(self, message, timestamp=None):
        # Ensure message is str or bytes
        # Normalize to str
        if isinstance(message, (bytes, bytearray, memoryview)):
            message_str = message.decode("utf-8")
        elif isinstance(message, str):
            message_str = message
        else:
            raise TypeError("Log event message must be str or bytes-like object")

        if timestamp is None:
            timestamp = int(datetime.timestamp(datetime.now(datetime.timezone.utc))*1000)  # Current time in milliseconds

        event_size = len(message_str.encode("utf-8")) + 26  # Approximate size with overhead

        if event_size > self._limit_bytes_per_event:
            raise ValueError(f"Log event size {event_size} exceeds limit of {self._limit_bytes_per_event} bytes")

        if (self._total_bytes_to_send + event_size > self._limit_bytes_per_batch or
                len(self._events_to_send) >= self._limit_num_log_events):
            self._send_batch()

        self._events_to_send.append({
            'timestamp': timestamp,
            'message': message_str
        })
        self._total_bytes_to_send += event_size
    
    def _send_batch(self):
        # Sort events by timestamp as required by CloudWatch Logs
        self._events_to_send.sort(key=lambda x: x['timestamp'])

        tries = 0

        while True:
            try:
                response = self.logs_client.put_log_events(
                    logGroupName=self.log_group_name,
                    logStreamName=self.log_stream_name,
                    logEvents=self._events_to_send,
                )

                # Rejected log events are not retried (logical errors, not transient failures)
                if "rejectedLogEventsInfo" in response:
                    self.logger.error(
                        "Some log events were rejected by CloudWatch Logs: %s",
                        response["rejectedLogEventsInfo"],
                    )
                break

            except Exception:
                tries += 1

                if tries >= self._max_tries:
                    self.logger.exception(
                        "Max retries reached. Failed to send %d log events.",
                        len(self._events_to_send),
                    )
                    # Best-effort
                    return

                if tries >= self._backoff_start_attempt:
                    delay = exponential_backoff_with_jitter(tries)
                    self.logger.warning(
                        "Retrying CloudWatch Logs batch (%d/%d) after %.2fs",
                        tries,
                        self._max_tries,
                        delay,
                    )
                    time.sleep(delay)

        self._total_bytes_sent += self._total_bytes_to_send
        self._total_batches_count += 1
        self._total_events_count += len(self._events_to_send)

        self._events_to_send.clear()
        self._total_bytes_to_send = 0
    
    def _send_last_batch(self):
        if len(self._events_to_send) > 0:
            self._send_batch()

        self.logger.info(
            f"All log events sent. Summary:\n"
            f"  Total log events: {self._total_events_count}\n"
            f"  Total batches: {self._total_batches_count}\n"
            f"  Total bytes sent: {self._total_bytes_sent} bytes"
        )
