# MIT License
# Copyright (c) 2026 Fran Moya

import hashlib
import random
import time
import json
import logging


class KinesisBatchWriter:
    """
    A helper class to batch and send records to an AWS Kinesis stream,
    respecting Kinesis limits and handling retries on failures.
    """

    def __init__ (self, kinesis_client, stream_name, logger=None):
        self.kinesis_client = kinesis_client
        self.stream_name = stream_name
        self.logger = logger or logging.getLogger(__name__)
        
        # AWS Kinesis limits
        self._limit_bytes_per_record = 10485760  # 10 MB
        self._limit_bytes_per_batch = 10485760  # 10 MB
        self._limit_records_per_batch = 500
        self._max_tries = 10
        self._tries_to_wait = 5

        self._records_to_send = []
        self._total_bytes_to_send = 0
        self._total_bytes_sent = 0
        self._total_batches_sent = 0
        self._total_records_sent = 0
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        Ensure any remaining records are sent when exiting the context manager.
        """
        try:
            self.send_last_batch()
        except Exception:
            self.logger.exception("Failed to flush last batch on context exit")
        
        if exc_type is not None:
            self.logger.error("Unhandled exception in context manager", exc_info=(exc_type, exc_value, exc_traceback))

    def put_record(self, data, partition_key=None):
        # Ensure data is str, bytes, or JSON-serializable
        # Normalize to bytes
        if isinstance(data, (bytes, bytearray, memoryview)):
            data_bytes = bytes(data)
        elif isinstance(data, str):
            data_bytes = data.encode("utf-8")
        else:
            try:
                data_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
            except TypeError as exc:
                raise TypeError("data must be str, bytes, or JSON-serializable") from exc

        # Generate partition key if not provided
        if partition_key is None:
            partition_key = hashlib.sha256(data_bytes).hexdigest()

        actual_data_bytes = len(data_bytes) + len(partition_key.encode("utf-8"))
        if actual_data_bytes > self._limit_bytes_per_record:
            raise ValueError(f"Record size ({actual_data_bytes} bytes) exceeds Kinesis limit of {self._limit_bytes_per_record} bytes")

        # Check if adding this record would exceed batch limits:
        if (len(self._records_to_send) + 1 > self._limit_records_per_batch or
            self._total_bytes_to_send + actual_data_bytes > self._limit_bytes_per_batch):
            self._send_batch()
        
        self._records_to_send.append({
            "Data": data_bytes,
            "PartitionKey": partition_key
        })
        self._total_bytes_to_send += actual_data_bytes

    def send_last_batch(self):
        if len(self._records_to_send) > 0:
            self._send_batch()
            self.logger.info(
                f"All records sent. Summary:\n"
                f"  Total records: {self._total_records_sent}\n"
                f"  Total batches: {self._total_batches_sent}\n"
                f"  Total bytes sent: {self._total_bytes_sent} bytes"
            )
    
    @staticmethod
    def _backoff_with_jitter(attempt, base=0.25, cap=5.0):
        max_delay = min(cap, base * (2 ** attempt))
        return random.uniform(0, max_delay)

    def _send_batch(self):
        try:
            num_records_to_send = len(self._records_to_send)
            failed_records = self._records_to_send
            tries = 0
            while failed_records:
                if tries >= self._max_tries:
                    self.logger.error(f"Max retries reached. Failed to send {len(failed_records)} records.")
                    break

                if tries > self._tries_to_wait:
                    self.logger.warning(f"Retrying to send {len(failed_records)} failed records after waiting.")
                    time.sleep(self._backoff_with_jitter(tries))
                
                response = self.kinesis_client.put_records(
                    Records=failed_records,
                    StreamName=self.stream_name
                )
                
                tries += 1

                failed_record_count = response.get('FailedRecordCount', 0)
                if failed_record_count > 0:
                    failed_indexes = [
                        i for i, record in enumerate(response['Records']) if 'ErrorCode' in record
                    ]

                    failed_records = [failed_records[i] for i in failed_indexes]
                else:
                    failed_records = []
                    break

            self._total_records_sent += num_records_to_send - len(failed_records)
            
            self.logger.info(f"Batch {self._total_batches_sent + 1} sent: {num_records_to_send} records, {self._total_bytes_to_send} bytes.")

            self._total_bytes_sent += self._total_bytes_to_send
            self._total_batches_sent += 1
            self._records_to_send.clear()
            self._total_bytes_to_send = 0
        
        except Exception:
            self.logger.exception(f"Exception occurred while sending batch: {e}")
