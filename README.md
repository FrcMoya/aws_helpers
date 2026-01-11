# aws_helpers

Small AWS helpers.

## Kinesis

### KinesisBatchWriter

A helper class to batch and send records to an AWS Kinesis stream,  
respecting Kinesis limits and handling retries on failures.

> **NOTE:** This helper is best-effort: records may be dropped after max retries.

#### Example
```python
from aws_helpers.kinesis import KinesisBatchWriter
import boto3

client = boto3.client("kinesis")

with KinesisBatchWriter(client, "my-stream") as writer:
    writer.put_record({"message": "hello world"})
```

## License

MIT
