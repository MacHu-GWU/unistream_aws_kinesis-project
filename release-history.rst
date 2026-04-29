.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.1.1 (2026-04-29)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- First release.
- Add ``KinesisRecord`` — frozen dataclass extending ``DataClassRecord`` with ``to_put_record_data()``, ``from_get_record_data()``, and a ``partition_key`` property for custom partitioning.
- Add ``AwsKinesisStreamProducer`` — producer that sends record batches to Kinesis via ``put_records()``, with WAL-based buffering and exponential-backoff retry.
- Add ``AwsKinesisStreamConsumer`` / ``BaseAwsKinesisStreamConsumer`` — consumer that reads records from a Kinesis shard via ``get_records()`` with checkpoint-based exactly-once processing.
- Add ``KinesisStreamShard`` helper to parse ``list_shards`` API responses.
- Add ``KinesisGetRecordsResponseRecord`` helper to parse ``get_records`` API responses.
- Add ``BaseMockAwsTest`` test infrastructure for moto-based mock AWS testing with configurable mock/real switching.

**Minor Improvements**

- Add Quick Start documentation with ``literalinclude`` examples: ``shared.py``, ``setup.py``, ``teardown.py``, ``producer.py``, ``consumer.py``.
- Add ``unistream-aws-kinesis`` Claude Code agent skill for self-contained API reference.

**Miscellaneous**

- Add ``typing_extensions`` conditional dependency for ``Self`` type on Python 3.10.
