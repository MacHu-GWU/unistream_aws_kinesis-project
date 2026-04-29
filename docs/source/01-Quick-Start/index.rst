Quick Start
==============================================================================

This guide walks you through producing and consuming records on a real
AWS Kinesis Data Stream using ``unistream_aws_kinesis``.


Prerequisites
------------------------------------------------------------------------------

- Python 3.10+
- An AWS account with permissions to create and delete Kinesis streams.
- The ``AWS_PROFILE`` environment variable set to a valid profile name
  (you can put it in the ``.env`` file at the project root).


Project Layout
------------------------------------------------------------------------------

The Quick Start consists of five scripts:

.. list-table::
    :header-rows: 1
    :widths: 20 80

    * - File
      - Purpose
    * - ``shared.py``
      - AWS session, stream configuration, shared data model, and
        ``setup()`` / ``teardown()`` helpers.
    * - ``setup.py``
      - Creates the Kinesis stream (provisioned, 1 shard).
    * - ``teardown.py``
      - Deletes the Kinesis stream.
    * - ``producer.py``
      - Sends one record every 5 seconds.
    * - ``consumer.py``
      - Polls the stream every 1 second and prints each record.


Step 1 - Shared Settings and Data Model
------------------------------------------------------------------------------

``shared.py`` defines everything that both the producer and the consumer need:
the boto3 session, the stream name, the record class, and convenience functions
to create / destroy the stream.

.. literalinclude:: shared.py
    :language: python
    :caption: shared.py


Step 2 - Create the Stream
------------------------------------------------------------------------------

Run ``setup.py`` to create a single-shard, provisioned Kinesis stream.
The script blocks until the stream reaches the ``ACTIVE`` state.

.. literalinclude:: setup.py
    :language: python
    :caption: setup.py

.. code-block:: console

    $ python setup.py
    Creating stream 'unistream_aws_kinesis_quick_start' (shards=1) ...
    Stream is ACTIVE.


Step 3 - Start the Consumer
------------------------------------------------------------------------------

Open **Terminal 1** and start the consumer. It discovers the shard automatically,
creates a local file-based checkpoint, and begins polling.

AWS Kinesis allows up to **5** ``GetRecords`` calls per second per shard, so the
1-second polling interval is well within the rate limit.

.. literalinclude:: consumer.py
    :language: python
    :caption: consumer.py

.. code-block:: console

    $ python consumer.py
    Consuming from shard: shardId-000000000000
    Consumer started. Polling every 1 second. Press Ctrl-C to stop.

    ... (waiting for records)


Step 4 - Start the Producer
------------------------------------------------------------------------------

Open **Terminal 2** and start the producer. It sends one record every 5 seconds.
Records are buffered locally (write-ahead log) and flushed to Kinesis in batches
of 3.

.. literalinclude:: producer.py
    :language: python
    :caption: producer.py

.. code-block:: console

    $ python producer.py
    Producer started. Sending one record every 5 seconds. Press Ctrl-C to stop.

      [1] put record id=a1b2c3d4-... tag=e5f6a7b8
      [2] put record id=d9e0f1a2-... tag=c3d4e5f6
      [3] put record id=b7c8d9e0-... tag=a1b2c3d4
      [4] put record id=f1a2b3c4-... tag=d5e6f7a8
      [5] put record id=e9f0a1b2-... tag=c3d4e5f6
      [6] put record id=a3b4c5d6-... tag=e7f8a9b0
    ^C
    Stopped after 6 records.

Meanwhile, **Terminal 1** (consumer) will start printing received records:

.. code-block:: console

    ... (consumer output continues)
      received: ith=1  id=a1b2c3d4-...  tag=e5f6a7b8
      received: ith=2  id=d9e0f1a2-...  tag=c3d4e5f6
      received: ith=3  id=b7c8d9e0-...  tag=a1b2c3d4
      received: ith=4  id=f1a2b3c4-...  tag=d5e6f7a8
      received: ith=5  id=e9f0a1b2-...  tag=c3d4e5f6
      received: ith=6  id=a3b4c5d6-...  tag=e7f8a9b0
    ^C
    Consumer stopped.

.. note::

    Records arrive at the consumer only after the producer's buffer flushes
    (every 3 records by default). You may see a short delay before the first
    batch appears.


Step 5 - Tear Down
------------------------------------------------------------------------------

Once you are done, delete the stream to avoid ongoing charges.

.. literalinclude:: teardown.py
    :language: python
    :caption: teardown.py

.. code-block:: console

    $ python teardown.py
    Deleting stream 'unistream_aws_kinesis_quick_start' ...
    Stream deleted.
