# -*- coding: utf-8 -*-

"""
Quick Start — Producer

Sends one record every 5 seconds to the Kinesis stream, indefinitely.
Each record carries a sequential ``ith`` counter and a random ``tag``.

Usage::

    python docs/source/01-Quick-Start/producer.py

Press Ctrl-C to stop.
"""

import time
import shutil
from pathlib import Path

from unistream.buffers.file_buffer import FileBuffer
from unistream.producer import RetryConfig

from unistream_aws_kinesis.api import AwsKinesisStreamProducer

from shared import MyRecord
from shared import STREAM_NAME
from shared import kinesis_client

# --- Buffer directory ---
dir_demo = Path(__file__).absolute().parent / ".producer_data"
shutil.rmtree(dir_demo, ignore_errors=True)
dir_demo.mkdir(parents=True, exist_ok=True)
path_wal = dir_demo / "buffer.log"

# --- Create producer ---
producer = AwsKinesisStreamProducer.new(
    buffer=FileBuffer.new(
        record_class=MyRecord,
        path_wal=path_wal,
        max_records=3,
    ),
    retry_config=RetryConfig(exp_backoff=[1, 2, 4, 8]),
    kinesis_client=kinesis_client,
    stream_name=STREAM_NAME,
)

# --- Run ---
print("Producer started. Sending one record every 5 seconds. Press Ctrl-C to stop.\n")
ith = 0
try:
    while True:
        ith += 1
        record = MyRecord(ith=ith)
        producer.put(record, verbose=True)
        print(f"  [{ith}] put record id={record.id} tag={record.tag}")
        time.sleep(5)
except KeyboardInterrupt:
    print(f"\nStopped after {ith} records.")
