# -*- coding: utf-8 -*-

"""
Shared settings, data model, and setup/teardown helpers for the Quick Start demo.

Prerequisites:

- Set the ``AWS_PROFILE`` environment variable (or put it in ``.env``).
- The profile must have permission to create/delete Kinesis streams.
"""

import os
import uuid
import dataclasses

import boto3

from unistream_aws_kinesis.api import KinesisRecord

# --- AWS session ---
aws_profile = os.environ["AWS_PROFILE"]
boto_ses = boto3.Session(profile_name=aws_profile)
kinesis_client = boto_ses.client("kinesis")

# --- Stream config ---
STREAM_NAME = "unistream_aws_kinesis_quick_start"
SHARD_COUNT = 1


# --- Data model ---
@dataclasses.dataclass(frozen=True)
class MyRecord(KinesisRecord):
    """
    A simple record carrying a sequence number and a random tag.
    """

    ith: int = dataclasses.field(default=0)
    tag: str = dataclasses.field(default_factory=lambda: uuid.uuid4().hex[:8])


# --- Setup / Teardown ---
def setup():
    """
    Create the Kinesis stream in PROVISIONED mode.
    Blocks until the stream becomes ACTIVE.
    """
    print(f"Creating stream {STREAM_NAME!r} (shards={SHARD_COUNT}) ...")
    kinesis_client.create_stream(
        StreamName=STREAM_NAME,
        ShardCount=SHARD_COUNT,
        StreamModeDetails={"StreamMode": "PROVISIONED"},
    )
    waiter = kinesis_client.get_waiter("stream_exists")
    waiter.wait(StreamName=STREAM_NAME)
    print("Stream is ACTIVE.")


def teardown():
    """
    Delete the Kinesis stream and wait until it is gone.
    """
    print(f"Deleting stream {STREAM_NAME!r} ...")
    kinesis_client.delete_stream(
        StreamName=STREAM_NAME,
        EnforceConsumerDeletion=True,
    )
    waiter = kinesis_client.get_waiter("stream_not_exists")
    waiter.wait(StreamName=STREAM_NAME)
    print("Stream deleted.")
