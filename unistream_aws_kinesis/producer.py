# -*- coding: utf-8 -*-

"""
AWS Kinesis Data Streams producer implementation.
"""

import typing as T
import dataclasses

from func_args.api import REQ

from unistream.abstraction import T_RECORD, T_BUFFER
from unistream.producer import BaseProducer, RetryConfig

from .records import KinesisRecord

if T.TYPE_CHECKING:
    from mypy_boto3_kinesis.client import KinesisClient


@dataclasses.dataclass
class AwsKinesisStreamProducer(BaseProducer):
    """
    Producer that sends records to an AWS Kinesis Data Stream.

    Uses ``kinesis_client.put_records()`` to send batches of records.

    :param kinesis_client: A boto3 Kinesis client.
    :param stream_name: The name of the Kinesis stream.
    """

    kinesis_client: "KinesisClient" = dataclasses.field(default=REQ)
    stream_name: str = dataclasses.field(default=REQ)

    @classmethod
    def new(
        cls,
        buffer: T_BUFFER,
        retry_config: RetryConfig,
        kinesis_client: "KinesisClient",
        stream_name: str,
    ):
        """
        Create an :class:`AwsKinesisStreamProducer` instance.

        :param buffer: Buffer for batching records.
        :param retry_config: Retry configuration for send failures.
        :param kinesis_client: A boto3 Kinesis client.
        :param stream_name: The name of the Kinesis stream.
        """
        return cls(
            buffer=buffer,
            retry_config=retry_config,
            kinesis_client=kinesis_client,
            stream_name=stream_name,
        )

    def send(self, records: T.List[T_RECORD]):
        """
        Send records to AWS Kinesis Data Stream via ``put_records``.
        """
        return self.kinesis_client.put_records(
            Records=[
                dict(
                    Data=record.to_put_record_data(),
                    PartitionKey=record.partition_key,
                )
                for record in records
            ],
            StreamName=self.stream_name,
        )
