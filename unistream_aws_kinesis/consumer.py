# -*- coding: utf-8 -*-

"""
AWS Kinesis Data Streams consumer implementation.
"""

import sys
import typing as T
import dataclasses

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from func_args.api import REQ

from unistream.abstraction import T_CHECK_POINT
from unistream.checkpoint import T_POINTER
from unistream.consumer import BaseConsumer

from .records import KinesisRecord
from .records import KinesisGetRecordsResponseRecord

if T.TYPE_CHECKING:
    from mypy_boto3_kinesis.client import KinesisClient


@dataclasses.dataclass
class KinesisStreamShard:
    """
    Represents metadata of a Kinesis Stream Shard.
    """

    ShardId: str = dataclasses.field(default=None)
    ParentShardId: str | None = dataclasses.field(default=None)
    AdjacentParentShardId: str | None = dataclasses.field(default=None)
    HashKeyRange: dict | None = dataclasses.field(default=None)
    SequenceNumberRange: dict | None = dataclasses.field(default=None)

    @classmethod
    def from_list_shards_response(cls, res: dict) -> list[Self]:
        """
        Create a list of shard objects from the ``list_shards`` API response.
        """
        shards = res.get("Shards", [])
        return [
            cls(
                ShardId=shard.get("ShardId"),
                ParentShardId=shard.get("ParentShardId"),
                AdjacentParentShardId=shard.get("AdjacentParentShardId"),
                HashKeyRange=shard.get("HashKeyRange"),
                SequenceNumberRange=shard.get("SequenceNumberRange"),
            )
            for shard in shards
        ]


@dataclasses.dataclass
class BaseAwsKinesisStreamConsumer(BaseConsumer):
    """
    Base consumer that reads records from an AWS Kinesis Data Stream.

    :param record_class: Record class used to deserialize received data.
    :param kinesis_client: A boto3 Kinesis client.
    :param stream_name: Kinesis Stream name.
    :param shard_id: Shard ID to read from.

    .. note::

        The ``delay`` parameter in :meth:`new` defaults to **1 second** because the
        ``GetRecords`` API is limited to **5 transactions per second per shard**
        (see `Kinesis Quotas <https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html>`_).
        A 1-second interval provides a comfortable margin (1 TPS vs 5 TPS limit).
        Set a smaller value (minimum ~0.2 s) for lower latency, or a larger value
        to reduce cost and throttling risk.
    """

    record_class: type[KinesisRecord] = dataclasses.field(default=REQ)
    kinesis_client: "KinesisClient" = dataclasses.field(default=REQ)
    stream_name: str = dataclasses.field(default=REQ)
    shard_id: str = dataclasses.field(default=REQ)

    @classmethod
    def new(
        cls,
        record_class: type[KinesisRecord],
        consumer_id: str,
        kinesis_client: "KinesisClient",
        stream_name: str,
        shard_id: str,
        checkpoint: T_CHECK_POINT,
        limit: int = 1000,
        exp_backoff_multiplier: int = 1,
        exp_backoff_base: int = 2,
        exp_backoff_min: int = 1,
        exp_backoff_max: int = 60,
        skip_error: bool = True,
        delay: int | float = 1,
        additional_kwargs: dict[str, T.Any] | None = None,
    ):
        if additional_kwargs is None:
            additional_kwargs = {}
        return cls(
            record_class=record_class,
            kinesis_client=kinesis_client,
            stream_name=stream_name,
            shard_id=shard_id,
            checkpoint=checkpoint,
            limit=limit,
            exp_backoff_multiplier=exp_backoff_multiplier,
            exp_backoff_base=exp_backoff_base,
            exp_backoff_min=exp_backoff_min,
            exp_backoff_max=exp_backoff_max,
            skip_error=skip_error,
            delay=delay,
            **additional_kwargs,
        )

    def get_records(
        self,
        limit: int | None = None,
    ) -> tuple[list[KinesisRecord], T_POINTER]:
        """
        Call ``kinesis_client.get_records(...)`` API to get records.
        """
        if limit is None:
            limit = self.limit
        res = self.kinesis_client.get_records(
            ShardIterator=self.checkpoint.start_pointer,
            Limit=limit,
        )
        next_pointer = res.get("NextShardIterator")
        response_records = KinesisGetRecordsResponseRecord.from_get_records_response(
            res
        )
        records = [
            self.record_class.from_get_record_data(response_record.data)
            for response_record in response_records
        ]
        return records, next_pointer


@dataclasses.dataclass
class AwsKinesisStreamConsumer(BaseAwsKinesisStreamConsumer):
    """
    Ready-to-use Kinesis consumer. Override ``process_record`` and
    ``process_failed_record`` to add your business logic.
    """
