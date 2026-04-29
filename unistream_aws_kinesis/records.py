# -*- coding: utf-8 -*-

"""
Kinesis-specific record types for producing to and consuming from
AWS Kinesis Data Streams.
"""

import sys
import base64
import dataclasses

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from unistream.api import DataClassRecord


@dataclasses.dataclass(frozen=True)
class KinesisRecord(DataClassRecord):
    """
    A record designed for AWS Kinesis Data Streams. Extends
    :class:`~unistream.records.dataclass.DataClassRecord` with Kinesis-specific
    binary encoding methods.

    :param partition_key: Kinesis partition key. Defaults to ``record.id``.
    """

    def to_put_record_data(self) -> bytes:
        """
        Convert the record to binary data for the ``put_records`` API.
        """
        return base64.b64encode(self.serialize().encode("utf-8"))

    @classmethod
    def from_get_record_data(
        cls,
        data: bytes,
    ) -> Self:
        """
        Convert ``get_records`` API response data to a record instance.
        """
        return cls.deserialize(base64.b64decode(data).decode("utf-8"))

    @property
    def partition_key(self) -> str:
        """
        Kinesis partition key. Override this property for custom partitioning.
        Defaults to ``self.id``.
        """
        return self.id


@dataclasses.dataclass
class KinesisGetRecordsResponseRecord:
    """
    Deserializes the ``Records`` part of the ``get_records`` API response.
    """

    sequence_number: str = dataclasses.field()
    approximate_arrival_timestamp: str = dataclasses.field()
    data: bytes = dataclasses.field()
    partition_key: str = dataclasses.field()
    encryption_type: str | None = dataclasses.field()

    @classmethod
    def from_get_records_response(
        cls,
        res: dict,
    ) -> list[Self]:
        """
        Parse the ``Records`` part of the ``get_records`` API response.
        """
        records = list()
        for record in res.get("Records", []):
            records.append(
                cls(
                    sequence_number=record["SequenceNumber"],
                    approximate_arrival_timestamp=record[
                        "ApproximateArrivalTimestamp"
                    ],
                    data=record["Data"],
                    partition_key=record["PartitionKey"],
                    encryption_type=record.get("EncryptionType"),
                )
            )
        return records
