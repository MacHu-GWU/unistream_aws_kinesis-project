# -*- coding: utf-8 -*-

"""
Kinesis-specific record types for producing to and consuming from
AWS Kinesis Data Streams.
"""

import typing as T
import base64
import dataclasses

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
        cls: T.Type["T_KINESIS_RECORD"],
        data: bytes,
    ) -> "T_KINESIS_RECORD":
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


T_KINESIS_RECORD = T.TypeVar("T_KINESIS_RECORD", bound=KinesisRecord)


@dataclasses.dataclass
class KinesisGetRecordsResponseRecord:
    """
    Deserializes the ``Records`` part of the ``get_records`` API response.
    """

    sequence_number: str = dataclasses.field()
    approximate_arrival_timestamp: str = dataclasses.field()
    data: bytes = dataclasses.field()
    partition_key: str = dataclasses.field()
    encryption_type: T.Optional[str] = dataclasses.field()

    @classmethod
    def from_get_records_response(
        cls,
        res: dict,
    ) -> T.List["KinesisGetRecordsResponseRecord"]:
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
