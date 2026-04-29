# -*- coding: utf-8 -*-

"""
AWS service mocking infrastructure with configurable mock/real switching.

**Why support both mocked and real AWS in the same test class?**

- **Mocked tests (``use_mock=True``, default)** run fast, need no AWS
  credentials, and are safe to execute in CI.  They use `moto
  <https://github.com/getmoto/moto>`_ to intercept boto calls in-process.

- **Real-AWS tests (``use_mock=False``)** catch issues that moto cannot
  reproduce: IAM permission gaps, region-specific service behavior, S3
  eventual-consistency edge cases, and quota limits.  These are typically run
  as integration tests (see ``tests_int/``) against a real AWS account.

By toggling a single ``use_mock`` flag on the test class, the same test logic
can run in both modes.  This avoids maintaining two parallel test suites and
ensures that the assertions stay in sync.

**Usage pattern**::

    class TestMyFeature(BaseMockAwsTest):
        use_mock = True  # flip to False for integration testing

        @classmethod
        def setup_class_post_hook(cls):
            # create fixtures (S3 buckets, etc.) after mock/session is ready
            cls.create_s3_bucket("my-test-bucket")

        def test_something(self):
            ...
"""

import typing as T
import os
import dataclasses

import moto
import boto3
import botocore.exceptions

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass(frozen=True)
class MockAwsTestConfig:
    """
    Configuration for AWS testing with mock/real AWS service selection.
    """

    use_mock: bool = dataclasses.field()
    aws_region: str = dataclasses.field()


class BaseMockAwsTest:
    """
    Base test class providing AWS service mocking infrastructure with boto session management.
    """

    use_mock: bool = True

    @classmethod
    def create_s3_bucket(
        cls,
        bucket_name: str,
        enable_versioning: bool = False,
    ):
        """
        Create S3 bucket with optional versioning, handling existing bucket gracefully.
        """
        try:
            cls.s3_client.create_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyExists":
                pass
            else:
                raise e

        if enable_versioning:
            cls.s3_client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={"Status": "Enabled"},
            )

    @classmethod
    def setup_mock(cls, mock_aws_test_config: MockAwsTestConfig):
        """
        Initialize AWS mocking or real AWS services based on configuration.
        """
        cls.mock_aws_test_config = mock_aws_test_config
        if mock_aws_test_config.use_mock:
            cls.mock_aws = moto.mock_aws()
            cls.mock_aws.start()

        if mock_aws_test_config.use_mock:
            cls.boto_ses: boto3.Session = boto3.Session(
                region_name=mock_aws_test_config.aws_region
            )
        else:
            cls.boto_ses: boto3.Session = boto3.Session(
                profile_name=os.environ["LOCAL_AWS_PROFILE"],
                region_name=mock_aws_test_config.aws_region,
            )

        cls.s3_client: "S3Client" = cls.boto_ses.client("s3")

    @classmethod
    def setup_class_post_hook(cls):
        """
        Hook for additional test class setup after AWS mock initialization.
        """

    @classmethod
    def setup_class(cls):
        """
        Set up test class with configured AWS mock or real services.
        """
        mock_aws_test_config = MockAwsTestConfig(
            use_mock=cls.use_mock,
            aws_region="us-east-1",
        )
        cls.setup_mock(mock_aws_test_config)
        cls.setup_class_post_hook()

    @classmethod
    def teardown_class(cls):
        """
        Clean up AWS mock services and test resources.
        """
        if cls.mock_aws_test_config.use_mock:
            cls.mock_aws.stop()

