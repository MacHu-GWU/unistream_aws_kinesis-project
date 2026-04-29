# -*- coding: utf-8 -*-

from unistream_aws_kinesis import api


def test():
    _ = api


if __name__ == "__main__":
    from unistream_aws_kinesis.tests import run_cov_test

    run_cov_test(
        __file__,
        "unistream_aws_kinesis.api",
        preview=False,
    )
