# -*- coding: utf-8 -*-

if __name__ == "__main__":
    from unistream_aws_kinesis.tests import run_cov_test

    run_cov_test(
        __file__,
        "unistream_aws_kinesis",
        is_folder=True,
        preview=False,
    )
