import os

import pytest
from fsspec.implementations.ftp import FTPFileSystem
from transparentpath import TransparentPath
from ..functions import init, skip_gcs


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, bucket, expected_fs_name, expected_fs_kind, expected_fs_type",
    [
        ("ssh", None, f"ssh_{os.getenv('SSH_HOST')}_{'SSH_USERNAME'}", "ssh", FTPFileSystem),
    ],
)
def test_init(fs_kind, bucket, expected_fs_name, expected_fs_kind, expected_fs_type):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind, bucket_=bucket)
    assert not TransparentPath.unset
    if expected_fs_kind == "local":
        assert len(TransparentPath.fss) == 1
    else:
        assert len(TransparentPath.fss) == 2
    assert expected_fs_kind == TransparentPath.fs_kind
    assert expected_fs_name in list(TransparentPath.fss.keys())[-1]
    assert isinstance(TransparentPath.fss[list(TransparentPath.fss.keys())[-1]], expected_fs_type)
