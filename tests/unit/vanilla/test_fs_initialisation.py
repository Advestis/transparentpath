import gcsfs
import pytest
from fsspec.implementations.local import LocalFileSystem
from transparentpath import TransparentPath, TPNotADirectoryError
from ..functions import init, skip_gcs, reinit


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, bucket, expected_fs_name, expected_fs_kind, expected_fs_type",
    [
        ("local", None, "local", "local", LocalFileSystem),
        ("gcs", "code_tests_sand", "gcs_sandbox-281209", "gcs", gcsfs.GCSFileSystem),
        ("gcs", None, "gcs_sandbox-281209", "gcs", gcsfs.GCSFileSystem),
    ],
)
def test_init(clean, fs_kind, bucket, expected_fs_name, expected_fs_kind, expected_fs_type):
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


def test_init_gcs_fail():  # Do not use clean : we do NOT want to execute the rm for fs will have FAILED to setup
    if skip_gcs["gcs"]:
        print("skipped")
        return

    reinit()
    with pytest.raises(TPNotADirectoryError):
        TransparentPath.set_global_fs("gcs", bucket="coucou")
    reinit()
