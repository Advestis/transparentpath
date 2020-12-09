import gcsfs
import pytest
from fsspec.implementations.local import LocalFileSystem
from transparentpath import TransparentPath
from .functions import init, reinit, skip_gcs, before_init

@pytest.mark.parametrize(
    "fs_kind, expected_fs_kind, expected_fs_type",
    [("local", "local", LocalFileSystem), ("gcs", "gcs_sandbox-281209", gcsfs.GCSFileSystem)],
)
def test_init(clean, fs_kind, expected_fs_kind, expected_fs_type):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    before_init()
    init(fs_kind)
    assert not TransparentPath.unset
    assert len(TransparentPath.fss) == 1
    assert TransparentPath.fs_kind == expected_fs_kind
    assert list(TransparentPath.fss.keys())[0] == expected_fs_kind
    assert isinstance(TransparentPath.fss[expected_fs_kind], expected_fs_type)
    reinit()


def test_init_gcs_fail(clean):
    if skip_gcs["gcs"]:
        print("skipped")
        return
    before_init()
    with pytest.raises(ValueError):
        TransparentPath.set_global_fs("gcs")
    reinit()
