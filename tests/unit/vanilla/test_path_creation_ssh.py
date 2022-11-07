import gcsfs
import pytest
import os
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.ftp import FTPFileSystem
from transparentpath import TransparentPath
from ..functions import init, reinit, skip_gcs, get_prefixes, bucket


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind", ["ssh"],
)
def test_set_global_fs_then_root_path(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return

    init(fs_kind)
    str_prefix, pathlib_prefix = get_prefixes(fs_kind)
    p = TransparentPath("chien")
    p2 = p / ".."
    assert str(p2) == str_prefix
    p2 = TransparentPath()
    assert str(p2) == str_prefix
    p2 = TransparentPath("/")
    if fs_kind == "ssh":
        assert str(p2) == "/"
    else:
        assert str(p2) == str_prefix


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, global_init, expected_fs_kind, expected_fs_type, args, kwargs",
    [
        ("ssh", True, "ssh", FTPFileSystem, ("chien/lion",), {"fs": "ssh"}),
        ("ssh", True, "ssh", FTPFileSystem, ("chien/lion",), {}),
        ("ssh", False, "ssh", FTPFileSystem, ("chien/lion",), {"fs": "ssh"}),
        ("ssh", False, "ssh", FTPFileSystem, ("chien/lion",), {}),
    ],
)
def test_path_success(fs_kind, global_init, expected_fs_kind, expected_fs_type, args, kwargs):
    if skip_gcs[fs_kind]:
        print("skipped")
        return

    if global_init:
        init(fs_kind)

    str_prefix, pathlib_prefix = get_prefixes(fs_kind)

    p = TransparentPath(*args, **kwargs)
    assert str(p.path) == f"{pathlib_prefix}/{args[0]}"
    assert str(p) == f"{str_prefix}/{args[0]}"
    assert p.__fspath__() == f"{str_prefix}/{args[0]}"

    assert fs_kind in p.fs_kind
    if global_init:
        for fs_name in TransparentPath.fss:
            if expected_fs_kind in fs_name:
                expected_fs_kind = fs_name
                break
        assert p.fs == TransparentPath.fss[expected_fs_kind]
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 2
        assert TransparentPath.fs_kind == fs_kind
        assert list(TransparentPath.fss.keys())[-1] == expected_fs_kind
        assert isinstance(TransparentPath.fss[expected_fs_kind], expected_fs_type)
    else:
        assert TransparentPath.unset
