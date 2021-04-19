import gcsfs
import pytest
import os
from fsspec.implementations.local import LocalFileSystem
from transparentpath import TransparentPath, TPValueError, TPNotADirectoryError
from ..functions import init, reinit, skip_gcs, get_prefixes, bucket


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind", ["local", "gcs"],
)
def test_set_global_fs_then_root_path(clean, fs_kind):
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
    if fs_kind == "local":
        assert str(p2) == "/"
    else:
        assert str(p2) == str_prefix


# noinspection PyUnusedLocal
def test_set_global_fs_then_path_with_gs_failed(clean):
    if skip_gcs["gcs"]:
        print("skipped")
        return

    init("gcs")
    with pytest.raises(TPValueError):
        TransparentPath(f"gs://{bucket + 'chat'}/chien", bucket=bucket)

    with pytest.raises(TPNotADirectoryError):
        TransparentPath(f"gs://{bucket + 'chat'}/chien")

    with pytest.raises(TPValueError):
        TransparentPath(f"gs://{bucket}/chien", fs="local")


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, global_init, expected_fs_kind, expected_fs_type, args, kwargs",
    [
        ("gcs", False, "gcs_sandbox-281209", gcsfs.GCSFileSystem, (f"gs://{bucket}/chien",), {}),
        ("gcs", False, "gcs_sandbox-281209", gcsfs.GCSFileSystem, ("chien",), {"fs": "gcs", "bucket": bucket}),
        ("local", True, "local", LocalFileSystem, ("chien",), {"fs": "local"}),
        ("local", True, "local", LocalFileSystem, ("chien",), {}),
        ("local", False, "local", LocalFileSystem, ("chien",), {"fs": "local"}),
        ("local", False, "local", LocalFileSystem, ("chien",), {}),
        ("gcs", True, "gcs_sandbox-281209", gcsfs.GCSFileSystem, ("chien",), {"fs": "gcs", "bucket": bucket}),
        ("gcs", True, "gcs_sandbox-281209", gcsfs.GCSFileSystem, (f"gs://{bucket}/chien",), {}),
        ("gcs", False, "gcs_sandbox-281209", gcsfs.GCSFileSystem, ("chien",), {"fs": "gcs", "bucket": bucket}),
        ("gcs", False, "gcs_sandbox-281209", gcsfs.GCSFileSystem, (f"gs://{bucket}/chien",), {}),
    ],
)
def test_path_success(clean, fs_kind, global_init, expected_fs_kind, expected_fs_type, args, kwargs):
    if skip_gcs[fs_kind]:
        print("skipped")
        return

    if global_init:
        init(fs_kind)

    str_prefix, pathlib_prefix = get_prefixes(fs_kind)

    p = TransparentPath(*args, **kwargs)
    if "gs://" not in args[0]:
        assert str(p.path) == f"{pathlib_prefix}/{args[0]}"
        assert str(p) == f"{str_prefix}/{args[0]}"
        assert p.__fspath__() == f"{str_prefix}/{args[0]}"
    else:
        assert str(p.path) == f"{args[0].replace('gs://', '')}"
        assert str(p) == f"{args[0]}"
        assert p.__fspath__() == f"{args[0]}"

    assert fs_kind in p.fs_kind
    if global_init:
        for fs_name in TransparentPath.fss:
            if expected_fs_kind in fs_name:
                expected_fs_kind = fs_name
                break
        assert p.fs == TransparentPath.fss[expected_fs_kind]
        assert not TransparentPath.unset
        if expected_fs_kind == "local":
            assert len(TransparentPath.fss) == 1
        else:
            assert len(TransparentPath.fss) == 2
        assert TransparentPath.fs_kind == fs_kind
        assert list(TransparentPath.fss.keys())[-1] == expected_fs_kind
        assert isinstance(TransparentPath.fss[expected_fs_kind], expected_fs_type)
    else:
        assert TransparentPath.unset


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "args, kwargs",
    [
        (("chien",), {"fs": "gcs"}),
    ],
)
def test_gcs_path_without_set_global_fs_fail(clean, args, kwargs):
    if skip_gcs["gcs"]:
        print("skipped")
        return

    with pytest.raises(TPNotADirectoryError):
        TransparentPath(*args, **kwargs)


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "args, kwargs",
    [
        ((f"gs://{bucket}/chien",), {"bucket": bucket + "chien"}),
        ((f"gs://{bucket}/chien",), {"fs": "local"}),
    ],
)
def test_failed_gs_path(clean, args, kwargs):
    with pytest.raises(ValueError):
        TransparentPath(*args, **kwargs)


# noinspection PyUnusedLocal
def init_local_class_then_gcs_path(clean):
    if skip_gcs["gcs"]:
        print("skipped")
        return

    init("local")
    p = TransparentPath("chien", fs="gcs", bucket=bucket)
    assert str(p.path) == f"{bucket}/chien"
    assert str(p) == f"gs://{bucket}/chien"
    assert p.__fspath__() == f"gs://{bucket}/chien"

    assert "gcs" in p.fs_kind
    assert p.fs == TransparentPath.fss["gcs_sandbox-281209"]
    assert not TransparentPath.unset
    assert len(TransparentPath.fss) == 2
    assert TransparentPath.fs_kind == "local"
    assert "gcs_sandbox-281209" in list(TransparentPath.fss.keys()) and "local" in list(TransparentPath.fss.keys())
    assert isinstance(TransparentPath.fss["gcs_sandbox-281209"], gcsfs.GCSFileSystem)
    assert isinstance(TransparentPath.fss["local"], LocalFileSystem)
    reinit()

    init("local")
    with pytest.raises(ValueError):
        TransparentPath(f"gs://{bucket}/chien", bucket=bucket + "chien")

    with pytest.raises(ValueError):
        TransparentPath(f"gs://{bucket}/chien", fs="local", bucket=bucket)

    with pytest.raises(ValueError):
        TransparentPath(f"gs://{bucket}/chien")

    p = TransparentPath(f"gs://{bucket}/chien",)
    assert str(p.path) == f"{bucket}/chien"
    assert str(p) == f"gs://{bucket}/chien"
    assert p.__fspath__() == f"gs://{bucket}/chien"

    assert "gcs" in p.fs_kind
    assert p.fs == TransparentPath.fss["gcs_sandbox-281209"]
    assert not TransparentPath.unset
    assert len(TransparentPath.fss) == 2
    assert TransparentPath.fs_kind == "local"
    assert "gcs_sandbox-281209" in list(TransparentPath.fss.keys()) and "local" in list(TransparentPath.fss.keys())
    assert isinstance(TransparentPath.fss["gcs_sandbox-281209"], gcsfs.GCSFileSystem)
    assert isinstance(TransparentPath.fss["local"], LocalFileSystem)


# noinspection PyUnusedLocal
def init_gcs_class_then_local_path(clean):
    if skip_gcs["gcs"]:
        print("skipped")
        return

    init("gcs")
    p = TransparentPath("chien", fs="local")
    assert str(p.path) == f"{os.getcwd()}/chien"
    assert str(p) == f"{os.getcwd()}/chien"
    assert p.__fspath__() == f"{os.getcwd()}/chien"

    assert p.fs_kind == "local"
    assert p.fs == TransparentPath.fss["local"]
    assert not TransparentPath.unset
    assert len(TransparentPath.fss) == 2
    assert "gcs" in TransparentPath.fs_kind
    assert "gcs_sandbox-281209" in list(TransparentPath.fss.keys()) and "local" in list(TransparentPath.fss.keys())
    assert isinstance(TransparentPath.fss["gcs_sandbox-281209"], gcsfs.GCSFileSystem)
    assert isinstance(TransparentPath.fss["local"], LocalFileSystem)
