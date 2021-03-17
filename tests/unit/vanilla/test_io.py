import pytest

from transparentpath import TransparentPath
from ..functions import init, skip_gcs


# noinspection PyUnusedLocal
def test_put(clean):
    print("test_put")
    if skip_gcs["gcs"]:
        print("skipped")
        return
    init("gcs")
    TransparentPath.show_state()
    localpath = TransparentPath("chien.txt", fs_kind="local")
    remotepath = TransparentPath("chien.txt")
    localpath.touch()
    localpath.put(remotepath)
    assert localpath.is_file()
    assert remotepath.is_file()


# noinspection PyUnusedLocal
def test_get(clean):
    print("test_get")
    if skip_gcs["gcs"]:
        print("skipped")
        return
    init("gcs")

    localpath = TransparentPath("chien.txt", fs_kind="local")
    remotepath = TransparentPath("chien.txt")
    remotepath.touch()
    remotepath.get(localpath)
    assert remotepath.is_file()
    assert localpath.is_file()


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind1, fs_kind2", [("local", "local"), ("gcs", "local"), ("local", "gcs"), ("gcs", "gcs")]
)
def test_mv(clean, fs_kind1, fs_kind2):
    print("test_mv", fs_kind1, fs_kind2)
    if skip_gcs[fs_kind1] or skip_gcs[fs_kind2]:
        print("skipped")
        return
    if fs_kind1 != "local":
        init(fs_kind1)
    elif fs_kind2 != "local":
        init(fs_kind2)

    path1 = TransparentPath("chien.txt", fs_kind=fs_kind1)
    path2 = TransparentPath("chien2.txt", fs_kind=fs_kind2)
    path1.touch()
    path1.mv(path2)
    assert not path1.is_file()
    assert path2.is_file()


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind1, fs_kind2", [("local", "local"), ("gcs", "local"), ("local", "gcs"), ("gcs", "gcs")]
)
def test_cp(clean, fs_kind1, fs_kind2):
    print("test_cp", fs_kind1, fs_kind2)
    if skip_gcs[fs_kind1] or skip_gcs[fs_kind2]:
        print("skipped")
        return
    if fs_kind1 != "local":
        init(fs_kind1)
    elif fs_kind2 != "local":
        init(fs_kind2)

    path1 = TransparentPath("chien.txt", fs_kind=fs_kind1)
    path2 = TransparentPath("chien2.txt", fs_kind=fs_kind2)
    path1.touch()
    path1.cp(path2)
    assert path1.is_file()
    assert path2.is_file()
