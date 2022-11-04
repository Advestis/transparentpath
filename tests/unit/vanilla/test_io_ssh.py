import os

import pytest

from transparentpath import TransparentPath
from ..functions import init, skip_gcs


# noinspection PyUnusedLocal
def test_put():
    print("test_put")
    init("ssh")
    TransparentPath.show_state()
    localpath = TransparentPath("chien.txt", fs_kind="local")
    remotepath = TransparentPath("chien/chien.txt")
    localpath.touch()
    localpath.put(remotepath)
    assert localpath.is_file()
    assert remotepath.is_file()


# noinspection PyUnusedLocal
def test_get():
    print("test_get")
    init("ssh")

    localpath = TransparentPath("chien.txt", fs_kind="local")
    remotepath = TransparentPath("chien/chien.txt")
    remotepath.touch()
    remotepath.get(localpath)
    assert remotepath.is_file()
    assert localpath.is_file()


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind1, fs_kind2", [("local", "ssh"), ("ssh", "local"), ("ssh", "ssh")]
)
def test_mv(fs_kind1, fs_kind2):
    print("test_mv", fs_kind1, fs_kind2)
    if fs_kind1 != "local":
        init(fs_kind1)
    elif fs_kind2 != "local":
        init(fs_kind2)
    if fs_kind1 == "local":
        path1 = TransparentPath("chien.txt", fs_kind=fs_kind1)
        path2 = TransparentPath("chien2.txt", fs_kind=fs_kind2)
    else:
        path1 = TransparentPath("chien/chien2.txt", fs_kind=fs_kind2)
        path2 = TransparentPath("chien.txt", fs_kind=fs_kind1)

    path1.touch()
    path1.mv(path2)
    assert not path1.is_file()
    assert path2.is_file()

# The "cp" function, does not work for the moment with ssh because the cp method in
# noinspection PyUnusedLocal
# @pytest.mark.parametrize(
#     "fs_kind1, fs_kind2", [("local", "ssh"), ("ssh", "local"), ("ssh", "ssh")]
# )
# def test_cp(clean, fs_kind1, fs_kind2):
#     print("test_cp", fs_kind1, fs_kind2)
#     if skip_gcs[fs_kind1] or skip_gcs[fs_kind2]:
#         print("skipped")
#         return
#     if fs_kind1 != "local":
#         init(fs_kind1)
#     elif fs_kind2 != "local":
#         init(fs_kind2)
#
#     path1 = TransparentPath("chien.txt", fs_kind=fs_kind1)
#     path2 = TransparentPath("chien.txt", fs_kind=fs_kind2)
#     path1.touch()
#     path1.cp(path2)
#     assert path1.is_file()
#     assert path2.is_file()
