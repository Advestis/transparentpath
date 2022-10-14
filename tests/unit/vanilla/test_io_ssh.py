import os

import pytest

from transparentpath import TransparentPath
from ..functions import init, skip_gcs


# noinspection PyUnusedLocal
# def test_put(clean):
#     print("test_put")
#     init("ssh")
#     TransparentPath.show_state()
#     localpath = TransparentPath("chien.txt", fs_kind="local")
#     remotepath = TransparentPath("test_tp/chien.txt")
#     localpath.touch()
#     localpath.put(remotepath)
#     assert localpath.is_file()
#     assert remotepath.is_file()
#
#
# # noinspection PyUnusedLocal
# def test_get(clean):
#     print("test_get")
#     init("ssh")
#
#     localpath = TransparentPath("chien.txt", fs_kind="local")
#     remotepath = TransparentPath("test_tp/chien.txt")
#     remotepath.touch()
#     remotepath.get(localpath)
#     assert remotepath.is_file()
#     assert localpath.is_file()
#
#
# # noinspection PyUnusedLocal
# @pytest.mark.parametrize(
#     "fs_kind1, fs_kind2", [("local", "ssh"), ("ssh", "local"), ("ssh", "ssh")]
# )
# def test_mv(clean, fs_kind1, fs_kind2):
#     print("test_mv", fs_kind1, fs_kind2)
#     if fs_kind1 != "local":
#         init(fs_kind1)
#     elif fs_kind2 != "local":
#         init(fs_kind2)
#     if fs_kind1 == "local":
#         path1 = TransparentPath("chien.txt", fs_kind=fs_kind1)
#         path2 = TransparentPath("chien2.txt", fs_kind=fs_kind2)
#     else:
#         path1 = TransparentPath("test_tp/chien2.txt", fs_kind=fs_kind2)
#         path2 = TransparentPath("chien.txt", fs_kind=fs_kind1)
#
#     path1.touch()
#     path1.mv(path2)
#     assert not path1.is_file()
#     assert path2.is_file()


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


# A SUPPRIMER
@pytest.mark.parametrize(
    "fs_kind, path",
    [("ssh", "chien/chat/test1_touche5.txt"),
     ("ssh", "chien/chat/test_touche.txt")]
)
def test_touch(fs_kind, path):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p = TransparentPath(path)
    print(p.bucket)
    # if p.exists():
    #     p.rm(ignore_kind=True)
    p.touch()
#     print(p.is_file(), "TTTTTTTTTTTTTTEEEEEEEEEEEEESSSSSSSSSSTTTTTTTTTTTTTTTTT", p)
#     assert p.is_file()

# A SUPPRIMER
# @pytest.mark.parametrize(
#     "fs_kind, path, expected",
#     [("ssh", "chien/chat", True)],
# )
# def test_mkdir(clean, fs_kind, path, expected):
#     if skip_gcs[fs_kind]:
#         print("skipped")
#         return
#     init(fs_kind)
#
#     p = TransparentPath(path)
#     # print(p.fs_kind)
#     # p.mkdir()
#     # print("test", p.exists(), "teeeeeeest")
#     # print(p.is_dir())
#     assert p.is_dir() is True

# A SUPPRIMER
# @pytest.mark.parametrize("fs_kind", ["ssh"])
# def test_walk(clean, fs_kind):
#     if skip_gcs[fs_kind]:
#         print("skipped")
#         return
#     init(fs_kind)
#
#     dic = {"chat": "file", "cheval": "dir", "cheval/chouette": "file"}
#     expected = [
#         (
#             TransparentPath("chien"),
#             [TransparentPath("chien") / "cheval"],
#             [TransparentPath("chien") / "chat"],
#         ),
#         (TransparentPath("chien") / "cheval", [], [TransparentPath("chien") / "cheval" / "chouette"]),
#     ]
#     print(expected)
#     root = TransparentPath("chien")
#     for word in dic:
#         p = root / word
#         print(p)
#         p.rm(absent="ignore", ignore_kind=True)
#     for word in dic:
#         p = root / word
#         if dic[word] == "file":
#             p.touch()
#         else:
#             p.mkdir()
#
#     print("tessst", list(root.walk()), "tessst")
#     assert list(root.walk()) == expected
#     for word in dic:
#         p = root / word
#         p.rm(absent="ignore", ignore_kind=True)

# A SUPPRIMER
# @pytest.mark.parametrize("fs_kind, ", ["ssh"])
# def test_exists(clean, fs_kind):
#     if skip_gcs[fs_kind]:
#         print("skipped")
#         return
#     init(fs_kind)
#
#     p = TransparentPath("chien/chiot")
#     p.touch()
#     assert p.exist()
#     assert p.exists()

#
# @pytest.mark.parametrize("fs_kind, ", ["ssh"])
# def test_urls(clean, fs_kind):
#     if skip_gcs[fs_kind]:
#         print("skipped")
#         return
#     init(fs_kind)
#
#     p = TransparentPath("chien/chat chat/chien chien")
#     # p = TransparentPath("chien/chat")
#     p.touch()
#     print(p.url)
#     if fs_kind == "gcs":
#         assert p.url == "https://console.cloud.google.com/storage/browser/_details/code_tests_sand/chat%20chat/" \
#                         "chien%20chien;tab=live_object?project=sandbox-281209"
#         print(p.url)
#         assert p.download == "https://storage.cloud.google.com/code_tests_sand/chat%20chat/chien%20chien"
#         print(p.download)
#         assert p.parent.url == "https://console.cloud.google.com/storage/browser/code_tests_sand/chat%20chat" \
#                                ";tab=objects?project=sandbox-281209"
#         print(p.parent.url)
#         assert p.parent.download is None
#     else:
#         if fs_kind == "local":
#             assert p.url == f"file://{str(p).replace(' ', '%20')}"
#             print(p.url)
#             assert p.parent.url == f"file://{str(p.parent).replace(' ', '%20')}"
#             print(p.parent.url)
#             assert p.download is None
#             assert p.parent.download is None
#         else:
#             assert p.url == f"sftp://{os.getenv('SSH_USERNAME')}@{os.getenv('SSH_HOST')}" \
#                             f"/home/{os.getenv('SSH_USERNAME')}/" \
#                             f"{str(p).replace(' ', '%20')}"
#             print(p.url)
#             assert p.parent.url == f"sftp://{os.getenv('SSH_USERNAME')}@{os.getenv('SSH_HOST')}" \
#                                    f"/home/{os.getenv('SSH_USERNAME')}/" \
#                                    f"{str(p).replace(' ', '%20')}"
#             print(p.parent.url)
#             assert p.download is None
#             assert p.parent.download is None
