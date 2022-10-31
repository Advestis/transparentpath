import importlib.util
import os
from pathlib import Path

import pytest

from transparentpath import TransparentPath
from ..functions import init, skip_gcs, get_reqs

requirements = get_reqs(Path(__file__).stem.split("test_")[1])

cc = "chien/chat"

for req in requirements:
    if importlib.util.find_spec(req) is None:
        raise ImportError(f"TransparentPath needs {req} package")


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind, excep", [("ssh", FileExistsError)],
                         )
def test_multipleexistenceerror(fs_kind, excep):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    # noinspection PyTypeChecker
    with pytest.raises(excep):
        p1 = TransparentPath("chien") / "chat"
        p2 = TransparentPath("chien") / "chat" / "chien_file"
        p2.touch()
        p1.touch()


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_equal(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien/chat")
    p2 = TransparentPath("chien/chat")
    assert p1 == p2

# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_lt(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien") / "chat"
    p2 = TransparentPath("chien")
    assert p2 < p1


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_gt(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien") / "chat"
    p2 = TransparentPath("chien")
    assert p1 > p2


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_le(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien") / "chat"
    p2 = TransparentPath("chien")
    p3 = TransparentPath("chien")
    assert p2 <= p1
    assert p3 <= p2


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_gt(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien") / "chat"
    p2 = TransparentPath("chien")
    p3 = TransparentPath("chien")
    assert p1 >= p2
    assert p3 >= p2


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_contains(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien") / "chat"
    assert "chi" in p1


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_add(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien")
    assert (p1 + "chat") == TransparentPath(cc)
    assert (p1 + "/chat") == TransparentPath(cc)


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_truediv(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien")
    assert (p1 / "chat") == TransparentPath(cc)
    assert (p1 / "/chat") == TransparentPath(cc)


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_itruediv(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien")
    p1 /= "chat"
    assert p1 == TransparentPath(cc)
    p1 = TransparentPath("chien")
    p1 /= "/chat"
    assert p1 == TransparentPath(cc)


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, path1, path2, expected",
    [
        ("ssh", "chien/chat/chat", "chien/chat/chat", True),

        ("ssh", "chien/chat/chat", "chien/chat", False),

    ],
)
def test_isfile(fs_kind, path1, path2, expected):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(path1)
    p1.touch()
    p2 = TransparentPath(path2)
    assert p2.is_file() == expected

# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, path1, path2, expected",
    [
        ("ssh", "chien/chiot", "chien", True),
        ("ssh", "chien/chiot", "chien", True),
        ("ssh", "chien/chiot", "chiot", False),
    ],
)
def test_isdir(fs_kind, path1, path2, expected):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(path1)
    p1.touch()
    p2 = TransparentPath(path2)
    assert p2.is_dir() == expected


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, path1, path2, kwargs, expected",
    [
        ("ssh", "chien/chiot", "chien/chiot", {"absent": "raise", "ignore_kind": False, "recursive": False}, None),
        ("ssh", "cheval", "cheval", {"absent": "raise", "ignore_kind": False, "recursive": True}, NotADirectoryError),
        ("ssh", "chien/chiot", "chien", {"absent": "raise", "ignore_kind": False, "recursive": False},
         IsADirectoryError),
        ("ssh", "chien/chat/chien_file", "chien/chat", {"absent": "raise", "ignore_kind": True, "recursive": False},
         None),
        ("ssh", "chien/chat/chien_file", "chien/chat", {"absent": "raise", "ignore_kind": False, "recursive": True},
         None),
        ("ssh", "", "chien/ch", {"absent": "raise", "ignore_kind": False, "recursive": False}, FileNotFoundError),
        ("ssh", "", "chien/ch", {"absent": "raise", "ignore_kind": False, "recursive": True}, NotADirectoryError),
        ("ssh", "", "chien/ch", {"absent": "ignore", "ignore_kind": False, "recursive": True}, None)
    ],
)
def test_rm(fs_kind, path1, path2, kwargs, expected):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(path1)
    p2 = TransparentPath(path2)
    if path1 != "":
        p1.touch()
    if expected is not None:
        with pytest.raises(expected):
            p2.rm(**kwargs)
    else:
        p2.rm(**kwargs)
        assert not p2.exists()
#
# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, pattern, expected",
    [
        ("ssh", "chien/*",["chien", "chat", "cheval"]),
        ("ssh", "chien/**", [ "cheval/chouette"]),
    ],
)
def test_glob(fs_kind, pattern, expected):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    dic = {"chien": "dir", "chat": "file", "cheval": "dir", "cheval/chouette": "file"}
    root = TransparentPath("chien")
    for word in dic:
        p = root / word
        p.rm(absent="ignore", ignore_kind=True)
    for word in dic:
        p = root / word
        if dic[word] == "file":
            p.touch()
        else:
            p.mkdir()
    print(list(root.ls()))
    content = [str(p) for p in root.glob(pattern)]
    assert content == expected
    for word in dic:
        p = root / word
        p.rm(absent="ignore", ignore_kind=True)


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, suffix, expected",
    [
     ("ssh", ".txt", ".txt"), ("ssh", "txt", ".txt")],
)
def test_with_suffix(fs_kind, suffix, expected):
    if skip_gcs[fs_kind]:
        print("skipped", fs_kind, suffix, expected)
        return
    init(fs_kind)

    p = TransparentPath("chien").with_suffix(suffix)
    assert p.suffix == expected

# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_ls(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    root = TransparentPath("chien")
    (root / "chien").mkdir()
    (root / "chat").touch()
    (root / "cheval").mkdir()
    (root /  "cheval" / "chouette").touch()
    res = [str(p).split("chien/")[1] for p in (root).ls()]
    res.sort()
    expected = ["chien", "chat", "cheval"]
    expected.sort()
    assert res == expected

# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_cd(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    root = TransparentPath("chien")
    (root / "chien").mkdir()
    (root / "chien" / "chat").touch()
    (root / "chien" / "cheval").mkdir()
    (root / "chien" / "cheval" / "chouette").touch()
    root.cd("chien")
    assert root == TransparentPath("chien/chien")
    root.cd("..")
    assert root == TransparentPath("chien")

# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, path",
    [("ssh", "chien/chiot")]
)
def test_touch(fs_kind, path):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    p = TransparentPath(path)
    if p.exists():
        p.rm(ignore_kind=True)
    p.touch()
    assert p.is_file()

# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, path, expected",
    [("ssh", "chien/chien", True), ("ssh", "chien/chat", True)],
)
def test_mkdir(fs_kind, path, expected):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    p = TransparentPath(path)
    p.mkdir()
    assert p.is_dir() is expected


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind, to_append",
                         [("ssh", "chat")])
def test_append(fs_kind, to_append):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    assert str(TransparentPath("chien").append(to_append)) == str(TransparentPath(f"chien{to_append}"))

# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_walk(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    dic = {"chien": "dir", "chat": "file", "cheval": "dir", "cheval/chouette": "file"}
    expected = [
        (
            TransparentPath("chien"),
            [TransparentPath("chien") / "cheval", TransparentPath("chien/chien")],
            [TransparentPath("chien") / "chat"],

        ),
        (TransparentPath("chien") / "cheval",
         [],
         [TransparentPath("chien") / "cheval" / "chouette"]),
        (TransparentPath("chien/chien"), [], [])
    ]
    root = TransparentPath("chien")
    for word in dic:
        p = root / word
        p.rm(absent="ignore", ignore_kind=True)
    for word in dic:
        p = root / word
        if dic[word] == "file":
            p.touch()
        else:
            p.mkdir()

    assert list(root.walk()) == expected
    for word in dic:
        p = root / word
        p.rm(absent="ignore", ignore_kind=True)


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind, ", ["ssh"])
def test_exists(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    p = TransparentPath("chien/chiot")

    p.touch()
    assert p.exist()
    assert p.exists()


@pytest.mark.parametrize("fs_kind",["ssh"])
def test_urls(fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    p = TransparentPath("chien/chat chat/chien chien")
    p.touch()
    print(p)
    assert p.url == f"sftp://{os.getenv('SSH_USERNAME')}@{os.getenv('SSH_HOST')}" \
                    f"/home/{os.getenv('SSH_USERNAME')}/" \
                    f"{str(p).replace(' ', '%20')}"
    print(p.url, "testttt")
    assert p.parent.url == f"sftp://{os.getenv('SSH_USERNAME')}@{os.getenv('SSH_HOST')}" \
                           f"/home/{os.getenv('SSH_USERNAME')}/" \
                           f"{str(p.parent).replace(' ', '%20')}"
    print(p.parent.url)
    assert p.download is None
    assert p.parent.download is None
