import pytest

from transparentpath import TransparentPath
from transparentpath.gcsutils.transparentpath import MultipleExistenceError
from pathlib import Path
from .functions import init, skip_gcs

cc = "chien/chat"


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "what, test_against, expected",
    [
        (TransparentPath, TransparentPath, True),
        (TransparentPath, Path, False),
        (TransparentPath, str, True),
        (Path, TransparentPath, False),
        (Path, str, False),
        (Path, Path, True),
        (str, str, True),
        (str, Path, False),
        (str, TransparentPath, False),
    ],
)
def test_isinstance(clean, what, test_against, expected):
    assert isinstance(what(), test_against) is expected


# noinspection PyUnusedLocal
def test_collapse_dots(clean):
    assert TransparentPath("chien/chat/../../../cheval") == TransparentPath("../cheval")


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind, excep", [("local", IsADirectoryError), ("gcs", MultipleExistenceError)])
def test_multipleexistenceerror(clean, fs_kind, excep):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    # noinspection PyTypeChecker
    with pytest.raises(excep):
        TransparentPath._do_update_cache = False
        p1 = TransparentPath("chien")
        p2 = TransparentPath("chien") / "chat"
        p2.touch()
        p1.touch()
        TransparentPath._do_update_cache = True
        TransparentPath("chien")


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_equal(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien")
    p2 = TransparentPath("chien")
    assert p1 == p2


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_lt(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien") / "chat"
    p2 = TransparentPath("chien")
    assert p2 < p1


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_gt(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien") / "chat"
    p2 = TransparentPath("chien")
    assert p1 > p2


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_le(clean, fs_kind):
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
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_gt(clean, fs_kind):
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
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_contains(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien") / "chat"
    assert "chi" in p1


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_add(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien")
    assert (p1 + "chat") == TransparentPath(cc)
    assert (p1 + "/chat") == TransparentPath(cc)


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_truediv(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath("chien")
    assert (p1 / "chat") == TransparentPath(cc)
    assert (p1 / "/chat") == TransparentPath(cc)


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_itruediv(clean, fs_kind):
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
    "fs_kind, path1, path2, exists, expected",
    [
        ("local", "chien/chat", "chien", True, True),
        ("gcs", "chien/chat", "chien", True, True),
        ("local", "chien/chat", "chien", False, True),
        ("gcs", "chien/chat", "chien", False, True),

        ("local", "chien", "chien", True, False),
        ("gcs", "chien", "chien", True, False),
        ("local", "chien", "chien", False, False),
        ("gcs", "chien", "chien", False, False),

        ("local", "chien", "chat", True, False),
        ("gcs", "chien", "chat", True, False),
        ("local", "chien", "chat", False, False),
        ("gcs", "chien", "chat", False, True),
    ],
)
def test_isdir(clean, fs_kind, path1, path2, exists, expected):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(path1)
    p1.touch()
    p2 = TransparentPath(path2)
    assert p2.is_dir(exist=exists) == expected


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, path1, path2, expected",
    [
        ("local", "chien/chat", "chien/chat", True),
        ("gcs", "chien/chat", "chien/chat", True),
        ("local", "chien/chat", "chien", False),
        ("gcs", "chien/chat", "chien", False),

        ("local", "chien", "chien", True),
        ("gcs", "chien", "chien", True),

        ("local", "chien", "chat", False),
        ("gcs", "chien", "chat", False),
    ],
)
def test_isfile(clean, fs_kind, path1, path2, expected):
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
    "fs_kind, path1, path2, kwargs, expected",
    [
        ("local", "chien", "chien", {"absent": "raise", "ignore_kind": False, "recursive": False}, None),
        ("gcs", "chien", "chien", {"absent": "raise", "ignore_kind": False, "recursive": False}, None),

        ("local", "chien", "chien", {"absent": "raise", "ignore_kind": False, "recursive": True}, NotADirectoryError),
        ("gcs", "chien", "chien", {"absent": "raise", "ignore_kind": False, "recursive": True}, NotADirectoryError),

        ("local", "ch/chat", "ch", {"absent": "raise", "ignore_kind": False, "recursive": False}, IsADirectoryError),
        ("gcs", "ch/chat", "ch", {"absent": "raise", "ignore_kind": False, "recursive": False}, IsADirectoryError),

        ("local", "ch/chat", "ch", {"absent": "raise", "ignore_kind": True, "recursive": False}, None),
        ("gcs", "ch/chat", "ch", {"absent": "raise", "ignore_kind": True, "recursive": False}, None),

        ("local", "ch/chat", "ch", {"absent": "raise", "ignore_kind": False, "recursive": True}, None),
        ("gcs", "ch/chat", "ch", {"absent": "raise", "ignore_kind": False, "recursive": True}, None),

        ("local", "", "ch", {"absent": "raise", "ignore_kind": False, "recursive": False}, FileNotFoundError),
        ("gcs", "", "ch", {"absent": "raise", "ignore_kind": False, "recursive": False}, FileNotFoundError),

        ("local", "", "ch", {"absent": "raise", "ignore_kind": False, "recursive": True}, NotADirectoryError),
        ("gcs", "", "ch", {"absent": "raise", "ignore_kind": False, "recursive": True}, NotADirectoryError),

        ("local", "", "ch", {"absent": "ignore", "ignore_kind": False, "recursive": True}, None),
        ("gcs", "", "ch", {"absent": "ignore", "ignore_kind": False, "recursive": True}, None),
    ],
)
def test_rm(clean, fs_kind, path1, path2, kwargs, expected):
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

