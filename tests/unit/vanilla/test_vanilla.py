import pytest
import importlib.util
from transparentpath import TransparentPath
from transparentpath.gcsutils.transparentpath import TPMultipleExistenceError
from pathlib import Path
from ..functions import init, skip_gcs, get_reqs

requirements = get_reqs(Path(__file__).stem.split("test_")[1])

cc = "chien/chat"

for req in requirements:
    if importlib.util.find_spec(req) is None:
        raise ImportError(f"TransparentPath needs {req} package")


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
@pytest.mark.parametrize("fs_kind, excep", [("local", FileExistsError), ("gcs", TPMultipleExistenceError)])
def test_multipleexistenceerror(clean, fs_kind, excep):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    # noinspection PyTypeChecker
    with pytest.raises(excep):
        p1 = TransparentPath("chien")
        p2 = TransparentPath("chien") / "chat"
        p2.touch()
        if excep == FileExistsError:
            p1.touch()
        else:
            p1.fs.touch(p1.__fspath__())
        TransparentPath("chien").read()


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
    "fs_kind, path1, path2, expected",
    [
        ("local", "chien/chat", "chien", True),
        ("gcs", "chien/chat", "chien", True),
        ("local", "chien/chat", "chien", True),
        ("gcs", "chien/chat", "chien", True),
        ("local", "chien", "chien", False),
        ("gcs", "chien", "chien", False),
        ("local", "chien", "chien", False),
        ("gcs", "chien", "chien", False),
        ("local", "chien", "chat", False),
        ("gcs", "chien", "chat", False),
        ("local", "chien", "chat", False),
    ],
)
def test_isdir(clean, fs_kind, path1, path2, expected):
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


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, pattern, expected",
    [
        ("local", "chien/*", ["chat", "cheval"]),
        ("local", "chien/**", ["chat", "cheval", "cheval/chouette"]),
        ("gcs", "chien/*", ["chat", "cheval"]),
        ("gcs", "chien/**", ["chat", "cheval", "cheval/chouette"]),
    ],
)
def test_glob(clean, fs_kind, pattern, expected):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    dic = {"chien": "dir", "chien/chat": "file", "chien/cheval": "dir", "chien/cheval/chouette": "file"}
    root = TransparentPath()
    for word in dic:
        p = root / word
        p.rm(absent="ignore", ignore_kind=True)
    for word in dic:
        p = root / word
        if dic[word] == "file":
            p.touch()
        else:
            p.mkdir()
    print(list(TransparentPath("chien").ls()))
    content = [str(p).split("chien/")[1] for p in TransparentPath().glob(pattern)]
    assert content == expected
    for word in dic:
        p = root / word
        p.rm(absent="ignore", ignore_kind=True)


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, suffix, expected",
    [("local", ".txt", ".txt"), ("local", "txt", ".txt"), ("gcs", ".txt", ".txt"), ("gcs", "txt", ".txt")],
)
def test_with_suffix(clean, fs_kind, suffix, expected):
    if skip_gcs[fs_kind]:
        print("skipped", fs_kind, suffix, expected)
        return
    init(fs_kind)

    p = TransparentPath("chien").with_suffix(suffix)
    assert p.suffix == expected


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_ls(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    root = TransparentPath()
    (root / "chien").mkdir()
    (root / "chien" / "chat").touch()
    (root / "chien" / "cheval").mkdir()
    (root / "chien" / "cheval" / "chouette").touch()
    res = [str(p).split("chien/")[1] for p in (root / "chien").ls()]
    res.sort()
    expected = ["chat", "cheval"]
    expected.sort()
    assert res == expected


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_cd(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    root = TransparentPath()
    (root / "chien").mkdir()
    (root / "chien" / "chat").touch()
    (root / "chien" / "cheval").mkdir()
    (root / "chien" / "cheval" / "chouette").touch()
    root.cd("chien")
    assert root == TransparentPath("chien")
    root.cd("..")
    assert root == TransparentPath()


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, path", [("local", "chien/chat"), ("local", "chien"), ("gcs", "chien/chat"), ("gcs", "chien")]
)
def test_touch(clean, fs_kind, path):
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
    [("local", "chien", True), ("local", "chien/chat", True), ("gcs", "chien", False), ("gcs", "chien/chat", False)],
)
def test_mkdir(clean, fs_kind, path, expected):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    p = TransparentPath(path)
    p.mkdir()
    assert p.is_dir() is expected


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind, to_append", [("local", "chat"), ("gcs", "chat")])
def test_append(clean, fs_kind, to_append):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    assert str(TransparentPath("chien").append(to_append)) == str(TransparentPath(f"chien{to_append}"))


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_walk(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    dic = {"chien": "dir", "chien/chat": "file", "chien/cheval": "dir", "chien/cheval/chouette": "file"}
    expected = [
        (
            TransparentPath("chien"),
            [TransparentPath("chien") / "cheval"],
            [TransparentPath("chien") / "chat"],
        ),
        (TransparentPath("chien") / "cheval", [], [TransparentPath("chien") / "cheval" / "chouette"]),
    ]
    root = TransparentPath()
    for word in dic:
        p = root / word
        p.rm(absent="ignore", ignore_kind=True)
    for word in dic:
        p = root / word
        if dic[word] == "file":
            p.touch()
        else:
            p.mkdir()

    assert list((root / "chien").walk()) == expected
    for word in dic:
        p = root / word
        p.rm(absent="ignore", ignore_kind=True)


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind, ", ["local", "gcs"])
def test_exists(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    p = TransparentPath("chien")
    p.touch()
    assert p.exist()
    assert p.exists()


# noinspection PyUnusedLocal
def test_buckets(clean):
    if skip_gcs["gcs"]:
        print("skipped")
        return
    init("gcs")

    assert "code_tests_sand/" in TransparentPath().buckets
