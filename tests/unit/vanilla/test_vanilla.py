import pytest
import importlib.util
from transparentpath import TransparentPath
from transparentpath.gcsutils.transparentpath import TPMultipleExistenceError
from pathlib import Path
from ..functions import init, skip_gcs, get_reqs

requirements = get_reqs(Path(__file__).stem.split("test_")[1])


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
def test_isinstance(clean, testfilename,  what, test_against, expected):
    assert isinstance(what(), test_against) is expected


# noinspection PyUnusedLocal
def test_collapse_dots(clean, testfilename):
    assert TransparentPath(f"chien{testfilename}/chat/../../../cheval") == TransparentPath("../cheval")


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind, excep", [("local", FileExistsError), ("gcs", TPMultipleExistenceError)])
def test_multipleexistenceerror(clean, testfilename,  fs_kind, excep):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    # noinspection PyTypeChecker
    with pytest.raises(excep):
        p1 = TransparentPath(f"chien{testfilename}")
        p2 = TransparentPath(f"chien{testfilename}") / "chat"
        p2.touch()
        if excep == FileExistsError:
            p1.touch()
        else:
            p1.fs.touch(p1.__fspath__())
        TransparentPath(f"chien{testfilename}").read()


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_equal(clean, testfilename,  fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(f"chien{testfilename}")
    p2 = TransparentPath(f"chien{testfilename}")
    assert p1 == p2


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_lt(clean, testfilename,  fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(f"chien{testfilename}") / "chat"
    p2 = TransparentPath(f"chien{testfilename}")
    assert p2 < p1


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_gt(clean, testfilename,  fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(f"chien{testfilename}") / "chat"
    p2 = TransparentPath(f"chien{testfilename}")
    assert p1 > p2


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_le(clean, testfilename,  fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(f"chien{testfilename}") / "chat"
    p2 = TransparentPath(f"chien{testfilename}")
    p3 = TransparentPath(f"chien{testfilename}")
    assert p2 <= p1
    assert p3 <= p2


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_gt(clean, testfilename,  fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(f"chien{testfilename}") / "chat"
    p2 = TransparentPath(f"chien{testfilename}")
    p3 = TransparentPath(f"chien{testfilename}")
    assert p1 >= p2
    assert p3 >= p2


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_contains(clean, testfilename,  fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(f"chien{testfilename}") / "chat"
    assert "chi" in p1


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_add(clean, testfilename,  fs_kind):
    cc = f"chien{testfilename}/chat"
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(f"chien{testfilename}")
    assert (p1 + "chat") == TransparentPath(cc)
    assert (p1 + "/chat") == TransparentPath(cc)


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_truediv(clean, testfilename,  fs_kind):
    cc = f"chien{testfilename}/chat"
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(f"chien{testfilename}")
    assert (p1 / "chat") == TransparentPath(cc)
    assert (p1 / "/chat") == TransparentPath(cc)


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_itruediv(clean, testfilename,  fs_kind):
    cc = f"chien{testfilename}/chat"
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    p1 = TransparentPath(f"chien{testfilename}")
    p1 /= "chat"
    assert p1 == TransparentPath(cc)
    p1 = TransparentPath(f"chien{testfilename}")
    p1 /= "/chat"
    assert p1 == TransparentPath(cc)


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, path1, path2, expected",
    [
        ("local", f"chien/chat", f"chien", True),
        ("gcs", f"chien/chat", f"chien", True),
        ("local", f"chien/chat", f"chien", True),
        ("gcs", f"chien/chat", f"chien", True),
        ("local", f"chien", f"chien", False),
        ("gcs", f"chien", f"chien", False),
        ("local", f"chien", f"chien", False),
        ("gcs", f"chien", f"chien", False),
        ("local", f"chien", "chat", False),
        ("gcs", f"chien", "chat", False),
        ("local", f"chien", "chat", False),
    ],
)
def test_isdir(clean, testfilename,  fs_kind, path1, path2, expected):
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
        ("local", f"chien{testfilename}/chat", f"chien{testfilename}/chat", True),
        ("gcs", f"chien{testfilename}/chat", f"chien{testfilename}/chat", True),
        ("local", f"chien{testfilename}/chat", f"chien{testfilename}", False),
        ("gcs", f"chien{testfilename}/chat", f"chien{testfilename}", False),
        ("local", f"chien{testfilename}", f"chien{testfilename}", True),
        ("gcs", f"chien{testfilename}", f"chien{testfilename}", True),
        ("local", f"chien{testfilename}", "chat", False),
        ("gcs", f"chien{testfilename}", "chat", False),
    ],
)
def test_isfile(clean, testfilename,  fs_kind, path1, path2, expected):
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
        ("local", f"chien{testfilename}", f"chien{testfilename}", {"absent": "raise", "ignore_kind": False, "recursive": False}, None),
        ("gcs", f"chien{testfilename}", f"chien{testfilename}", {"absent": "raise", "ignore_kind": False, "recursive": False}, None),
        ("local", f"chien{testfilename}", f"chien{testfilename}", {"absent": "raise", "ignore_kind": False, "recursive": True}, NotADirectoryError),
        ("gcs", f"chien{testfilename}", f"chien{testfilename}", {"absent": "raise", "ignore_kind": False, "recursive": True}, NotADirectoryError),
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
def test_rm(clean, testfilename,  fs_kind, path1, path2, kwargs, expected):
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
        ("local", f"chien{testfilename}/*", ["chat", "cheval"]),
        ("local", f"chien{testfilename}/**", ["chat", "cheval", "cheval/chouette"]),
        ("gcs", f"chien{testfilename}/*", ["chat", "cheval"]),
        ("gcs", f"chien{testfilename}/**", ["chat", "cheval", "cheval/chouette"]),
    ],
)
def test_glob(clean, testfilename,  fs_kind, pattern, expected):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    dic = {f"chien{testfilename}": "dir", f"chien{testfilename}/chat": "file", f"chien{testfilename}/cheval": "dir", f"chien{testfilename}/cheval/chouette": "file"}
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
    print(list(TransparentPath(f"chien{testfilename}").ls()))
    content = [str(p).split(f"chien{testfilename}/")[1] for p in TransparentPath().glob(pattern)]
    assert content == expected
    for word in dic:
        p = root / word
        p.rm(absent="ignore", ignore_kind=True)


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, suffix, expected",
    [("local", ".txt", ".txt"), ("local", "txt", ".txt"), ("gcs", ".txt", ".txt"), ("gcs", "txt", ".txt")],
)
def test_with_suffix(clean, testfilename,  fs_kind, suffix, expected):
    if skip_gcs[fs_kind]:
        print("skipped", fs_kind, suffix, expected)
        return
    init(fs_kind)

    p = TransparentPath(f"chien{testfilename}").with_suffix(suffix)
    assert p.suffix == expected


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_ls(clean, testfilename,  fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    root = TransparentPath()
    (root / f"chien{testfilename}").mkdir()
    (root / f"chien{testfilename}" / "chat").touch()
    (root / f"chien{testfilename}" / "cheval").mkdir()
    (root / f"chien{testfilename}" / "cheval" / "chouette").touch()
    res = [str(p).split(f"chien{testfilename}/")[1] for p in (root / f"chien{testfilename}").ls()]
    res.sort()
    expected = ["chat", "cheval"]
    expected.sort()
    assert res == expected


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_cd(clean, testfilename,  fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    root = TransparentPath()
    (root / f"chien{testfilename}").mkdir()
    (root / f"chien{testfilename}" / "chat").touch()
    (root / f"chien{testfilename}" / "cheval").mkdir()
    (root / f"chien{testfilename}" / "cheval" / "chouette").touch()
    root.cd(f"chien{testfilename}")
    assert root == TransparentPath(f"chien{testfilename}")
    root.cd("..")
    assert root == TransparentPath()


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, path", [("local", f"chien{testfilename}/chat"), ("local", f"chien{testfilename}"), ("gcs", f"chien{testfilename}/chat"), ("gcs", f"chien{testfilename}")]
)
def test_touch(clean, testfilename,  fs_kind, path):
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
    [("local", f"chien{testfilename}", True), ("local", f"chien{testfilename}/chat", True), ("gcs", f"chien{testfilename}", False), ("gcs", f"chien{testfilename}/chat", False)],
)
def test_mkdir(clean, testfilename,  fs_kind, path, expected):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    p = TransparentPath(path)
    p.mkdir()
    assert p.is_dir() is expected


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind, to_append", [("local", "chat"), ("gcs", "chat")])
def test_append(clean, testfilename,  fs_kind, to_append):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    assert str(TransparentPath(f"chien{testfilename}").append(to_append)) == str(TransparentPath(f"chien{to_append}"))


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_walk(clean, testfilename,  fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    dic = {f"chien{testfilename}": "dir", f"chien{testfilename}/chat": "file", f"chien{testfilename}/cheval": "dir", f"chien{testfilename}/cheval/chouette": "file"}
    expected = [
        (
            TransparentPath(f"chien{testfilename}"),
            [TransparentPath(f"chien{testfilename}") / "cheval"],
            [TransparentPath(f"chien{testfilename}") / "chat"],
        ),
        (TransparentPath(f"chien{testfilename}") / "cheval", [], [TransparentPath(f"chien{testfilename}") / "cheval" / "chouette"]),
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

    assert list((root / f"chien{testfilename}").walk()) == expected
    for word in dic:
        p = root / word
        p.rm(absent="ignore", ignore_kind=True)


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind, ", ["local", "gcs"])
def test_exists(clean, testfilename,  fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    p = TransparentPath(f"chien{testfilename}")
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


@pytest.mark.parametrize("fs_kind, ", ["local", "gcs"])
def test_urls(clean, testfilename,  fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    p = TransparentPath("chat chat/chien chien")
    p.touch()
    if fs_kind == "gcs":
        assert p.url == "https://console.cloud.google.com/storage/browser/_details/code_tests_sand/chat%20chat/" \
                        "chien%20chien;tab=live_object?project=sandbox-281209"
        print(p.url)
        assert p.download == "https://storage.cloud.google.com/code_tests_sand/chat%20chat/chien%20chien"
        print(p.download)
        assert p.parent.url == "https://console.cloud.google.com/storage/browser/code_tests_sand/chat%20chat" \
                               ";tab=objects?project=sandbox-281209"
        print(p.parent.url)
        assert p.parent.download is None
    else:
        assert p.url == f"file://{str(p).replace(' ', '%20')}"
        print(p.url)
        assert p.parent.url == f"file://{str(p.parent).replace(' ', '%20')}"
        print(p.parent.url)
        assert p.download is None
        assert p.parent.download is None
