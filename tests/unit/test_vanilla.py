import pytest

from transparentpath import TransparentPath
from transparentpath.gcsutils.transparentpath import MultipleExistenceError
from pathlib import Path
from .functions import init


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
        (str, TransparentPath, False)
    ]
)
def test_isinstance(clean, what, test_against, expected):
    assert isinstance(what(), test_against) is expected


def test_collapse_dots(clean):
    assert TransparentPath("chien/chat/../../../cheval") == TransparentPath("../cheval")


@pytest.mark.parametrize(
    "fs_kind, excep", [("local", IsADirectoryError), ("gcs", MultipleExistenceError)]
)
def test_multipleexistenceerror(clean, fs_kind, excep):
    init(fs_kind)
    # noinspection PyTypeChecker
    with pytest.raises(excep):
        TransparentPath._do_update_cache = False
        p1 = TransparentPath("chien")
        p2 = TransparentPath("chien/chat")
        p2.touch()
        p1.touch()
        TransparentPath._do_update_cache = True
        TransparentPath("chien")
