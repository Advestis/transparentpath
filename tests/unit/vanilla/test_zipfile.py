import zipfile
import pytest
from ..functions import init, skip_gcs

from transparentpath import TransparentPath


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_zipfile(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)
    if fs_kind == "local":
        data_path = TransparentPath("tests/data/chien.zip")
    else:
        local_path = TransparentPath("tests/data/chien.zip", fs_kind="local")
        data_path = TransparentPath("chien.zip")
        local_path.put(data_path)

    zf = zipfile.ZipFile(data_path)
    text1 = zf.open("text.txt")
    text2 = zf.open("text 2.txt")
    assert text1.read() == b'chien\n'
    assert text2.read() == b'chat\n'
