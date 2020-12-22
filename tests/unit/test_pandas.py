import pandas as pd
import sys
import pytest
import importlib.util
from importlib import reload
from .functions import init, skip_gcs
from transparentpath import TransparentPath

df_csv = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.mark.parametrize(
    "fs_kind",
    [
        "local", "gcs"
    ]
)
def test_csv(clean, fs_kind):
    if importlib.util.find_spec("pandas") is None:
        pcsv = get_path(fs_kind)
        with pytest.raises(ImportError):
            pcsv.write(df_csv)
        with pytest.raises(ImportError):
            pcsv.read()
    else:
        # noinspection PyTypeChecker
        pcsv = get_path(fs_kind)
        pcsv.write(df_csv)
        assert pcsv.is_file()
        pd.testing.assert_frame_equal(df_csv, pcsv.read(index_col=0))


def get_path(fs_kind):
    reload(sys.modules["transparentpath"])
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    pcsv = TransparentPath("chien.csv")
    pcsv.rm(absent="ignore", ignore_kind=True)
    assert not pcsv.is_file()
    return pcsv
