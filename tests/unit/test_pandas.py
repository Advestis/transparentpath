import sys
import pytest
import importlib.util
from importlib import reload
from .functions import init, skip_gcs
from transparentpath import TransparentPath


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.mark.parametrize(
    "fs_kind",
    [
        "local", "gcs"
    ]
)
def test_csv(clean, fs_kind):
    print("test_csv", fs_kind)
    if importlib.util.find_spec("pandas") is None:
        pcsv = get_path(fs_kind)
        with pytest.raises(ImportError):
            pcsv.write("coucou")
        with pytest.raises(ImportError):
            pcsv.read()
    else:
        import pandas as pd
        df_csv = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])
        # noinspection PyTypeChecker
        pcsv = get_path(fs_kind)
        if pcsv == "skipped":
            return
        print(TransparentPath.token)
        pcsv.write(df_csv)
        assert pcsv.is_file()
        pd.testing.assert_frame_equal(df_csv, pcsv.read(index_col=0))


def get_path(fs_kind):
    print("get_path", fs_kind)
    reload(sys.modules["transparentpath"])
    if skip_gcs[fs_kind]:
        print("skipped")
        return "skipped"
    init(fs_kind)

    TransparentPath.show_state()
    pcsv = TransparentPath("chien.csv", token=TransparentPath.token)
    print(pcsv.buckets)
    import pandas as pd
    df_csv = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])
    pcsv.write(df_csv)
    pcsv.rm(absent="ignore", ignore_kind=True)
    assert not pcsv.is_file()
    return pcsv
