import sys
import pytest
import importlib.util
from importlib import reload
from .functions import init, skip_gcs
from transparentpath import TransparentPath
from pathlib import Path

requirements = []
for s in Path("pandas-requirements.txt").read_text().splitlines():
    s = s.split("==")[0] if "==" in s else s
    s = s.split("<=")[0] if "==" in s else s
    s = s.split(">=")[0] if "==" in s else s
    s = s.split("<")[0] if "==" in s else s
    s = s.split(">")[0] if "==" in s else s
    requirements.append(s)

reqs_ok = True
for req in requirements:
    if importlib.util.find_spec(req) is None:
        reqs_ok = False
        break


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.mark.parametrize(
    "fs_kind",
    [
        "local", "gcs"
    ]
)
def test_csv(clean, fs_kind):
    if reqs_ok is False:
        pcsv = get_path(fs_kind)
        with pytest.raises(ImportError):
            pcsv.read()
    else:
        # noinspection PyUnresolvedReferences
        import pandas as pd
        df_csv = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])
        # noinspection PyTypeChecker
        pcsv = get_path(fs_kind)
        if pcsv == "skipped":
            return
        pcsv.write(df_csv)
        assert pcsv.is_file()
        pd.testing.assert_frame_equal(df_csv, pcsv.read(index_col=0))


def get_path(fs_kind):
    reload(sys.modules["transparentpath"])
    if skip_gcs[fs_kind]:
        print("skipped")
        return "skipped"
    init(fs_kind)

    if fs_kind == "local":
        pcsv = TransparentPath("tests/data/chien.csv")
    else:
        local_path = TransparentPath("tests/data/chien.csv", fs_kind="local")
        pcsv = TransparentPath("chien.csv")
        local_path.put(pcsv)

    return pcsv
