import sys
import pytest
import importlib.util
from importlib import reload
from transparentpath import TransparentPath
from .functions import init, skip_gcs, get_reqs
from pathlib import Path

requirements = get_reqs(Path(__file__).stem.split("test_")[1])

reqs_ok = True
for req in requirements:
    if importlib.util.find_spec(req) is None:
        reqs_ok = False
        break


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.mark.parametrize(
    "fs_kind, data, use_pandas",
    [
        ("local", [0, 1], False),
        ("local", None, False),
        ("local", [0, 1], True),
        ("local", None, True),
        ("gcs", [0, 1], False),
        ("gcs", None, False),
        ("gcs", [0, 1], True),
        ("gcs", None, True),
    ]
)
def test_hdf5(clean, fs_kind, data, use_pandas):

    # No H5PY module
    if not reqs_ok:
        phdf5 = get_path(fs_kind, use_pandas, data is None)
        with pytest.raises(ImportError):
            if data is None:
                data = [0, 1]
                if use_pandas:
                    import pandas as pd
                    data = pd.Series(data)
                with phdf5.write(None, use_pandas=use_pandas) as f:
                    f["data"] = data
            else:
                if use_pandas:
                    import pandas as pd
                    data = pd.Series(data)
                # noinspection PyTypeChecker
                phdf5.write({"df1": data, "df2": 2 * data}, use_pandas=use_pandas)
        with pytest.raises(ImportError):
            phdf5.read()

    # With H5PY module
    else:
        import pandas as pd
        import numpy as np
        df_hdf5 = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])
        phdf5 = get_path(fs_kind, use_pandas, data is None)

        # No Data : only one set
        if data is None:
            with phdf5.write(None, use_pandas=use_pandas) as f:
                if use_pandas:
                    f["data"] = df_hdf5
                else:
                    f["data"] = df_hdf5.values
            assert phdf5.is_file()

            with phdf5.read(use_pandas=use_pandas) as f:
                if use_pandas:
                    df2 = f["data"]
                else:
                    df2 = np.array(f["data"])

            if use_pandas:
                pd.testing.assert_frame_equal(df_hdf5, df2)
            else:
                df2 = np.array(df2)
                np.testing.assert_equal(df_hdf5.values, df2)

        # With data : two sets
        else:
            # noinspection PyTypeChecker
            phdf5.write({"df1": df_hdf5, "df2": 2 * df_hdf5}, use_pandas=use_pandas)
            assert phdf5.is_file()
            with phdf5.read(use_pandas=use_pandas) as f:
                if use_pandas:
                    pd.testing.assert_frame_equal(df_hdf5, f["df1"])
                    # noinspection PyTypeChecker
                    pd.testing.assert_frame_equal(df_hdf5 * 2, f["df2"])
                else:
                    np.testing.assert_equal(df_hdf5.values, np.array(f["df1"]))
                    np.testing.assert_equal(df_hdf5.values * 2, np.array(f["df2"]))


def get_path(fs_kind, use_pandas, simple):
    reload(sys.modules["transparentpath"])
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    to_add = ""
    if use_pandas:
        to_add = "_pandas"
    if not simple:
        to_add = f"{to_add}_multi"

    if fs_kind == "local":
        local_path = TransparentPath(f"tests/data/chien{to_add}.hdf5")
        phdf5 = TransparentPath(f"chien.hdf5")
        local_path.cp(phdf5)
    else:
        local_path = TransparentPath(f"tests/data/chien{to_add}.hdf5", fs_kind="local")
        phdf5 = TransparentPath(f"chien.hdf5")
        local_path.put(phdf5)
    assert phdf5.is_file()
    return phdf5
