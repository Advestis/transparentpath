import sys
import pytest
import importlib.util
from importlib import reload
from .functions import init, skip_gcs
from transparentpath import TransparentPath


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.mark.parametrize(
    "fs_kind, data, use_pandas",
    [
        ("local", "yes", False),
        ("local", None, False),
        ("local", "yes", True),
        ("local", None, True),
        ("gcs", "yes", False),
        ("gcs", None, False),
        ("gcs", "yes", True),
        ("gcs", None, True),
    ]
)
def test_hdf5(clean, fs_kind, data, use_pandas):

    # No H5PY module
    if importlib.util.find_spec("h5py") is None:
        phdf5 = get_path(fs_kind)
        with pytest.raises(ImportError):
            if data is None:
                with phdf5.write(None, use_pandas=use_pandas) as f:
                    f["data"] = [0, 1]
            else:
                # noinspection PyTypeChecker
                phdf5.write({"df1": data, "df2": 2 * data}, use_pandas=use_pandas)
        with pytest.raises(ImportError):
            phdf5.read()

    # With H5PY module
    else:
        import pandas as pd
        import numpy as np
        df_hdf5 = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])
        phdf5 = get_path(fs_kind)

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


def get_path(fs_kind):
    reload(sys.modules["transparentpath"])
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    phdf5 = TransparentPath("chien.hdf5")
    phdf5.rm(absent="ignore", ignore_kind=True)
    assert not phdf5.is_file()
    return phdf5
