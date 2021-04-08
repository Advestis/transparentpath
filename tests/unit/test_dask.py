import pytest
import importlib.util
from .functions import get_reqs, get_path
from pathlib import Path

requirements = get_reqs(Path(__file__).stem.split("test_")[1])

reqs_ok = True
for req in requirements:
    if "[" in req:
        main = req.split("[")[0]
        if importlib.util.find_spec(main) is None:
            reqs_ok = False
            break
        req = req.replace(main, "").replace("[", "").replace("]", "")
        req = req.split(",")
        for r in req:
            if r == "distributed":
                if importlib.util.find_spec(r) is None:
                    reqs_ok = False
                    break
            elif r == "dataframe":
                continue
            else:
                raise AssertionError(f"I do not know what to do with {r} in requirement {req}")

    else:
        if importlib.util.find_spec(req) is None:
            reqs_ok = False
            break


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.mark.parametrize(
    "fs_kind, suffix, kwargs",
    [
        ("local", ".csv", {"index_col": 0}), ("gcs", ".csv", {"index_col": 0}),
        ("local", ".parquet", {}), ("gcs", ".parquet", {}),
        ("local", ".xlsx", {"index_col": 0}), ("gcs", ".xlsx", {"index_col": 0}),
        ("local", ".hdf5", {"set_names": "data"}), ("gcs", ".hdf5", {"set_names": "data"}),
    ]
)
def test(clean, fs_kind, suffix, kwargs):
    if reqs_ok is False:
        pfile = get_path(fs_kind, suffix)
        with pytest.raises(ImportError):
            pfile.read(use_dask=True)
    else:
        # noinspection PyUnresolvedReferences
        import pandas as pd
        import dask.dataframe as dd
        df_dask = dd.from_pandas(
            pd.DataFrame(
                columns=["foo", "bar"],
                index=["a", "b"],
                data=[[1, 2], [3, 4]]
            ),
            npartitions=1
        )
        # noinspection PyTypeChecker
        pfile = get_path(fs_kind, suffix)
        if pfile == "skipped":
            return
        pfile.write(df_dask)
        assert pfile.is_file()
        pd.testing.assert_frame_equal(df_dask.head(), pfile.read(use_dask=True, **kwargs).head())
