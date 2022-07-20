import pytest
import importlib.util
from .functions import get_reqs, get_path
from pathlib import Path

requirements = get_reqs(Path(__file__).stem.split("test_")[1])

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
def test_parquet(clean, fs_kind):
    if reqs_ok is False:
        pparquet = get_path(fs_kind, ".parquet")
        with pytest.raises(ImportError):
            pparquet.read()
    else:
        # noinspection PyUnresolvedReferences
        import pandas as pd
        df_parquet = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])
        # noinspection PyTypeChecker
        pparquet = get_path(fs_kind, ".parquet")
        if pparquet == "skipped":
            return
        pparquet.write(df_parquet)
        assert pparquet.is_file()
        pd.testing.assert_frame_equal(df_parquet, pparquet.read())


def test_fastparquet(clean, fs_kind):
    if reqs_ok is False:
        pparquet = get_path(fs_kind, ".parquet")
        with pytest.raises(ImportError):
            pparquet.read()
    else:
        # noinspection PyUnresolvedReferences
        import pandas as pd
        df_parquet = pd.DataFrame(columns=["timedelta", "date", "int"], index=["a", "b"],
                                  data=[
                                        ["0 days 00:07:20", "2016-01-01 20:17:48", 1],
                                        ["6 days 00:20:34", "2018-08-31 00:34:46", 4]
                                        ]
                                  )
        df_parquet['date'] = pd.to_datetime(df_parquet['date'])
        df_parquet['timedelta'] = pd.to_timedelta(df_parquet['foo'])
        # noinspection PyTypeChecker
        pparquet = get_path(fs_kind, ".parquet")
        if pparquet == "skipped":
            return
        pparquet.write(df_parquet, engine="fastparquet", compression="gzip")
        assert pparquet.is_file()
        pd.testing.assert_frame_equal(df_parquet, pparquet.read(engine="fastparquet"))
