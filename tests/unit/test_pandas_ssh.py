import pytest
import importlib.util
from .functions import get_reqs, get_path
from pathlib import Path
from transparentpath import TransparentPath

requirements = get_reqs(Path(__file__).stem.split("test_")[1])

reqs_ok = True
for req in requirements:
    if importlib.util.find_spec(req) is None:
        reqs_ok = False
        break


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.mark.parametrize("fs_kind", ["ssh"])
def test_csv(fs_kind):
    if reqs_ok is False:
        pcsv = get_path(fs_kind, ".csv")
        with pytest.raises(ImportError):
            pcsv.read()
    else:
        # noinspection PyUnresolvedReferences
        import pandas as pd

        df_csv = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])
        # noinspection PyTypeChecker
        pcsv = get_path(fs_kind, ".csv")
        if pcsv == "skipped":
            return
        pcsv.write(df_csv)
        assert pcsv.is_file()
        pd.testing.assert_frame_equal(df_csv, pcsv.read(index_col=0))


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.mark.parametrize(
    "path,enable_caching,fs,bucket,mod",
    [
        ("chien/chat.csv", True, "ssh", None, "ram"),
    ],
)
def test_reload_from_write(path, enable_caching, fs, bucket, mod):
    """
    testing unload and read with args/kwargs
    """
    if reqs_ok:
        import pandas as pd

        tp = TransparentPath(path, enable_caching=enable_caching, fs=fs, bucket=bucket)

        chat = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])
        TransparentPath.caching = mod
        path = tp
        path.write(chat)
        path.read(index_col=0)
        if mod == "ram":
            data_to_test = TransparentPath.cached_data_dict[path.__hash__()]["data"]
        else:
            data_to_test = TransparentPath(
                TransparentPath.cached_data_dict[path.__hash__()]["file"].name, fs="local"
            ).read(index_col=0)
        assert all(data_to_test == chat)
        paschat = pd.DataFrame(columns=["bar", "foo"], index=["a", "b"], data=[[5, 7], [6, 2]])
        path.write(paschat)
        if mod == "ram":
            data_to_test = TransparentPath.cached_data_dict[path.__hash__()]["data"]
        else:
            data_to_test = TransparentPath(
                TransparentPath.cached_data_dict[path.__hash__()]["file"].name, fs="local"
            ).read(index_col=0)
        assert all(data_to_test == paschat)
