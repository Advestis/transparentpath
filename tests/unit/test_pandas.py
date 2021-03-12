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
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_csv(clean, fs_kind):
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
    "suffix,kwargs", [(".csv", {"index_col": 0})],
)
def test_caching_ram(clean, suffix, kwargs):
    if reqs_ok:
        import pandas as pd

        data = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])
        TransparentPath.caching = "ram"
        path = TransparentPath(f"tests/data/chien{suffix}", enable_caching=True, fs="local")
        path.read(**kwargs)
        assert all(TransparentPath.cached_data_dict[path.__hash__()]["data"] == data)
        assert all(TransparentPath(f"tests/data/chien{suffix}", enable_caching=True, fs="local").read() == data)


# noinspection PyUnusedLocal,PyShadowingNames
def test_max_size(clean):
    """
    testing behavior when reatching maximum size
    """
    TransparentPath.caching_max_memory = 0.000070
    TransparentPath.caching = "ram"
    TransparentPath("tests/data/chat.txt", enable_caching=True, fs="local").read()
    path = TransparentPath("tests/data/groschat.txt", enable_caching=True, fs="local")
    path.read()
    ret = list(TransparentPath.cached_data_dict.keys())
    retdata = list(TransparentPath.cached_data_dict.values())
    assert len(ret) == 1
    assert ret[0] == path.__hash__()
    assert retdata[0]["data"] == "grostest"


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.mark.parametrize(
    "path,enable_caching,fs,bucket,mod",
    [
        ("tests/data/chat.csv", True, "local", None, "ram"),
        ("chat.csv", True, "gcs", "code_tests_sand", "tmpfile"),
        ("chat.csv", True, "gcs", "code_tests_sand", "ram"),
    ],
)
def test_reload_from_write(clean, path, enable_caching, fs, bucket, mod):
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
