from transparentpath import Path
import pytest
import pandas as pd


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.mark.parametrize("suffix,data,kwargs", [
    (".csv", pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]]), {"index_col": 0}),
])
def test_caching_ram(clean, suffix, data, kwargs):
    Path.caching = "ram"
    path = Path(f"tests/data/chien{suffix}", enable_caching=True, fs="local")
    path.read(**kwargs)
    assert all(Path.cached_data_dict[path.__hash__()]["data"] == data)
    assert all(Path(f"tests/data/chien{suffix}", enable_caching=True, fs="local").read() == data)


# noinspection PyUnusedLocal,PyShadowingNames
def test_max_size(clean):
    """
    testing behavior when reatching maximum size
    """
    Path.caching_max_memory = 0.000070
    Path("tests/data/chat.txt", enable_caching=True, fs="local").read()
    path = Path("tests/data/groschat.txt", enable_caching=True, fs="local")
    path.read()
    ret = list(Path.cached_data_dict.keys())
    retdata = list(Path.cached_data_dict.values())
    assert len(ret) == 1
    assert ret[0] == path.__hash__()
    assert retdata[0]["data"] == "grostest"


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.mark.parametrize("TP,mod", [(Path("tests/data/chat.csv", enable_caching=True, fs="local"), "ram"),
                                    (Path("chat.csv", enable_caching=True, fs="gcs", bucket="code_tests_sand"), "tmpfile"),
                                    (Path("chat.csv", enable_caching=True, fs="gcs", bucket="code_tests_sand"), "ram")])
def test_reload_from_write(clean, TP,mod):
    """
    testing unload and read with args/kwargs
    """
    chat = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])
    Path.caching = mod
    path = TP
    path.write(chat)
    path.read(index_col=0)
    if mod == "ram":
        data_to_test = Path.cached_data_dict[path.__hash__()]["data"]
    else:
        data_to_test = Path(Path.cached_data_dict[path.__hash__()]["file"].name, fs="local").read(index_col=0)
    assert all(data_to_test == chat)
    paschat = pd.DataFrame(columns=["bar", "foo"], index=["a", "b"], data=[[5, 7], [6, 2]])
    path.write(paschat)
    if mod == "ram":
        data_to_test = Path.cached_data_dict[path.__hash__()]["data"]
    else:
        data_to_test = Path(Path.cached_data_dict[path.__hash__()]["file"].name, fs="local").read(index_col=0)
    assert all(data_to_test == paschat)
