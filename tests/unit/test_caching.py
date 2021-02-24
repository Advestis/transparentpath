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
    assert all(Path.cached_data_dict[path] == data)
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
    assert ret[0] == path
    assert retdata[0] == "grostest"