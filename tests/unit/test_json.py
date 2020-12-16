import pytest

# noinspection PyPackageRequirements
import numpy as np

# noinspection PyPackageRequirements
import pandas as pd
from .functions import init, skip_gcs

from transparentpath import TransparentPath

dic = {"animals": {"chien": 4, "bird": 2}, "plants": {"drosera": "miam", "celeri": "beurk"}}
arr = np.array([[1, 2, 3], [4, 5, 6]])
df = pd.DataFrame(
    data=arr,
    columns=["A", "B", "C"],
    index=[10, 30],
)


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_read_json(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    if fs_kind == "local":
        data_path = TransparentPath("tests/data/chien.json")
    else:
        local_path = TransparentPath("tests/data/chien.json", fs_kind="local")
        data_path = TransparentPath("chien.json")
        local_path.put(data_path)

    res = data_path.read()
    assert res == dic


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind, data", [
        ("local", dic), ("gcs", dic),
        ("local", arr), ("gcs", arr),
        ("local", df), ("gcs", df),
    ]
)
def test_write_json(clean, fs_kind, data):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    path = TransparentPath("chien.json")
    path.write(data=data)
    assert path.is_file()
    res = path.read()
    if isinstance(data, dict):
        assert res == data
    elif isinstance(data, np.ndarray):
        assert (res == data).all()
    else:
        assert (np.array(res["data"]) == data.values).all()
        assert res["columns"] == data.columns.tolist()
        assert res["index"] == data.index.tolist()
