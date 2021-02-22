import pytest
import sys
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


dic = {"animals": {"chien": 4, "bird": 2}, "plants": {"drosera": "miam", "celeri": "beurk"}}


# noinspection PyUnusedLocal
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_read_json(clean, fs_kind):
    reload(sys.modules["transparentpath"])
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

    if reqs_ok:
        res = data_path.read()
        assert res == dic
    else:
        with pytest.raises(ImportError):
            data_path.read()


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind", ["local", "gcs"]
)
def test_write_dict(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    path = get_path(fs_kind)
    data = dic
    if reqs_ok:
        path.write(data=data)
        assert path.is_file()
        res = path.read()
        assert res == data
    else:
        with pytest.raises(ImportError):
            path.write(data=data)


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind", ["local", "gcs"]
)
def test_write_numpy(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    path = get_path(fs_kind)
    if reqs_ok:
        # noinspection PyUnresolvedReferences
        import numpy as np
        data = np.array([[1, 2, 3], [4, 5, 6]])
        path.write(data=data)
        assert path.is_file()
        res = path.read()
        # noinspection PyUnresolvedReferences
        assert (res == data).all()


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "fs_kind", ["local", "gcs"]
)
def test_write_pandas(clean, fs_kind):
    if skip_gcs[fs_kind]:
        print("skipped")
        return
    init(fs_kind)

    path = get_path(fs_kind)
    if importlib.util.find_spec("pandas") is not None and reqs_ok:
        # noinspection PyUnresolvedReferences
        import pandas as pd
        # noinspection PyUnresolvedReferences
        import numpy as np
        data = pd.DataFrame(
            data=np.array([[1, 2, 3], [4, 5, 6]]),
            columns=["A", "B", "C"],
            index=[10, 30],
        )
        path.write(data=data)
        assert path.is_file()
        res = path.read()
        # noinspection PyUnresolvedReferences
        assert (np.array(res["data"]) == data.values).all()
        assert res["columns"] == data.columns.tolist()
        assert res["index"] == data.index.tolist()


def get_path(fs_kind):
    reload(sys.modules["transparentpath"])
    if skip_gcs[fs_kind]:
        print("skipped")
        return "skipped"
    init(fs_kind)

    pcsv = TransparentPath("chien.json")
    pcsv.rm(absent="ignore", ignore_kind=True)
    assert not pcsv.is_file()
    return pcsv
