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
@pytest.mark.parametrize("fs_kind", ["local", "gcs"])
def test_joblib(clean, fs_kind):
    if reqs_ok is False:
        pparquet = get_path(fs_kind, ".joblib")
        with pytest.raises(ImportError):
            from joblib import load
            import numpy as np
    else:
        from joblib import load
        import numpy as np

        # noinspection PyTypeChecker
        picklefile = get_path(fs_kind, ".joblib")
        if picklefile == "skipped":
            return
        c = load(picklefile)
        a = np.array(
                [
                    [1.0, 1.0, 1.0, 1.0, 1.0],
                    [1.0, 1.0, 1.0, 1.0, 1.0],
                    [1.0, 1.0, 1.0, 1.0, 1.0],
                    [1.0, 1.0, 1.0, 1.0, 1.0],
                    [1.0, 1.0, 1.0, 1.0, 1.0],
                ]
            )
        np.testing.assert_equal(c, a)
