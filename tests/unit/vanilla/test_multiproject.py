import importlib.util
from ..functions import init, skip_gcs, get_reqs
from transparentpath import TransparentPath
import os


requirements = get_reqs("vanilla")

for req in requirements:
    if importlib.util.find_spec(req) is None:
        raise ImportError(f"TransparentPath needs {req} package")


def test_multiproject_1(clean):
    if skip_gcs["gcs"]:
        print("skipped")
        return
    init("gcs")
    TransparentPath.set_global_fs("gcs", token=os.environ["GOOGLE_APPLICATION_CREDENTIALS_2"])
    p1 = TransparentPath("code_tests_sand")
    p2 = TransparentPath("code_tests_dev")
    p3 = TransparentPath("coucou")
    assert "gcs" in p1.fs_kind
    assert "gcs" in p2.fs_kind
    assert p1.fs_kind != p2.fs_kind
    assert p3.fs_kind == "local"


def test_multiproject_2(clean):
    if skip_gcs["gcs"]:
        print("skipped")
        return
    init("gcs")
    TransparentPath("code_test_sand")
    TransparentPath.set_global_fs("gcs", bucket="code_tests", token=os.environ["GOOGLE_APPLICATION_CREDENTIALS_2"])
    p1 = TransparentPath("code_tests_sand")
    p2 = TransparentPath("code_tests_dev")
    p3 = TransparentPath("coucou")
    assert "gcs" in p1.fs_kind
    assert "gcs" in p2.fs_kind
    assert "gcs" in p3.fs_kind
    assert p1.fs_kind != p2.fs_kind
    assert p3.fs_kind == p2.fs_kind
