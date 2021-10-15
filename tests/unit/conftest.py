import pytest
import os
from .functions import before_init, reinit
from pathlib import Path
from transparentpath import TransparentPath


def process_token(token, token_file):
    if token is not None:
        if not token_file.is_file():
            with open(str(token_file), "w") as ofile:
                ofile.write(token)
        if "2" in str(token_file):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS_2"] = str(token_file)
        else:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(token_file)


@pytest.fixture
def clean(pytestconfig):
    token_file = Path("cred.json")
    token_file2 = Path("cred2.json")
    token = pytestconfig.getoption("token")
    token2 = pytestconfig.getoption("token2")
    process_token(token, token_file)
    process_token(token2, token_file2)

    TransparentPath._do_update_cache = True
    TransparentPath._do_check = True
    before_init()
    yield
    TransparentPath._do_update_cache = False
    TransparentPath._do_check = False
    path1 = TransparentPath("chien")
    path2 = TransparentPath("chien2")
    suffixes = ["", ".zip", ".txt", ".json", ".csv", ".parquet", ".hdf5", ".xlsx", ".joblib"]
    for suffix in suffixes:
        path1.with_suffix(suffix).rm(recursive=True, ignore_kind=True, absent="ignore")
        path1.with_suffix(suffix).rm(recursive=True, ignore_kind=True, absent="ignore")
        path2.with_suffix(suffix).rm(recursive=True, ignore_kind=True, absent="ignore")
        path2.with_suffix(suffix).rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath._do_update_cache = True
    TransparentPath._do_check = True
    reinit()


def pytest_addoption(parser):
    parser.addoption("--token", action="store", default=None)
    parser.addoption("--token2", action="store", default=None)
