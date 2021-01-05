import pytest
from .functions import before_init, reinit
from pathlib import Path
from transparentpath import TransparentPath


@pytest.fixture
def clean(pytestconfig):
    # print("\nExecuting before init...\n")

    token_file = Path("cred.json")
    token = pytestconfig.getoption("token")
    if token is not None:
        if not token_file.is_file():
            with open("cred.json", "w") as ofile:
                ofile.write(token)
        TransparentPath.token = str(token_file)

    TransparentPath._do_update_cache = True
    TransparentPath._do_check = True
    before_init()
    yield
    # print("\n...executing clean ...\n")
    TransparentPath._do_update_cache = False
    TransparentPath._do_check = False
    TransparentPath("chien").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.zip").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.zip").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.txt").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.txt").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien2.txt").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien2.txt").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.json").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.json").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.csv").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.csv").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.parquet").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.parquet").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.hdf5").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath("chien.hdf5").rm(recursive=True, ignore_kind=True, absent="ignore")
    TransparentPath._do_update_cache = True
    TransparentPath._do_check = True
    # print("\n...executing reinit\n")
    reinit()


def pytest_addoption(parser):
    parser.addoption("--token", action="store", default=None)
