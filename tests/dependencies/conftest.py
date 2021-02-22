import pytest
from pathlib import Path


acceptable = {
    "pandas": ["numpy"],
    "hdf5": ["numpy"],
    "parquet": ["numpy"],
    "excel": ["numpy"],
    "dask": ["pandas", "numpy"]
}


@pytest.fixture
def reqs(pytestconfig):
    name = pytestconfig.getoption("name")
    requirements = {}
    all_reqs = []

    for afile in Path("").glob("*requirements.txt"):
        if afile.stem == "requirements":
            opt = "vanilla"
        else:
            opt = afile.stem.split("-requirements")[0]

        requirements[opt] = []
        for s in afile.read_text().splitlines():
            s = s.split("==")[0] if "==" in s else s
            s = s.split("<=")[0] if "<=" in s else s
            s = s.split(">=")[0] if ">=" in s else s
            s = s.split("<")[0] if "<" in s else s
            s = s.split(">")[0] if ">=" in s else s
            s = s.split("[")[0] if "[" in s else s
            requirements[opt].append(s)
        all_reqs = list(set(all_reqs) | set(requirements[opt]))

    if name == "all":
        return all_reqs, [], []

    this_one = requirements[name]
    others = []

    for opt in requirements:
        if opt == name or opt == "vanilla":
            continue
        else:
            others += [r for r in requirements[opt] if r not in this_one and r not in others]
    return this_one, others, acceptable[name] if name in acceptable else []


def pytest_addoption(parser):
    parser.addoption("--name", action="store", default=None)
