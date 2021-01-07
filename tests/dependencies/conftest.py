import pytest
from pathlib import Path


@pytest.fixture
def reqs(pytestconfig):
    name = pytestconfig.getoption("name")
    requirements = {}

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
            requirements[opt].append(s)

    this_one = requirements[name]
    others = []

    for opt in requirements:
        if opt == name:
            continue
        else:
            others += [r for r in requirements[opt] if r not in this_one and r not in others]

    yield this_one, others


def pytest_addoption(parser):
    parser.addoption("--name", action="store", default=None)
