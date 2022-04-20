import pytest


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
    requirements = {"vanilla": []}

    with open("setup.cfg", "r") as setupfile:
        all_reqs_lines = setupfile.read()
        all_reqs_lines = all_reqs_lines[
                         all_reqs_lines.index("[options.extras_require]"):all_reqs_lines.index("[versioneer]")
                         ]
        all_reqs_lines = all_reqs_lines.replace("[options.extras_require]", "").replace("[versioneer]", "").split("\n")
        opt = None
        for line in all_reqs_lines:
            if line == "":
                continue
            if line.endswith("="):
                opt = line.replace("=", "")
                requirements[opt] = []
            else:
                if opt is None:
                    raise ValueError("Malformed setup.cfg : can not read requirements")
                package = line.replace(" ", "")
                package = package.split("==")[0] if "==" in package else package
                package = package.split("<=")[0] if "<=" in package else package
                package = package.split(">=")[0] if ">=" in package else package
                package = package.split("<")[0] if "<" in package else package
                package = package.split(">")[0] if ">=" in package else package
                package = package.split("[")[0] if "[" in package else package
                requirements[opt].append(package)

    if name == "all":
        return requirements["all"], [], []

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
