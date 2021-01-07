import importlib.util


def test_self(reqs):
    failed = []
    for req in reqs[0]:
        print(f"Asserting that {req} is installed...")
        if importlib.util.find_spec(req) is None:
            failed.append(req)
    if len(failed) != 0:
        raise AssertionError("Some required packages are not installed :\n"
                             f"{failed}")


def test_others(reqs):
    failed = []
    for other in reqs[1]:
        print(f"Asserting that {other} is not installed...")
        if importlib.util.find_spec(other) is not None:
            failed.append(other)
    if len(failed) != 0:
        raise AssertionError("Some extra packages are installed and should not be:\n"
                             f"{failed}")
