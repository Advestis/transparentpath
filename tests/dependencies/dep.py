import importlib.util


def test_self(reqs):
    for req in reqs[0]:
        print(f"Asserting that {req} is installed...")
        assert importlib.util.find_spec(req) is not None


def test_others(reqs):
    for other in reqs[1]:
        print(f"Asserting that {other} is not installed...")
        assert importlib.util.find_spec(other) is None
