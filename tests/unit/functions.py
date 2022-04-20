import sys
import os
from transparentpath import TransparentPath
from importlib import reload

bucket = "code_tests_sand"
skip_gcs = {"local": False, "gcs": False}
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

# if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
#     print("No google credentials found. Skipping GCS tests.")
#     skip_gcs["gcs"] = True
# else:
#     print(f"Using google credentials {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")


def before_init():
    assert TransparentPath.fss == {}
    assert TransparentPath.unset
    assert TransparentPath.fs_kind is None
    assert TransparentPath.bucket is None
    assert TransparentPath.nas_dir is None


def init(fs_kind, bucket_=""):
    if bucket_ == "":
        bucket_ = bucket
    TransparentPath.set_global_fs(fs_kind, bucket=bucket_)


def reinit():
    TransparentPath.reinit()


def get_prefixes(fs_kind):
    str_prefix = os.getcwd()
    pathlib_prefix = str_prefix
    if fs_kind == "gcs":
        str_prefix = f"gs://{bucket}"
        pathlib_prefix = bucket
    return str_prefix, pathlib_prefix


def get_reqs(name):
    return requirements[name]


def get_path(fs_kind, suffix):
    reload(sys.modules["transparentpath"])
    if skip_gcs[fs_kind]:
        print("skipped")
        return "skipped"
    init(fs_kind)

    if fs_kind == "local":
        local_path = TransparentPath(f"tests/data/chien{suffix}")
        pfile = TransparentPath(f"chien{suffix}")
        local_path.cp(pfile)
    else:
        local_path = TransparentPath(f"tests/data/chien{suffix}", fs_kind="local")
        pfile = TransparentPath(f"chien{suffix}")
        local_path.put(pfile)

    return pfile
