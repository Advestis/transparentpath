import sys
import os
from transparentpath import TransparentPath
from pathlib import Path
from importlib import reload

bucket = "code_tests_sand"
skip_gcs = {"local": False, "gcs": False}

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
    assert TransparentPath.nas_dir == "/media/SERVEUR"


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
    requirements = []
    filename = Path(f"{name}-requirements.txt")
    if name == "vanilla":
        filename = Path("requirements.txt")
    if not filename.is_file():
        raise FileNotFoundError(f"File {filename} does not exist")
    for s in filename.read_text().splitlines():
        s = s.split("==")[0] if "==" in s else s
        s = s.split("<=")[0] if "<=" in s else s
        s = s.split(">=")[0] if ">=" in s else s
        s = s.split("<")[0] if "<" in s else s
        s = s.split(">")[0] if ">=" in s else s
        requirements.append(s)
    return requirements


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
