import os
from transparentpath import TransparentPath

project = "sandbox-281209"
bucket = "code_tests_sand"
skip_gcs = {"local": False, "gcs": False}

if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    print("No google credentials found. Skipping GCS tests.")
    skip_gcs["gcs"] = True
else:
    print(f"Using google credentials {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")


def before_init():
    assert TransparentPath.fss == {}
    assert TransparentPath.unset
    assert TransparentPath.fs_kind == ""
    assert TransparentPath.project is None
    assert TransparentPath.bucket is None
    assert TransparentPath.nas_dir == "/media/SERVEUR"


def init(fs_kind):
    TransparentPath.set_global_fs(fs_kind, project=project, bucket=bucket)


def reinit():
    print("coucou")
    TransparentPath.fss = {}
    TransparentPath.unset = True
    TransparentPath.fs_kind = ""
    TransparentPath.project = None
    TransparentPath.bucket = None
    TransparentPath.nas_dir = "/media/SERVEUR"


def get_prefixes(fs_kind):
    str_prefix = os.getcwd()
    pathlib_prefix = str_prefix
    if fs_kind == "gcs":
        str_prefix = f"gs://{bucket}"
        pathlib_prefix = bucket
    return str_prefix, pathlib_prefix
