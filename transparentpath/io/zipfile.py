import zipfile
import tempfile
from pathlib import Path
from ..gcsutils.transparentpath import TransparentPath

zipfileclass = zipfile.ZipFile


class TpZipFile(zipfileclass):
    """
    Overload of ZipFile class to handle files on GCS
    """

    def __init__(self, path, *args, **kwargs):
        if type(path) == TransparentPath and path.fs_kind != "local":
            if path.when_checked["used"] and not path.nocheck:
                # noinspection PyProtectedMember
                path._check_multiplicity()
            f = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
            f.close()  # deletes tmp file, but we can still use its name
            path.get(f.name)
            path = Path(f.name)  # Path is pathlib, not TransparentPath
            super().__init__(path, *args, **kwargs)
            path.unlink()
        else:
            super().__init__(path, *args, **kwargs)


zipfile.ZipFile = TpZipFile
