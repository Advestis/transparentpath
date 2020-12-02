errormessage = "zipfile does not seem to be installed. You will not be able to use pandas objects through "\
               "TransparentPath.\nYou can change that by running 'pip install transparentpath[pandas]'."


class Myzipfile:
    def __init__(self):
        raise ImportError(errormessage)


try:
    # noinspection PyUnresolvedReferences
    import zipfile
    import tempfile
    from pathlib import Path
    from..gcsutils.transparentpath import TransparentPath

    zipfileclass = zipfile.ZipFile


    class Myzipfile(zipfileclass):
        """
        Overload of ZipFile class to handle files on GCS
        """

        def __init__(self, path, *args, **kwargs):
            if type(path) == TransparentPath and path.fs_kind == "gcs":
                f = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
                f.close()  # deletes tmp file, but we can still use its name
                path.get(f.name)
                path = Path(f.name)  # Path is pathlib, not TransparentPath
                super().__init__(path, *args, **kwargs)
                path.unlink()
            else:
                super().__init__(path, *args, **kwargs)


    zipfile.ZipFile = Myzipfile
except ImportError:
    import warnings
    warnings.warn(errormessage)