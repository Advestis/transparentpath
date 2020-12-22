errormessage = (
    "Support for zipfiles does not seem to be installed for TransparentPath.\n"
    "You can change that by running 'pip install transparentpath[zip]'."
)


class TpZipFile:
    def __init__(self):
        raise ImportError(errormessage)


try:
    # noinspection PyUnresolvedReferences
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
                f = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
                f.close()  # deletes tmp file, but we can still use its name
                path.get(f.name)
                path = Path(f.name)  # Path is pathlib, not TransparentPath
                super().__init__(path, *args, **kwargs)
                path.unlink()
            else:
                super().__init__(path, *args, **kwargs)

    zipfile.ZipFile = TpZipFile
except ImportError as e:
    # import warnings
    # warnings.warn(f"{errormessage}. Full ImportError message was:\n{e}")
    raise e
