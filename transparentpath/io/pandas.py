errormessage = (
    "pandas does not seem to be installed. You will not be able to use pandas objects through "
    "TransparentPath.\nYou can change that by running 'pip install transparentpath[pandas]'."
)


class TPImportError(ImportError):
    def __init__(self, message: str = ""):
        self.message = f"Error in TransparentPath: {message}"
        super().__init__(self.message)


try:
    import pandas as pd
    import tempfile
    import warnings
    from typing import Union, List
    from ..gcsutils.transparentpath import TransparentPath, check_kwargs, TPFileExistsError, TPFileNotFoundError

    class MyHDFStore(pd.HDFStore):
        """Same as MyHDFFile but for pd.HDFStore objects"""

        def __init__(self, *args, remote: Union[TransparentPath, None] = None, **kwargs):
            self.remote_file = remote
            self.local_path = args[0]
            # noinspection PyUnresolvedReferences,PyProtectedMember
            if isinstance(self.local_path, tempfile._TemporaryFileWrapper):
                pd.HDFStore.__init__(self, args[0].name, *args[1:], **kwargs)
            else:
                pd.HDFStore.__init__(self, *args, **kwargs)

        def __exit__(self, exc_type, exc_value, traceback):
            pd.HDFStore.__exit__(self, exc_type, exc_value, traceback)
            if self.remote_file is not None:
                TransparentPath(self.local_path.name, fs="local").put(self.remote_file)
                self.local_path.close()

    def read(self, **kwargs) -> pd.DataFrame:

        if not self.is_file():
            raise TPFileNotFoundError(f"Could not find file {self}")

        # noinspection PyTypeChecker,PyUnresolvedReferences
        try:

            check_kwargs(pd.read_csv, kwargs)
            return pd.read_csv(self.__fspath__(), **kwargs)

        except pd.errors.ParserError:
            # noinspection PyUnresolvedReferences
            raise pd.errors.ParserError(
                "Could not read data. Most likely, the file is encrypted."
                " Ask your cloud manager to remove encryption on it."
            )

    def write(
        self, data: Union[pd.DataFrame, pd.Series], overwrite: bool = True, present: str = "ignore", **kwargs,
    ):

        if self.suffix != ".csv":
            warnings.warn(f"path {self} does not have '.csv' as suffix while using to_csv. The path will be "
                          f"changed to a path with '.csv' as suffix")
            self.change_suffix(".csv")

        if not overwrite and self.is_file() and present != "ignore":
            raise TPFileExistsError()

        check_kwargs(data.to_csv, kwargs)
        data.to_csv(self.__fspath__(), **kwargs)


except ImportError as e:
    raise TPImportError(str(e))
