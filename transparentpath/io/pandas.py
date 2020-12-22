errormessage = "pandas does not seem to be installed. You will not be able to use pandas objects through "\
               "TransparentPath.\nYou can change that by running 'pip install transparentpath[pandas]'."


class MyHDFStore:
    def __init__(self):
        raise ImportError(errormessage)


try:
    # noinspection PyUnresolvedReferences
    import pandas as pd
    import tempfile
    from typing import Union, List
    from ..gcsutils.transparentpath import TransparentPath, check_kwargs


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
    
    def read(self, update_cache: bool = True, **kwargs) -> pd.DataFrame:
        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()

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
        self,
        data: Union[pd.DataFrame, pd.Series],
        overwrite: bool = True,
        present: str = "ignore",
        update_cache: bool = True,
        **kwargs,
    ):
        if update_cache and self._do_update_cache:
            self._update_cache()
        if not overwrite and self.is_file() and present != "ignore":
            raise FileExistsError()

        check_kwargs(data.to_csv, kwargs)
        data.to_csv(self.__fspath__(), **kwargs)

except ImportError as e:
    import warnings
    warnings.warn(errormessage)
    raise e
