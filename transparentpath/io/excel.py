errormessage = (
    "Excel for TransparentPath does not seem to be installed. You will not be able to use excel files "
    "through TransparentPath.\nYou can change that by running 'pip install transparentpath[excel]'."
)

excel_ok = False

try:
    # noinspection PyUnresolvedReferences
    import pandas as pd
    import tempfile
    from pathlib import Path
    import sys
    import importlib.util
    from typing import Union, List, Tuple
    from ..gcsutils.transparentpath import TransparentPath, check_kwargs

    if importlib.util.find_spec("xlrd") is None:
        raise ImportError("Need the 'xlrd' package")
    if importlib.util.find_spec("openpyxl") is None:
        raise ImportError("Need the 'openpyxl' package")

    excel_ok = True

    def read(self, update_cache: bool = True, **kwargs) -> pd.DataFrame:
        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()

        check_kwargs(pd.read_excel, kwargs)
        # noinspection PyTypeChecker,PyUnresolvedReferences
        try:
            if self.fs_kind == "local":
                return pd.read_excel(self.__fspath__(), **kwargs)
            else:
                f = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
                f.close()  # deletes the tmp file, but we can still use its name
                self.get(f.name)
                data = pd.read_excel(f.name, **kwargs)
                Path(f.name).unlink()
                return data
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
    ) -> None:
        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()
        if not overwrite and self.is_file() and present != "ignore":
            raise FileExistsError()

        # noinspection PyTypeChecker

        if self.fs_kind == "local":
            data.to_excel(self, **kwargs)
        else:
            with tempfile.NamedTemporaryFile(delete=True, suffix=".xlsx") as f:
                check_kwargs(data.to_excel, kwargs)
                data.to_excel(f.name, **kwargs)
                TransparentPath(path=f.name, fs="local", bucket=self.bucket, project=self.project).put(self.__path)


except ImportError as e:
    # import warnings
    # warnings.warn(f"{errormessage}. Full ImportError message was:\n{e}")
    raise e
