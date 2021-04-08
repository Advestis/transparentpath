errormessage = (
    "Excel for TransparentPath does not seem to be installed. You will not be able to use excel files "
    "through TransparentPath.\nYou can change that by running 'pip install transparentpath[excel]'."
)


class TPImportError(ImportError):
    def __init__(self, message: str = ""):
        self.message = f"Error in TransparentPath: {message}"
        super().__init__(self.message)


excel_ok = False

try:
    # noinspection PyUnresolvedReferences
    import pandas as pd
    import tempfile
    import warnings
    from pathlib import Path
    import sys
    import importlib.util
    from typing import Union, List, Tuple
    from ..gcsutils.transparentpath import TransparentPath, check_kwargs, TPFileExistsError, TPFileNotFoundError

    if importlib.util.find_spec("xlrd") is None:
        raise TPImportError("Need the 'xlrd' package")
    if importlib.util.find_spec("openpyxl") is None:
        raise TPImportError("Need the 'openpyxl' package")

    excel_ok = True

    def read(self, **kwargs) -> pd.DataFrame:

        if not self.is_file():
            raise TPFileNotFoundError(f"Could not find file {self}")

        check_kwargs(pd.read_excel, kwargs)
        # noinspection PyTypeChecker,PyUnresolvedReferences
        try:
            if self.fs_kind == "local":
                return pd.read_excel(self.__fspath__(), **kwargs)
            else:
                f = tempfile.NamedTemporaryFile(delete=False, suffix=self.suffix)
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
        self, data: Union[pd.DataFrame, pd.Series], overwrite: bool = True, present: str = "ignore", **kwargs,
    ) -> None:

        if self.suffix != ".xlsx" and self.suffix != ".xls" and self.suffix != ".xlsm":
            warnings.warn(f"path {self} does not have '.xls(x,m)' as suffix while using to_excel. The path will be "
                          f"changed to a path with '.xlsx' as suffix")
            self.change_suffix(".xlsx")

        if not overwrite and self.is_file() and present != "ignore":
            raise TPFileExistsError()

        # noinspection PyTypeChecker

        if self.fs_kind == "local":
            data.to_excel(self.__fspath__(), **kwargs)
        else:
            with tempfile.NamedTemporaryFile(delete=True, suffix=self.suffix) as f:
                check_kwargs(data.to_excel, kwargs)
                data.to_excel(f.name, **kwargs)
                TransparentPath(
                    path=f.name,
                    fs="local",
                    notupdatecache=self.notupdatecache,
                    nocheck=self.nocheck,
                    when_checked=self.when_checked,
                    when_updated=self.when_updated,
                    update_expire=self.update_expire,
                    check_expire=self.check_expire,
                ).put(self.path)


except ImportError as e:
    raise TPImportError(str(e))
