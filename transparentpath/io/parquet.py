errormessage = (
    "parquet for TransparentPath does not seem to be installed. You will not be able to use parquet files "
    "through TransparentPath.\nYou can change that by running 'pip install transparentpath[parquet]'."
)

parquet_ok = False


class TPImportError(ImportError):
    def __init__(self, message: str = ""):
        self.message = f"Error in TransparentPath: {message}"
        super().__init__(self.message)


try:
    import pandas as pd
    import tempfile
    import warnings
    import sys
    from typing import Union, List, Tuple
    import importlib.util
    from ..gcsutils.transparentpath import TransparentPath, check_kwargs, TPFileExistsError, TPFileNotFoundError

    if importlib.util.find_spec("pyarrow") is None:
        raise TPImportError("Need the 'pyarrow' package")

    parquet_ok = True

    def get_index_and_date_from_kwargs(**kwargs: dict) -> Tuple[int, bool, dict]:
        index_col = kwargs.get("index_col", None)
        parse_dates = kwargs.get("parse_dates", None)
        if index_col is not None:
            del kwargs["index_col"]
        if parse_dates is not None:
            del kwargs["parse_dates"]
        # noinspection PyTypeChecker
        return index_col, parse_dates, kwargs

    def apply_index_and_date(index_col: int, parse_dates: bool, df: pd.DataFrame) -> pd.DataFrame:
        if index_col is not None:
            df = df.set_index(df.columns[index_col])
            df.index = df.index.rename(None)
        if parse_dates is not None:
            # noinspection PyTypeChecker
            df.index = pd.to_datetime(df.index)
        return df

    def read(self, **kwargs) -> Union[pd.DataFrame, pd.Series]:

        if not self.is_file():
            raise TPFileNotFoundError(f"Could not find file {self}")

        index_col, parse_dates, kwargs = get_index_and_date_from_kwargs(**kwargs)

        check_kwargs(pd.read_parquet, kwargs)
        if self.fs_kind == "local":
            return apply_index_and_date(
                index_col, parse_dates, pd.read_parquet(self.__fspath__(), engine="pyarrow", **kwargs)
            )

        else:
            return apply_index_and_date(
                index_col, parse_dates, pd.read_parquet(self.open("rb"), engine="pyarrow", **kwargs)
            )

    def write(
        self,
        data: Union[pd.DataFrame, pd.Series],
        overwrite: bool = True,
        present: str = "ignore",
        columns_to_string: bool = True,
        to_dataframe: bool = True,
        compression: str = None,
        **kwargs,
    ) -> None:
        """
        Warning : if data is a Dask dataframe, the output will be written in a directory. For convenience, the directory
        if self.with_suffix(""). Reading is transparent and one can specify a path with .parquet suffix.
        """

        if self.suffix != ".parquet":
            warnings.warn(f"path {self} does not have '.parquet' as suffix while using to_parquet. The path will be "
                          f"changed to a path with '.parquet' as suffix")
            self.change_suffix(".parquet")

        if compression is not None and compression != "snappy":
            warnings.warn("TransparentPath can not write parquet files with a compression that is not snappy. You "
                          f"specified '{compression}', it will be replaced by 'snappy'.")

        if not overwrite and self.is_file() and present != "ignore":
            raise TPFileExistsError()

        if to_dataframe and isinstance(data, pd.Series):
            name = data.name
            data = pd.DataFrame(data=data)
            if name is not None:
                data.columns = [name]

        if columns_to_string and not isinstance(data.columns[0], str):
            # noinspection PyUnresolvedReferences
            data.columns = data.columns.astype(str)

        # noinspection PyTypeChecker
        check_kwargs(data.to_parquet, kwargs)
        data.to_parquet(self.open("wb"), engine="pyarrow", compression="snappy", **kwargs)


except ImportError as e:
    raise TPImportError(str(e))
