errormessage = "parquet for TransparentPath does not seem to be installed. You will not be able to use parquet files " \
               "through TransparentPath.\nYou can change that by running 'pip install transparentpath[parquet]'."

parquet_ok = False

try:
    # noinspection PyUnresolvedReferences
    import pandas as pd
    import tempfile
    import sys
    from typing import Union, List, Tuple
    from ..gcsutils.transparentpath import TransparentPath, check_kwargs

    if "pyarrow" not in sys.modules:
        raise ImportError("Need the 'pyarrow' package")

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


    def apply_index_and_date(
            index_col: int, parse_dates: bool, df: pd.DataFrame
    ) -> pd.DataFrame:
        if index_col is not None:
            df = df.set_index(df.columns[index_col])
            df.index = df.index.rename(None)
        if parse_dates is not None:
            # noinspection PyTypeChecker
            df.index = pd.to_datetime(df.index)
        return df


    def read(
        self, update_cache: bool = True, **kwargs
    ) -> Union[pd.DataFrame, pd.Series]:
        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()

        index_col, parse_dates, kwargs = get_index_and_date_from_kwargs(**kwargs)

        check_kwargs(pd.read_parquet, kwargs)
        if self.fs_kind == "local":
            return apply_index_and_date(index_col, parse_dates, pd.read_parquet(str(self), engine="pyarrow", **kwargs))

        else:
            return apply_index_and_date(
                index_col, parse_dates, pd.read_parquet(self.open("rb"), engine="pyarrow", **kwargs)
            )

    def write(
        self,
        data: Union[pd.DataFrame, pd.Series],
        overwrite: bool = True,
        present: str = "ignore",
        update_cache: bool = True,
        columns_to_string: bool = True,
        to_dataframe: bool = True,
        **kwargs,
    ) -> None:
        """
        Warning : if data is a Dask dataframe, the output will be written in a directory. For convenience, the directory
        if self.with_suffix(""). Reading is transparent and one can specify a path with .parquet suffix.
        """
        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()
        if not overwrite and self.is_file() and present != "ignore":
            raise FileExistsError()

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


except ImportError:
    import warnings
    warnings.warn(errormessage)
