errormessage = (
    "parquet for TransparentPath does not seem to be installed. You will not be able to use parquet files "
    "through TransparentPath.\nYou can change that by running 'pip install transparentpath[parquet]'."
)

parquet_ok = False


try:
    import pandas as pd
    import tempfile
    import warnings
    import sys
    from typing import Union, List, Tuple
    import importlib.util
    from pathlib import Path
    from ..gcsutils.transparentpath import TransparentPath, check_kwargs

    if importlib.util.find_spec("pyarrow") is None:
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
            raise FileNotFoundError(f"Could not find file {self}")

        index_col, parse_dates, kwargs = get_index_and_date_from_kwargs(**kwargs)

        check_kwargs(pd.read_parquet, kwargs)
        if "engine" in kwargs:
            engine = kwargs["engine"]
        else:
            engine = "pyarrow"
        del kwargs["engine"]
        if (self.fs_kind == "local") and (engine == "pyarrow"):
            return apply_index_and_date(
                index_col, parse_dates, pd.read_parquet(self.__fspath__(), engine=engine, **kwargs)
            )

        elif engine == "pyarrow":
            return apply_index_and_date(
                index_col, parse_dates, pd.read_parquet(self.open("rb"), engine="pyarrow", **kwargs)
            )
        else:
            f = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
            f.close()  # deletes the tmp file, but we can still use its name
            self.get(f.name)
            data = pd.read_parquet(f.name, engine=engine, **kwargs)
            Path(f.name).unlink()
            return apply_index_and_date(index_col, parse_dates, data)

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
        if "engine" in kwargs:
            engine = kwargs["engine"]
        else:
            engine = "pyarrow"
        del kwargs["engine"]
        if "compression" in kwargs:
            compression = kwargs["compression"]
        else:
            compression = "snappy"
        del kwargs["compression"]
        if (self.fs_kind != "local") and ((engine != "pyarrow") or (compression != "snappy")):
            with tempfile.NamedTemporaryFile(delete=True, suffix=".parquet") as f:
                thefile = pd.read_parquet(f.name, **kwargs)
                thefile.close()
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
        else:
            data.to_parquet(self.open("wb"), engine=engine, compression=compression, **kwargs)

except ImportError as e:
    raise ImportError(str(e))
