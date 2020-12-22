errormessage = "Dask does not seem to be installed. You will not be able to use Dask DataFrames through "\
               "TransparentPath.\nYou can change that by running 'pip install transparentpath[dask]'."

try:
    # noinspection PyUnresolvedReferences,PyPackageRequirements
    import dask.dataframe as dd
    # noinspection PyUnresolvedReferences,PyPackageRequirements
    from dask.delayed import delayed
    # noinspection PyUnresolvedReferences,PyPackageRequirements
    from dask.distributed import client
    from typing import Union
    import tempfile
    # noinspection PyPackageRequirements
    import pandas as pd

    from ..gcsutils.transparentpath import TransparentPath, get_index_and_date_from_kwargs, check_kwargs
    from .hdf5 import hdf5_ok
    from .hdf5 import errormessage as errormessage_hdf5
    from .excel import excel_ok
    from .excel import errormessage as errormessage_excel
    from .parquet import parquet_ok
    from .parquet import errormessage as errormessage_parquet

    client_ = client


    def check_dask(self, which: str = "read"):
        if which != "read":
            return
        if self.__class__.cli is None:
            self.__class__.cli = client.Client(processes=False)
        if self.suffix != ".csv" and self.suffix != ".parquet" and not self.is_file():
            raise FileNotFoundError(f"Could not find file {self}")
        else:
            # noinspection PyUnresolvedReferences
            if (
                    not self.is_file()
                    and not self.is_dir(exist=True)
                    and not self.with_suffix("").is_dir(exist=True)
                    and "*" not in str(self)
            ):
                raise FileNotFoundError(f"Could not find file nor directory {self}")

    def apply_index_and_date_dd(
            index_col: int, parse_dates: bool, df: dd.DataFrame
    ) -> dd.DataFrame:
        if index_col is not None:
            df = df.set_index(df.columns[index_col])
            df.index = df.index.rename(None)
        if parse_dates is not None:
            # noinspection PyTypeChecker
            df.index = dd.to_datetime(df.index)
        return df


    def read_csv(self, update_cache: bool = True, **kwargs) -> dd.DataFrame:
        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()

        check_dask(self)
        # noinspection PyTypeChecker,PyUnresolvedReferences
        if self.path.is_file():
            to_use = self
        else:
            to_use = self.path.with_suffix("")
        index_col, parse_dates, kwargs = get_index_and_date_from_kwargs(**kwargs)
        check_kwargs(dd.read_csv, kwargs)
        return apply_index_and_date_dd(index_col, parse_dates, dd.read_csv(to_use.__fspath__(), **kwargs))

    def read_parquet(self, update_cache: bool = True, **kwargs) -> Union[dd.DataFrame, dd.Series]:

        if not parquet_ok:
            raise ImportError(errormessage_parquet)

        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()

        index_col, parse_dates, kwargs = get_index_and_date_from_kwargs(**kwargs)

        check_dask(self)
        if self.path.is_file():
            to_use = self
        else:
            to_use = self.path.with_suffix("")
        check_kwargs(dd.read_parquet, kwargs)
        return apply_index_and_date_dd(
            index_col, parse_dates, dd.read_parquet(to_use.__fspath__(), engine="pyarrow", **kwargs)
        )

    def read_hdf5(
        self,
        update_cache: bool = True,
        set_names: str = "",
        **kwargs,
    ) -> dd.DataFrame:

        if not hdf5_ok:
            raise ImportError(errormessage_hdf5)

        mode = "r"
        if "mode" in kwargs:
            mode = kwargs["mode"]
            del kwargs["mode"]
        if "r" not in mode:
            raise ValueError("If using read_hdf5, mode must contain 'r'")

        check_dask(self)
        if len(set_names) == 0:
            raise ValueError(
                "If using Dask, you must specify the dataset name to extract using set_names='aname'"
                "or a wildcard."
            )

        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()

        check_kwargs(dd.read_hdf, kwargs)
        if self.path.fs_kind == "local":
            return dd.read_hdf(pattern=self, key=set_names, **kwargs)
        f = tempfile.NamedTemporaryFile(delete=False, suffix=".hdf5")
        f.close()  # deletes the tmp file, but we can still use its name to download the remote file locally
        self.path.get(f.name)
        data = TransparentPath.cli.submit(dd.read_hdf, f.name, set_names, **kwargs)
        # Do not delete the tmp file, since dask tasks are delayed
        return data.result()

    def read_excel(self, update_cache: bool = True, **kwargs) -> pd.DataFrame:

        if not excel_ok:
            raise ImportError(errormessage_excel)

        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()

        check_dask(self)
        check_kwargs(pd.read_excel, kwargs)
        # noinspection PyTypeChecker,PyUnresolvedReferences
        try:
            if self.path.fs_kind == "local":
                parts = delayed(pd.read_excel)(self, **kwargs)
                return dd.from_delayed(parts)
            else:
                f = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
                f.close()  # deletes the tmp file, but we can still use its name
                self.path.get(f.name)
                parts = delayed(pd.read_excel)(f.name, **kwargs)
                data = dd.from_delayed(parts)
                # We should not delete the tmp file, since dask does its operations lasily.
                return data
        except pd.errors.ParserError:
            # noinspection PyUnresolvedReferences
            raise pd.errors.ParserError(
                "Could not read data. Most likely, the file is encrypted. Ask your cloud manager to remove encryption "
                "on it."
            )


except ImportError as e:
    import warnings
    warnings.warn(errormessage)
    raise e
