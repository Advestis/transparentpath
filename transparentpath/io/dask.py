errormessage = (
    "Dask does not seem to be installed. You will not be able to use Dask DataFrames through "
    "TransparentPath.\nYou can change that by running 'pip install transparentpath[dask]'."
)

try:
    # noinspection PyUnresolvedReferences,PyPackageRequirements
    import dask.dataframe as dd

    # noinspection PyUnresolvedReferences,PyPackageRequirements
    from dask.delayed import delayed

    # noinspection PyUnresolvedReferences,PyPackageRequirements
    from dask.distributed import client
    from typing import Union, List, Any
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

    TransparentPath.cli = None

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

    def apply_index_and_date_dd(index_col: int, parse_dates: bool, df: dd.DataFrame) -> dd.DataFrame:
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
        if self.is_file():
            to_use = self
        else:
            to_use = self.with_suffix("")
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
        if self.is_file():
            to_use = self
        else:
            to_use = self.with_suffix("")
        check_kwargs(dd.read_parquet, kwargs)
        return apply_index_and_date_dd(
            index_col, parse_dates, dd.read_parquet(to_use.__fspath__(), engine="pyarrow", **kwargs)
        )

    def read_hdf5(self, update_cache: bool = True, set_names: str = "", use_pandas: bool = False, **kwargs) -> \
            dd.DataFrame:

        if not hdf5_ok:
            raise ImportError(errormessage_hdf5)

        if use_pandas:
            raise NotImplementedError("Using dask in transparentpath does not support pandas's HDFStore")

        mode = "r"
        if "mode" in kwargs:
            mode = kwargs["mode"]
            del kwargs["mode"]
        if "r" not in mode:
            raise ValueError("If using read_hdf5, mode must contain 'r'")

        check_dask(self)
        if len(set_names) == 0:
            raise ValueError(
                "If using Dask, you must specify the dataset name to extract using set_names='aname' or a wildcard."
            )

        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()

        check_kwargs(dd.read_hdf, kwargs)
        if self.fs_kind == "local":
            return dd.read_hdf(pattern=self, key=set_names, **kwargs)
        f = tempfile.NamedTemporaryFile(delete=False, suffix=".hdf5")
        f.close()  # deletes the tmp file, but we can still use its name to download the remote file locally
        self.get(f.name)
        data = self.__class__.cli.submit(dd.read_hdf, f.name, set_names, **kwargs)
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
            if self.fs_kind == "local":
                parts = delayed(pd.read_excel)(self.__fspath__(), **kwargs)
                return dd.from_delayed(parts)
            else:
                f = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
                f.close()  # deletes the tmp file, but we can still use its name
                self.get(f.name)
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

    def write_csv(
        self, data: dd.DataFrame, overwrite: bool = True, present: str = "ignore", update_cache: bool = True, **kwargs,
    ) -> Union[None, List[TransparentPath]]:

        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()
        if not overwrite and self.is_file() and present != "ignore":
            raise FileExistsError()

        if self.__class__.cli is None:
            self.__class__.cli = client.Client(processes=False)
        check_kwargs(dd.to_csv, kwargs)
        path_to_save = self
        if not path_to_save.stem.endswith("*"):
            path_to_save = path_to_save.parent / (path_to_save.stem + "_*.csv")
        futures = self.__class__.cli.submit(dd.to_csv, data, path_to_save.__fspath__(), **kwargs)
        outfiles = [
            TransparentPath(f, fs=self.fs_kind, bucket=self.bucket, project=self.project) for f in futures.result()
        ]
        if len(outfiles) == 1:
            outfiles[0].mv(self)
        else:
            return outfiles

    def write_parquet(
        self,
        data: Union[pd.DataFrame, pd.Series, dd.DataFrame],
        overwrite: bool = True,
        present: str = "ignore",
        update_cache: bool = True,
        columns_to_string: bool = True,
        **kwargs,
    ) -> None:

        if not hdf5_ok:
            raise ImportError(errormessage_hdf5)

        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()
        if not overwrite and self.is_file() and present != "ignore":
            raise FileExistsError()

        if columns_to_string and not isinstance(data.columns[0], str):
            data.columns = data.columns.astype(str)

        if self.__class__.cli is None:
            self.__class__.cli = client.Client(processes=False)
        check_kwargs(dd.to_parquet, kwargs)
        dd.to_parquet(data, self.with_suffix("").__fspath__(), engine="pyarrow", compression="snappy", **kwargs)

    # noinspection PyUnresolvedReferences

    def write_hdf5(
        self, data: Any = None, set_name: str = None, update_cache: bool = True, use_pandas: bool = False, **kwargs,
    ) -> Union[None, "h5py.File"]:

        if not hdf5_ok:
            raise ImportError(errormessage_hdf5)

        if use_pandas:
            raise NotImplementedError("TransparentPath does not support storing Dask objects in pandas's HDFStore yet.")

        if self.__class__.cli is None:
            self.__class__.cli = client.Client(processes=False)
        check_kwargs(dd.to_hdf, kwargs)

        mode = "w"
        if "mode" in kwargs:
            mode = kwargs["mode"]
            del kwargs["mode"]

        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()

        if isinstance(data, dict):
            sets = data
        else:
            if set_name is None:
                set_name = "data"
            sets = {set_name: data}

        if self.fs_kind == "local":
            for aset in sets:
                dd.to_hdf(sets[aset], self, aset, mode=mode, **kwargs)
        else:
            with tempfile.NamedTemporaryFile() as f:
                futures = self.__class__.cli.map(
                    dd.to_hdf, list(sets.values()), [f.name] * len(sets), list(sets.keys()), mode=mode, **kwargs
                )
                self.__class__.cli.gather(futures)
                TransparentPath(path=f.name, fs="local", bucket=self.bucket, project=self.project).put(self.path)
        return

    def write_excel(
        self,
        data: Union[pd.DataFrame, pd.Series, dd.DataFrame],
        overwrite: bool = True,
        present: str = "ignore",
        update_cache: bool = True,
        **kwargs,
    ) -> None:

        if not excel_ok:
            raise ImportError(errormessage_excel)

        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()
        if not overwrite and self.is_file() and present != "ignore":
            raise FileExistsError()

        if self.fs_kind == "local":
            if self.__class__.cli is None:
                self.__class__.cli = client.Client(processes=False)
            check_kwargs(pd.DataFrame.to_excel, kwargs)
            parts = delayed(pd.DataFrame.to_excel)(data, self.__fspath__(), **kwargs)
            parts.compute()
            return
        else:
            with tempfile.NamedTemporaryFile(delete=True, suffix=".xlsx") as f:
                if TransparentPath.cli is None:
                    TransparentPath.cli = client.Client(processes=False)
                check_kwargs(pd.DataFrame.to_excel, kwargs)
                parts = delayed(pd.DataFrame.to_excel)(data, f.name, **kwargs)
                parts.compute()
                TransparentPath(path=f.name, fs="local", bucket=self.bucket, project=self.project).put(self.path)


except ImportError as e:
    # import warnings
    # warnings.warn(f"{errormessage}. Full ImportError message was:\n{e}")
    raise e
