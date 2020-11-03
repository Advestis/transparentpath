errormessage = "Dask does not seem to be installed. You will not be able to use Dask DataFrames through "\
               "TransparentPath.\nYou can change that by running 'pip install transparentpath[dask]'."


def apply_index_and_date_dd(index_col, parse_dates, df):
    raise ImportError(errormessage)


try:
    # noinspection PyUnresolvedReferences
    import dask.dataframe as dd
    # noinspection PyUnresolvedReferences
    from dask.delayed import delayed
    # noinspection PyUnresolvedReferences
    from dask.distributed import client
    from typing import Union
    import tempfile
    import pandas as pd

    from ..gcsutils.transparentpath import TransparentPath
    from .io import get_index_and_date_from_kwargs, check_kwargs
    from .hdf5 import hdf5_ok
    from .hdf5 import errormessage as errormessage_hdf5

    client_ = client

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


    def read(
        reader,
        use_pandas: bool = False,
        update_cache: bool = True,
        **kwargs,
    ):
        if TransparentPath.cli is None:
            TransparentPath.cli = client.Client(processes=False)
        if reader.path.suffix != ".csv" and reader.path.suffix != ".parquet" and not reader.path.is_file():
            raise FileNotFoundError(f"Could not find file {reader}")
        else:
            if (
                    not reader.path.is_file()
                    and not reader.path.is_dir(exist=True)
                    and not reader.path.with_suffix("").is_dir(exist=True)
                    and "*" not in str(reader.path)
            ):
                raise FileNotFoundError(f"Could not find file nor directory {reader}")
        
        if reader.path.suffix == ".csv":
            return read_csv(reader, update_cache=update_cache, **kwargs)
        elif reader.path.suffix == ".parquet":
            index_col = None
            if "index_col" in kwargs:
                index_col = kwargs["index_col"]
                del kwargs["index_col"]
            content = read_parquet(reader, update_cache=update_cache, **kwargs)
            if index_col:
                content.set_index(content.columns[index_col])
            return content
        elif reader.path.suffix == ".hdf5" or reader.path.suffix == ".h5":
            return read_hdf5(reader, update_cache=update_cache, use_pandas=use_pandas, **kwargs)
        elif reader.path.suffix == ".json":
            raise ValueError("Can't use Dask on JSON files!")
        elif reader.path.suffix == ".xlsx":
            return read_excel(reader, update_cache=update_cache, **kwargs)
        else:
            raise ValueError(f"Can't use Dask on file {reader.path}!")


    def read_csv(reader, update_cache: bool = True, **kwargs) -> dd.DataFrame:
        if update_cache:
            reader.path.update_cache()

        # noinspection PyTypeChecker,PyUnresolvedReferences
        if reader.path.is_file():
            to_use = reader
        else:
            to_use = reader.path.with_suffix("")
        index_col, parse_dates, kwargs = get_index_and_date_from_kwargs(**kwargs)
        check_kwargs(dd.read_csv, kwargs)
        return apply_index_and_date_dd(index_col, parse_dates, dd.read_csv(to_use.__fspath__(), **kwargs))

    def read_parquet(reader, update_cache: bool = True, **kwargs) -> Union[dd.DataFrame, dd.Series]:
        if update_cache:
            reader.path.update_cache()

        index_col, parse_dates, kwargs = get_index_and_date_from_kwargs(**kwargs)

        if reader.path.is_file():
            to_use = reader
        else:
            to_use = reader.path.with_suffix("")
        check_kwargs(dd.read_parquet, kwargs)
        return apply_index_and_date_dd(
            index_col, parse_dates, dd.read_parquet(to_use.__fspath__(), engine="pyarrow", **kwargs)
        )

    def read_hdf5(
            reader,
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

        if len(set_names) == 0:
            raise ValueError(
                "If using Dask, you must specify the dataset name to extract using set_names='aname'"
                "or a wildcard."
            )

        if update_cache:
            reader.path.update_cache()

        check_kwargs(dd.read_hdf, kwargs)
        if reader.path.fs_kind == "local":
            return dd.read_hdf(pattern=reader, key=set_names, **kwargs)
        f = tempfile.NamedTemporaryFile(delete=False, suffix=".hdf5")
        f.close()  # deletes the tmp file, but we can still use its name to download the remote file locally
        reader.path.get(f.name)
        data = TransparentPath.cli.submit(dd.read_hdf, f.name, set_names, **kwargs)
        # Do not delete the tmp file, since dask tasks are delayed
        return data.result()

    def read_excel(reader, update_cache: bool = True, **kwargs) -> pd.DataFrame:
        if update_cache:
            reader.path.update_cache()

        check_kwargs(pd.read_excel, kwargs)
        # noinspection PyTypeChecker,PyUnresolvedReferences
        try:
            if reader.path.fs_kind == "local":
                parts = delayed(pd.read_excel)(reader, **kwargs)
                return dd.from_delayed(parts)
            else:
                f = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
                f.close()  # deletes the tmp file, but we can still use its name
                reader.path.get(f.name)
                parts = delayed(pd.read_excel)(f.name, **kwargs)
                data = dd.from_delayed(parts)
                # We should not delete the tmp file, since dask does its operations lasily.
                return data
        except pd.errors.ParserError:
            # noinspection PyUnresolvedReferences
            raise pd.errors.ParserError(
                "Could not read data. Most likely, the file is encrypted. Ask your cloud"
                " manager to remove encryption on it."
            )


except ImportError:
    import warnings
    warnings.warn(errormessage)
