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

except ImportError:
    import warnings
    warnings.warn(errormessage)