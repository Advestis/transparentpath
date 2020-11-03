import json
import builtins
import tempfile
import sys
from typing import Any, Callable, List, IO, Union, Tuple
from pathlib import Path
from ..gcsutils.transparentpath import TransparentPath

from ..jsonencoder.jsonencoder import JSONEncoder
from inspect import signature

from .dask import apply_index_and_date_dd, client_
from .pandas import apply_index_and_date_pd, MyHDFStore
from .hdf5 import MyHDFFile


def check_dask():
    if "dask" not in sys.modules:
        raise ImportError("Daks does not seem to be installed. You will not be able to use Dask DataFrames"
                          " in TransparentPath.\n You can change that by running"
                          " 'pip install transparentpath[dask]'.")


builtins_open = builtins.open


def myopen(*args, **kwargs) -> IO:
    """Method overloading builtins' 'open' method, allowing to open files on GCS using TransparentPath."""
    if len(args) == 0:
        raise ValueError("open method needs arguments.")
    thefile = args[0]
    if type(thefile) == str and "gs://" in thefile:
        if "gcs" not in TransparentPath.fss:
            raise OSError("You are trying to access a file on GCS without having set a proper GCSFileSystem first.")
        thefile = TransparentPath(thefile.replace(f"gs://{TransparentPath.bucket}", ""), fs="gcs")
    if isinstance(thefile, TransparentPath):
        return thefile.open(*args[1:], **kwargs)
    elif (
        isinstance(thefile, str) or isinstance(thefile, Path) or isinstance(thefile, int) or isinstance(thefile, bytes)
    ):
        return builtins_open(*args, **kwargs)
    else:
        raise ValueError(f"Unknown type {type(thefile)} for path argument")


setattr(builtins, "open", myopen)


def get_index_and_date_from_kwargs(**kwargs: dict) -> Tuple[int, bool, dict]:
    index_col = kwargs.get("index_col", None)
    parse_dates = kwargs.get("parse_dates", None)
    if index_col is not None:
        del kwargs["index_col"]
    if parse_dates is not None:
        del kwargs["parse_dates"]
    # noinspection PyTypeChecker
    return index_col, parse_dates, kwargs


def apply_index_and_date(index_col: int, parse_dates: bool, df):
    if "pandas" in str(type(df)):
        return apply_index_and_date_pd(index_col, parse_dates, df)
    elif "dask" in str(type(df)):
        return apply_index_and_date_dd(index_col, parse_dates, df)
    else:
        raise TypeError(f"Unexpected type {type(df)} for argument df")


def check_kwargs(method: Callable, kwargs: dict):
    """Takes as argument a method and some kwargs. Will look in the method signature and return in two separate dicts
    the kwargs that are in the signature and those that are not.

    If the method does not return any signature or if it explicitely accepts **kwargs, does not do anything
    """
    unexpected_kwargs = []
    s = ""
    try:
        sig = signature(method)
        if "kwargs" in sig.parameters or "kwds" in sig.parameters:
            return
        for arg in kwargs:
            if arg not in sig.parameters:
                unexpected_kwargs.append(kwargs[arg])

        if len(unexpected_kwargs) > 0:
            s = f"You provided unexpected kwargs for method {method.__name__}:"
            s = "\n  - ".join([s] + unexpected_kwargs)
    except ValueError:
        return

    if s != "":
        raise ValueError(s)


class TransparentIO:

    def __init__(self, path: TransparentPath):
        self.__path = path

    @property
    def path(self):
        return self.__path

    def open(self, *arg, **kwargs) -> IO:
        """ Uses the file system open method

        Parameters
        ----------
        arg
            Any args valid for the builtin open() method

        kwargs
            Any kwargs valid for the builtin open() method


        Returns
        -------
        IO
            The IO buffer object

        """

        self.path.check_multiplicity()
        check_kwargs(self.path.fs.open, kwargs)
        return self.path.fs.open(self.path.__path, *arg, **kwargs)

    def read(
            self,
            *args,
            get_obj: bool = False,
            use_pandas: bool = False,
            update_cache: bool = True,
            use_dask: bool = False,
            **kwargs,
    ) -> Any:
        """Method used to read the content of the file located at self

        Will raise FileNotFound error if there is no file. Calls a specific method to read self based on the suffix
        of self.path.path:

            1: .csv : will use pandas's read_csv

            2: .parquet : will use pandas's read_parquet with pyarrow engine

            3: .hdf5 or .h5 : will use h5py.File or pd.HDFStore (if use_pandas = True). Since it does not support
            remote file systems, the file will be downloaded localy in a tmp file read, then removed.

            4: .json : will use open() method to get file content then json.loads to get a dict

            5: .xlsx : will use pd.read_excel

            6: any other suffix : will return a IO buffer to read from, or the string contained in the file if
            get_obj is False.


        Parameters
        ----------
        get_obj: bool
            Only relevant for files that are not csv, parquet nor HDF5. If True returns the IO Buffer,
            else the string contained in the IO Buffer (Default value = False)

        use_pandas: bool
            Must pass it as True if hdf5 file was written using HDFStore and not h5py.File (Default value = False)

        update_cache: bool
            FileSystem objects do not necessarily follow changes on the system in real time if they were not
            perfermed by them directly. If update_cache is True, the FileSystem will update its cache before trying
            to read anything. If False, it won't, potentially saving some time but this might result in a
            FileNotFoundError. (Default value = True)

        use_dask: bool
            To return a Dask DataFrame instead of a pandas DataFrame. Only makes sense if file suffix is xlsx, csv,
            parquet. (Default value = False)

        args:
            any args to pass to the underlying reading method

        kwargs:
            any kwargs to pass to the underlying reading method


        Returns
        -------
        Any

        """
        self.path.check_multiplicity()
        if use_dask:
            if TransparentPath.cli is None:
                TransparentPath.cli = client.Client(processes=False)
            if self.path.suffix != ".csv" and self.path.suffix != ".parquet" and not self.path.is_file():
                raise FileNotFoundError(f"Could not find file {self}")
            else:
                if (
                        not self.path.is_file()
                        and not self.path.is_dir(exist=True)
                        and not self.path.with_suffix("").is_dir(exist=True)
                        and "*" not in str(self)
                ):
                    raise FileNotFoundError(f"Could not find file nor directory {self}")
        else:
            if not self.path.is_file():
                raise FileNotFoundError(f"Could not find file {self}")

        if self.path.suffix == ".csv":
            return self.path.read_csv(update_cache=update_cache, use_dask=use_dask, **kwargs)
        elif self.path.suffix == ".parquet":
            index_col = None
            if "index_col" in kwargs:
                index_col = kwargs["index_col"]
                del kwargs["index_col"]
            content = self.path.read_parquet(update_cache=update_cache, use_dask=use_dask, **kwargs)
            if index_col:
                content.set_index(content.columns[index_col])
            return content
        elif self.path.suffix == ".hdf5" or self.path.suffix == ".h5":
            return self.path.read_hdf5(update_cache=update_cache, use_pandas=use_pandas, use_dask=use_dask, **kwargs)
        elif self.path.suffix == ".json":
            jsonified = self.path.read_text(*args, get_obj=get_obj, update_cache=update_cache, **kwargs)
            return json.loads(jsonified)
        elif self.path.suffix == ".xlsx":
            return self.path.read_excel(update_cache=update_cache, use_dask=use_dask, **kwargs)
        else:
            return self.path.read_text(*args, get_obj=get_obj, update_cache=update_cache, **kwargs)

    def read_csv(self, update_cache: bool = True, use_dask: bool = False, **kwargs) -> pd.DataFrame:
        if update_cache:
            self.path.update_cache()

        # noinspection PyTypeChecker,PyUnresolvedReferences
        try:
            if use_dask:
                check_dask()
                if self.path.is_file():
                    to_use = self
                else:
                    to_use = self.path.with_suffix("")
                index_col, parse_dates, kwargs = get_index_and_date_from_kwargs(**kwargs)
                check_kwargs(dd.read_csv, kwargs)
                return apply_index_and_date(index_col, parse_dates, dd.read_csv(to_use.__fspath__(), **kwargs))

            check_kwargs(pd.read_csv, kwargs)
            return pd.read_csv(self.path.__fspath__(), **kwargs)

        except pd.errors.ParserError:
            # noinspection PyUnresolvedReferences
            raise pd.errors.ParserError(
                "Could not read data. Most likely, the file is encrypted."
                " Ask your cloud manager to remove encryption on it."
            )

    def read_parquet(
            self, update_cache: bool = True, use_dask: bool = False, **kwargs
    ) -> Union[pd.DataFrame, pd.Series]:
        if update_cache:
            self.path.update_cache()

        index_col, parse_dates, kwargs = get_index_and_date_from_kwargs(**kwargs)

        if use_dask:
            check_dask()
            # Dask's read_parquet supports remote files, pandas does not
            if self.path.is_file():
                to_use = self
            else:
                to_use = self.path.with_suffix("")
            check_kwargs(dd.read_parquet, kwargs)
            return apply_index_and_date(
                index_col, parse_dates, dd.read_parquet(to_use.__fspath__(), engine="pyarrow", **kwargs)
            )

        check_kwargs(pd.read_parquet, kwargs)
        if self.path.fs_kind == "local":
            return apply_index_and_date(index_col, parse_dates, pd.read_parquet(str(self), engine="pyarrow", **kwargs))

        else:
            return apply_index_and_date(
                index_col, parse_dates, pd.read_parquet(self.path.open("rb"), engine="pyarrow", **kwargs)
            )

    def read_text(self, *args, get_obj: bool = False, update_cache: bool = True, **kwargs) -> Union[str, IO]:
        if update_cache:
            self.path.update_cache()
        byte_mode = True
        if len(args) == 0:
            byte_mode = False
            args = ("rb",)
        if "b" not in args[0]:
            byte_mode = False
            args[0] += "b"
        if get_obj:
            return self.path.open(*args, **kwargs)

        with self.path.open(*args, **kwargs) as f:
            to_ret = f.read()
            if not byte_mode:
                to_ret = to_ret.decode()
        return to_ret

    def read_hdf5(
            self,
            update_cache: bool = True,
            use_pandas: bool = False,
            use_dask: bool = False,
            set_names: str = "",
            **kwargs,
    ) -> Union[h5py.File, pd.HDFStore, dd.DataFrame]:
        """Reads a HDF5 file. Must have been created by h5py.File or pd.HDFStore (specify use_pandas=True if so)

        Since h5py.File/pd.HDFStore does not support GCS, first copy it in a tmp file.


        Parameters
        ----------

        update_cache: bool
            FileSystem objects do not necessarily follow changes on the system if they were not perfermed by them
            directly. If update_cache is True, the FileSystem will update its cache before trying to read anything.
            If False, it won't, potentially saving some time but this might result in a FileNotFoundError. (Default
            value = True)

        use_pandas: bool
            To use HDFStore instead of h5py.File (Default value = False)

        use_dask: bool
            To indicate that the HDF5 should be opened using dask.dataframe.read_hdf(). (Default value = False)

        set_names: str
            See using dask.dataframe.read_hdf()'s 'key' argument. (Default value = "")
        kwargs
            The kwargs to pass to h5py.File/pd.HDFStore method, or to dask.dataframe.read_hdf()


        Returns
        -------
        Union[h5py.File, pd.HDFStore]
            Opened h5py.File/pd.HDFStore

        """

        mode = "r"
        if "mode" in kwargs:
            mode = kwargs["mode"]
            del kwargs["mode"]
        if "r" not in mode:
            raise ValueError("If using read_hdf5, mode must contain 'r'")

        if use_dask:
            check_dask()
            if len(set_names) == 0:
                raise ValueError(
                    "If using Dask, you must specify the dataset name to extract using set_names='aname'"
                    "or a wildcard."
                )
            check_kwargs(dd.read_hdf, kwargs)
            if self.path.fs_kind == "local":
                return dd.read_hdf(pattern=self, key=set_names, **kwargs)
            f = tempfile.NamedTemporaryFile(delete=False, suffix=".hdf5")
            f.close()  # deletes the tmp file, but we can still use its name to download the remote file locally
            self.path.get(f.name)
            data = TransparentPath.cli.submit(dd.read_hdf, f.name, set_names, **kwargs)
            # Do not delete the tmp file, since dask tasks are delayed
            return data.result()

        class_to_use = h5py.File
        if use_pandas:
            class_to_use = pd.HDFStore

        if update_cache:
            self.path.update_cache()
        if self.path.fs_kind == "local":
            # Do not check kwargs since HDFStore and h5py both accepct kwargs anyway
            data = class_to_use(self.path.__path, mode=mode, **kwargs)
        else:
            f = tempfile.NamedTemporaryFile(delete=False, suffix=".hdf5")
            f.close()  # deletes the tmp file, but we can still use its name
            self.path.get(f.name)
            # Do not check kwargs since HDFStore and h5py both accepct kwargs anyway
            data = class_to_use(f.name, mode=mode, **kwargs)
            Path(f.name).unlink()
        return data

    def read_excel(self, update_cache: bool = True, use_dask: bool = False, **kwargs) -> pd.DataFrame:
        if update_cache:
            self.path.update_cache()

        check_kwargs(pd.read_excel, kwargs)
        # noinspection PyTypeChecker,PyUnresolvedReferences
        try:
            if self.path.fs_kind == "local":
                if use_dask:
                    check_dask()
                    parts = delayed(pd.read_excel)(self, **kwargs)
                    return dd.from_delayed(parts)
                return pd.read_excel(self, **kwargs)
            else:
                f = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
                f.close()  # deletes the tmp file, but we can still use its name
                self.path.get(f.name)
                if use_dask:
                    check_dask()
                    parts = delayed(pd.read_excel)(f.name, **kwargs)
                    data = dd.from_delayed(parts)
                    # We should not delete the tmp file, since dask does its operations lasily.
                else:
                    data = pd.read_excel(f.name, **kwargs)
                    Path(f.name).unlink()
                return data
        except pd.errors.ParserError:
            # noinspection PyUnresolvedReferences
            raise pd.errors.ParserError(
                "Could not read data. Most likely, the file is encrypted. Ask your cloud"
                " manager to remove encryption on it."
            )

    def write(
            self,
            data: Any,
            *args,
            set_name: str = "data",
            use_pandas: bool = False,
            overwrite: bool = True,
            present: str = "ignore",
            update_cache: bool = True,
            make_parents: bool = False,
            **kwargs,
    ) -> Union[None, pd.HDFStore, h5py.File]:
        """Method used to write the content of the file located at self

        Calls a specific method to write data based on the suffix of self.path.path:

            1: .csv : will use pandas's to_csv

            2: .parquet : will use pandas's to_parquet with pyarrow engine

            3: .hdf5 or .h5 : will use h5py.File. Since it does not support remote file systems, the file will be
            created localy in a tmp filen written to, then uploaded and removed localy.

            4: .json : will use jsonencoder.JSONEncoder class. Works with DataFrames and np.ndarrays too.

            5: .xlsx : will use pandas's to_excel

            5: any other suffix : uses self.path.open to write to an IO Buffer


        Parameters
        ----------
        data: Any
            The data to write

        set_name: str
            Name of the dataset to write. Only relevant if using HDF5 (Default value = 'data')

        use_pandas: bool
            Must pass it as True if hdf file must be written using HDFStore and not h5py.File

        overwrite: bool
            If True, any existing file will be overwritten. Only relevant for csv, hdf5 and parquet files,
            since others use the 'open' method, which args already specify what to do (Default value = True).

        present: str
            Indicates what to do if overwrite is False and file is present. Here too, only relevant for csv,
            hsf5 and parquet files.

        update_cache: bool
            FileSystem objects do not necessarily follow changes on the system if they were not perfermed by them
            directly. If update_cache is True, the FileSystem will update its cache before trying to read anything.
            If False, it won't, potentially saving some time but this might result in a FileExistError. (Default
            value = True)

        make_parents: bool
            If True and if the parent arborescence does not exist, it is created. (Default value = False)

        args:
            any args to pass to the underlying writting method

        kwargs:
            any kwargs to pass to the underlying reading method


        Returns
        -------
        Union[None, pd.HDFStore, h5py.File]

        """
        self.path.check_multiplicity()
        if "dask" in str(type(data)):
            check_dask()

        if make_parents and not self.path.parent.is_dir():
            self.path.parent.mkdir()

        if self.path.suffix != ".hdf5" and self.path.suffix != ".h5" and data is None:
            data = args[0]
            args = args[1:]

        if self.path.suffix == ".csv":
            ret = self.path.to_csv(data=data, overwrite=overwrite, present=present, update_cache=update_cache, **kwargs)
            if ret is not None:
                # To skip the assert at the end of the function. Indeed if something is returned it means we used
                # Dask, which will have written files with a different name than self, so the assert would fail.
                return
        elif self.path.suffix == ".parquet":
            self.path.to_parquet(
                data=data, overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,
            )
            if isinstance(data, dd.DataFrame):
                assert self.path.with_suffix("").is_dir(exist=True)
                return
        elif self.path.suffix == ".hdf5" or self.path.suffix == ".h5":
            ret = self.path.to_hdf5(
                data=data, set_name=set_name, use_pandas=use_pandas, update_cache=update_cache, **kwargs,
            )
            if ret is not None:
                return ret
        elif self.path.suffix == ".json":
            self.path.to_json(
                data=data, overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,
            )
        elif self.path.suffix == ".txt":
            self.path.write_stuff(
                *args, data=data, overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,
            )
        elif self.path.suffix == ".xlsx":
            self.path.to_excel(
                data=data, overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,
            )
        else:
            self.path.write_bytes(
                *args, data=data, overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,
            )
        assert self.path.is_file()

    def write_stuff(
            self, data: Any, *args, overwrite: bool = True, present: str = "ignore", update_cache: bool = True, **kwargs
    ) -> None:
        if update_cache:
            self.path.update_cache()
        if not overwrite and self.path.is_file() and present != "ignore":
            raise FileExistsError()

        args = list(args)
        if len(args) == 0:
            args = ("w",)

        with self.path.open(*args, **kwargs) as f:
            f.write(data)

    def write_bytes(
            self, data: Any, *args, overwrite: bool = True, present: str = "ignore", update_cache: bool = True,
            **kwargs,
    ) -> None:

        args = list(args)
        if len(args) == 0:
            args = ("wb",)
        if "b" not in args[0]:
            args[0] += "b"

        self.path.write_stuff(
            data, *tuple(args), overwrite=overwrite, present=present, update_cache=update_cache, **kwargs
        )

    def to_csv(
            self,
            data: Union[pd.DataFrame, pd.Series, dd.DataFrame],
            overwrite: bool = True,
            present: str = "ignore",
            update_cache: bool = True,
            **kwargs,
    ) -> Union[None, List[TransparentPath]]:
        """
        Warning : if data is a Dask dataframe, the output might be written in multiple file. one can always specify
        single_file=True, but that is not supported in GCS. If only one file is produced, then this file will be
        moved to self.path.
        """
        if update_cache:
            self.path.update_cache()
        if not overwrite and self.path.is_file() and present != "ignore":
            raise FileExistsError()

        if isinstance(data, dd.DataFrame):
            if TransparentPath.cli is None:
                TransparentPath.cli = client.Client(processes=False)
            check_kwargs(dd.to_csv, kwargs)
            path_to_save = self.path
            if not path_to_save.stem.endswith("*"):
                path_to_save = path_to_save.parent / (path_to_save.stem + "_*.csv")
            futures = TransparentPath.cli.submit(dd.to_csv, data, path_to_save.__fspath__(), **kwargs)
            outfiles = [TransparentPath(f, fs=self.path.fs_kind) for f in futures.result()]
            if len(outfiles) == 1:
                outfiles[0].mv(self.path)
            else:
                return outfiles
        else:
            check_kwargs(data.to_csv, kwargs)
            data.to_csv(self.path.__fspath__(), **kwargs)

    def to_parquet(
            self,
            data: Union[pd.DataFrame, pd.Series, dd.DataFrame],
            overwrite: bool = True,
            present: str = "ignore",
            update_cache: bool = True,
            columns_to_string: bool = True,
            to_dataframe: bool = True,
            **kwargs,
    ) -> None:
        """
        Warning : if data is a Dask dataframe, the output will be written in a directory. For convenience, the directory
        if self.path.with_suffix(""). Reading is transparent and one can specify a path with .parquet suffix.
        """
        if update_cache:
            self.path.update_cache()
        if not overwrite and self.path.is_file() and present != "ignore":
            raise FileExistsError()

        if to_dataframe and isinstance(data, pd.Series):
            name = data.name
            data = pd.DataFrame(data=data)
            if name is not None:
                data.columns = [name]

        if columns_to_string and not isinstance(data.columns[0], str):
            data.columns = data.columns.astype(str)

        # noinspection PyTypeChecker
        if isinstance(data, dd.DataFrame):
            if TransparentPath.cli is None:
                TransparentPath.cli = client.Client(processes=False)
            check_kwargs(dd.to_parquet, kwargs)
            dd.to_parquet(
                data, self.path.with_suffix("").__fspath__(), engine="pyarrow", compression="snappy", **kwargs
            )
        else:
            check_kwargs(data.to_parquet, kwargs)
            data.to_parquet(self.path.open("wb"), engine="pyarrow", compression="snappy", **kwargs)

    def to_hdf5(
            self, data: Any = None, set_name: str = None, update_cache: bool = True, use_pandas: bool = False, **kwargs,
    ) -> Union[None, h5py.File, pd.HDFStore]:
        """

        Parameters
        ----------
        data: Any
            The data to store. Can be None, in that case an opened file is returned (Default value = None)
        set_name: str
            The name of the dataset (Default value = None)
        update_cache: bool
            FileSystem objects do not necessarily follow changes on the system if they were not perfermed by them
            directly. If update_cache is True, the FileSystem will update its cache before trying to read anything.
            If False, it won't, potentially saving some time but this might result in a FileExistError. (Default
            value = True)
        use_pandas: bool
            To use pd.HDFStore object instead of h5py.File (Default = False)
        **kwargs

        Returns
        -------
        Union[None, pd.HDFStore, h5py.File]

        """

        mode = "w"
        if "mode" in kwargs:
            mode = kwargs["mode"]
            del kwargs["mode"]

        if update_cache:
            self.path.update_cache()

        # If no data is specified, an HDF5 file is returned, opened in write mode, or any other specified mode.
        if data is None:

            class_to_use = MyHDFFile
            if use_pandas:
                class_to_use = MyHDFStore
            if self.path.fs_kind == "local":
                return class_to_use(self.path.__path, mode=mode, **kwargs)
            else:
                f = tempfile.NamedTemporaryFile(delete=True, suffix=".hdf5")
                return class_to_use(f, remote=self.path.__path, mode=mode, **kwargs)
        else:

            if isinstance(data, dict):
                sets = data
            else:
                if set_name is None:
                    set_name = "data"
                sets = {set_name: data}

            class_to_use = h5py.File
            if use_pandas:
                class_to_use = pd.HDFStore
                if isinstance(data, dd.DataFrame):
                    raise NotImplementedError(
                        "TransparentPath does not support storing Dask objects in pandas's HDFStore yet."
                    )

            if isinstance(data, dd.DataFrame):

                # raise NotImplementedError(f"For whatever fucking stupid reason, Dask will not be able to read your "
                #                           f"dataframe back, and pandas will read bullshit from it. So don't"
                #                           f"try to write into HDF5 using Dask.")

                if TransparentPath.cli is None:
                    TransparentPath.cli = client.Client(processes=False)
                check_kwargs(dd.to_hdf, kwargs)
                if self.path.fs_kind == "local":
                    for aset in sets:
                        dd.to_hdf(sets[aset], self, aset, mode=mode, **kwargs)
                else:
                    with tempfile.NamedTemporaryFile() as f:
                        futures = TransparentPath.cli.map(
                            dd.to_hdf, list(sets.values()), [f.name] * len(sets), list(sets.keys()), mode=mode, **kwargs
                        )
                        TransparentPath.cli.gather(futures)
                        TransparentPath(path=f.name, fs="local").put(self.path.__path)
                return

            if self.path.fs_kind == "local":
                thefile = class_to_use(self.path.__path, mode=mode, **kwargs)
                for aset in sets:
                    thefile[aset] = sets[aset]
                thefile.close()
            else:
                with tempfile.NamedTemporaryFile(delete=True, suffix=".hdf5") as f:
                    thefile = class_to_use(f.name, mode=mode, **kwargs)
                    for aset in sets:
                        thefile[aset] = sets[aset]
                    thefile.close()
                    TransparentPath(path=f.name, fs="local").put(self.path.__path)

    def to_json(self, data: Any, overwrite: bool = True, present: str = "ignore", update_cache: bool = True, **kwargs):

        jsonified = json.dumps(data, cls=JSONEncoder)
        self.path.write_stuff(
            jsonified, "w", overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,
        )

    def to_excel(
            self,
            data: Union[pd.DataFrame, pd.Series, dd.DataFrame],
            overwrite: bool = True,
            present: str = "ignore",
            update_cache: bool = True,
            **kwargs,
    ) -> None:
        if update_cache:
            self.path.update_cache()
        if not overwrite and self.path.is_file() and present != "ignore":
            raise FileExistsError()

        # noinspection PyTypeChecker

        if self.path.fs_kind == "local":
            if isinstance(data, dd.DataFrame):
                if TransparentPath.cli is None:
                    TransparentPath.cli = client.Client(processes=False)
                check_kwargs(pd.DataFrame.to_excel, kwargs)
                parts = delayed(pd.DataFrame.to_excel)(data, self.path.__fspath__(), **kwargs)
                parts.compute()
                return
            data.to_excel(self, **kwargs)
        else:
            with tempfile.NamedTemporaryFile(delete=True, suffix=".xlsx") as f:
                if isinstance(data, dd.DataFrame):
                    if TransparentPath.cli is None:
                        TransparentPath.cli = client.Client(processes=False)
                    check_kwargs(pd.DataFrame.to_excel, kwargs)
                    parts = delayed(pd.DataFrame.to_excel)(data, f.name, **kwargs)
                    parts.compute()
                else:
                    check_kwargs(data.to_excel, kwargs)
                    data.to_excel(f.name, **kwargs)
                TransparentPath(path=f.name, fs="local").put(self.path.__path)
