errormessage = (
    "h5py does not seem to be installed. You will not be able to use HDF5 files through "
    "TransparentPath.\nYou can change that by running 'pip install transparentpath[h5py]'."
)
hdf5_ok = False


class TPImportError(ImportError):
    def __init__(self, message: str = ""):
        self.message = f"Error in TransparentPath: {message}"
        super().__init__(self.message)


class MyHDFFile:
    def __init__(self):
        raise TPImportError(errormessage)


class MyHDFStore:
    def __init__(self):
        raise TPImportError(
            "pandas does not seem to be installed. You will not be able to use pandas objects through "
            "TransparentPath.\nYou can change that by running 'pip install transparentpath[pandas]'."
        )


try:
    import warnings
    import h5py
    import tempfile
    from typing import Union, Any
    from pathlib import Path
    from ..gcsutils.transparentpath import TransparentPath, TPValueError, TPFileNotFoundError
    from .pandas import MyHDFStore
    import sys
    import importlib.util

    hdf5_ok = True

    if importlib.util.find_spec("tables") is None:
        raise TPImportError("Need the 'tables' package")

    class MyHDFFile(h5py.File):
        """Class to override h5py.File to handle files on GCS.

        This allows to do :
        >>> from transparentpath import TransparentPath  # doctest: +SKIP
        >>> import numpy as np  # doctest: +SKIP
        >>> TransparentPath.set_global_fs("gcs", bucket="bucket_name")  # doctest: +SKIP
        >>> path = TransparentPath("chien.hdf5"  # doctest: +SKIP
        >>>
        >>> with path.write() as ifile:  # doctest: +SKIP
        >>>     ifile["data"] = np.array([1, 2])  # doctest: +SKIP
        """

        def __init__(self, *args, remote: Union[TransparentPath, None] = None, **kwargs):
            """Overload of h5py.File.__init__, to accept a 'remote' argument

            Parameters
            ----------
            args: tuple
                First argument is the local path to read. It can be a TransparentPath or a
                tempfile._TemporaryFileWrapper
            remote: Union[TransparentPath, None]
                Path to the file on GCS. Only relevant if file is opened in a 'with' statement in write mode. It
                will be used to put the modified local temporary HDF5 back to GCS. If specified, args[0] is expected to
                be a tempfile._TemporaryFileWrapper (Default value = None).
            kwargs: dict
            """
            self.remote_file = remote
            self.local_path = args[0]
            # noinspection PyUnresolvedReferences,PyProtectedMember
            if isinstance(self.local_path, tempfile._TemporaryFileWrapper):
                # noinspection PyUnresolvedReferences
                h5py.File.__init__(self, args[0].name, *args[1:], **kwargs)
            else:
                h5py.File.__init__(self, *args, **kwargs)

        def __exit__(self, *args):
            """Overload of the h5py.File.__exit__ method to push any modified local temporary HDF5 back to GCS after
            a 'with' statement."""
            h5py.File.__exit__(self, *args)
            if self.remote_file is not None:
                # noinspection PyUnresolvedReferences
                TransparentPath(self.local_path.name, fs="local").put(self.remote_file)
                # noinspection PyUnresolvedReferences
                self.local_path.close()

    def read(self: TransparentPath, use_pandas: bool = False, **kwargs,) -> Union[h5py.File, MyHDFStore]:
        """Reads a HDF5 file. Must have been created by h5py.File or pd.HDFStore (specify use_pandas=True if so)

        Since h5py.File/pd.HDFStore does not support GCS, first copy it in a tmp file.


        Parameters
        ----------
        self: TransparentPath

        use_pandas: bool
            To use HDFStore instead of h5py.File (Default value = False)

        kwargs
            The kwargs to pass to h5py.File/pd.HDFStore method, or to dask.dataframe.read_hdf()


        Returns
        -------
        Union[h5py.File, MyHDFStore]
            Opened h5py.File/pd.HDFStore

        """

        mode = "r"
        if "mode" in kwargs:
            mode = kwargs["mode"]
            del kwargs["mode"]
        if "r" not in mode:
            raise TPValueError("If using read_hdf5, mode must contain 'r'")

        class_to_use = h5py.File
        if use_pandas:
            class_to_use = MyHDFStore

        if not self.is_file():
            raise TPFileNotFoundError(f"Could not find file {self}")

        if self.fs_kind == "local":
            # Do not check kwargs since HDFStore and h5py both accepct kwargs anyway
            data = class_to_use(self.path, mode=mode, **kwargs)
        else:
            f = tempfile.NamedTemporaryFile(delete=False, suffix=".hdf5")
            f.close()  # deletes the tmp file, but we can still use its name
            self.get(f.name)
            # Do not check kwargs since HDFStore and h5py both accepct kwargs anyway
            data = class_to_use(f.name, mode=mode, **kwargs)
            Path(f.name).unlink()
        return data

    def write(
        self: TransparentPath, data: Any = None, set_name: str = None, use_pandas: bool = False, **kwargs,
    ) -> Union[None, h5py.File, MyHDFStore]:
        """

        Parameters
        ----------
        self: TransparentPath
        data: Any
            The data to store. Can be None, in that case an opened file is returned (Default value = None)
        set_name: str
            The name of the dataset (Default value = None)
        use_pandas: bool
            To use pd.HDFStore object instead of h5py.File (Default = False)
        **kwargs

        Returns
        -------
        Union[None, pd.HDFStore, h5py.File]

        """

        if self.suffix != ".hdf5" and self.suffix != "h5":
            warnings.warn(f"path {self} does not have '.h(df)5' as suffix while using to_hdf5. The path will be "
                          f"changed to a path with '.hdf5' as suffix")
            self.change_suffix(".hdf5")

        mode = "w"
        if "mode" in kwargs:
            mode = kwargs["mode"]
            del kwargs["mode"]

        if self.when_checked["used"] and not self.nocheck:
            self._check_multiplicity()

        # If no data is specified, an HDF5 file is returned, opened in write mode, or any other specified mode.
        if data is None:

            class_to_use = MyHDFFile
            if use_pandas:
                class_to_use = MyHDFStore

            if self.fs_kind == "local":
                return class_to_use(self.path, mode=mode, **kwargs)
            else:
                f = tempfile.NamedTemporaryFile(delete=True, suffix=".hdf5")
                return class_to_use(f, remote=self.path, mode=mode, **kwargs)
        else:

            if isinstance(data, dict):
                sets = data
            else:
                if set_name is None:
                    set_name = "data"
                sets = {set_name: data}

            class_to_use = h5py.File
            if use_pandas:
                class_to_use = MyHDFStore

            if self.fs_kind == "local":
                thefile = class_to_use(self.path, mode=mode, **kwargs)
                for aset in sets:
                    thefile[aset] = sets[aset]
                thefile.close()
            else:
                with tempfile.NamedTemporaryFile(delete=True, suffix=".hdf5") as f:
                    thefile = class_to_use(f.name, mode=mode, **kwargs)
                    for aset in sets:
                        thefile[aset] = sets[aset]
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

    try:
        # noinspection PyUnresolvedReferences
        from .pandas import MyHDFStore as Hs

        MyHDFStore = Hs
    except ImportError:
        pass

except ImportError as e:
    raise TPImportError(str(e))
