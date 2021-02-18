errormessage = (
    "h5py does not seem to be installed. You will not be able to use HDF5 files through "
    "TransparentPath.\nYou can change that by running 'pip install transparentpath[h5py]'."
)
hdf5_ok = False

TPImportError = ImportError


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
    # noinspection PyUnresolvedReferences
    import h5py
    import tempfile
    from typing import Union, Any
    from pathlib import Path
    from ..gcsutils.transparentpath import TransparentPath, TPImportError, TPValueError
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
        >>> TransparentPath.set_global_fs("gcs", bucket="bucket_name", project="project_name")  # doctest: +SKIP
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

    def read(
        self: TransparentPath, update_cache: bool = True, use_pandas: bool = False, **kwargs,
    ) -> Union[h5py.File, MyHDFStore]:
        """Reads a HDF5 file. Must have been created by h5py.File or pd.HDFStore (specify use_pandas=True if so)

        Since h5py.File/pd.HDFStore does not support GCS, first copy it in a tmp file.


        Parameters
        ----------
        self: TransparentPath
        update_cache: bool
            FileSystem objects do not necessarily follow changes on the system if they were not perfermed by them
            directly. If update_cache is True, the FileSystem will update its cache before trying to read anything.
            If False, it won't, potentially saving some time but this might result in a FileNotFoundError. (Default
            value = True)

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

        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()
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
        self: TransparentPath,
        data: Any = None,
        set_name: str = None,
        update_cache: bool = True,
        use_pandas: bool = False,
        **kwargs,
    ) -> Union[None, h5py.File, MyHDFStore]:
        """

        Parameters
        ----------
        self: TransparentPath
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

        # noinspection PyProtectedMember
        if update_cache and self.__class__._do_update_cache:
            self._update_cache()

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
                    TransparentPath(path=f.name, fs="local", bucket=self.bucket, project=self.project).put(self.path)

    try:
        # noinspection PyUnresolvedReferences
        from .pandas import MyHDFStore as Hs
        MyHDFStore = Hs
    except ImportError:
        pass

except ImportError as e:
    # import warnings
    # warnings.warn(f"{errormessage}. Full ImportError message was:\n{e}")
    raise TPImportError(str(e))
