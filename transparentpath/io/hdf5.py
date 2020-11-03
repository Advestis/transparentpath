class MyHDFFile:
    def __init__(self):
        raise ImportError(
            "h5py does not seem to be installed. You will not be able to use HDF5 files through "
            "TransparentPath.\nYou can change that by running 'pip install transparentpath[h5py]'."
        )


try:
    # noinspection PyUnresolvedReferences
    import h5py
    import tempfile
    from typing import Union
    from..gcsutils.transparentpath import TransparentPath


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
                h5py.File.__init__(self, args[0].name, *args[1:], **kwargs)
            else:
                h5py.File.__init__(self, *args, **kwargs)

        def __exit__(self, *args):
            """Overload of the h5py.File.__exit__ method to push any modified local temporary HDF5 back to GCS after
            a 'with' statement."""
            h5py.File.__exit__(self, *args)
            if self.remote_file is not None:
                TransparentPath(self.local_path.name, fs="local").put(self.remote_file)
                self.local_path.close()

except ImportError:
    import warnings
    warnings.warn("h5py does not seem to be installed. You will not be able to use HDF5 files through "
                  "TransparentPath.\nYou can change that by running 'pip install transparentpath[h5py]'.")
