errormessage = (
    "joblib does not seem to be installed. You will not be able load joblib files through "
    "TransparentPath.\nYou can change that by running 'pip install transparentpath[joblib]'."
)


class TPImportError(ImportError):
    def __init__(self, message: str = ""):
        self.message = f"Error in TransparentPath: {message}"
        super().__init__(self.message)


try:
    import joblib
    from ..gcsutils.transparentpath import TransparentPath
    from pathlib import Path

    old_joblib_load = joblib.load

    # noinspection PyProtectedMember,PyUnresolvedReferences
    def myload(filename, mmap_mode=None):
        """Reconstruct a Python object from a file persisted with joblib.dump.

        Read more in the :ref:`User Guide <persistence>`.

        WARNING: joblib.load relies on the pickle module and can therefore
        execute arbitrary Python code. It should therefore never be used
        to load files from untrusted sources.

        Parameters
        -----------
        filename: str, pathlib.Path, or file object.
            The file object or path of the file from which to load the object
        mmap_mode: {None, 'r+', 'r', 'w+', 'c'}, optional
            If not None, the arrays are memory-mapped from the disk. This
            mode has no effect for compressed files. Note that in this
            case the reconstructed object might no longer match exactly
            the originally pickled object.

        Returns
        -------
        result: any Python object
            The object stored in the file.

        See Also
        --------
        joblib.dump : function to save an object

        Notes
        -----

        This function can load numpy array files saved separately during the
        dump. If the mmap_mode argument is given, it is passed to np.load and
        arrays are loaded as memmaps. As a consequence, the reconstructed
        object might not match the original pickled object. Note that if the
        file was saved with compression, the arrays cannot be memmapped.
        """
        if Path is not None and isinstance(filename, Path):
            filename = str(filename)

        if hasattr(filename, "read") and not isinstance(filename, TransparentPath):
            fobj = filename
            filename = getattr(fobj, 'name', '')
            with joblib.numpy_pickle_utils._read_fileobject(fobj, filename, mmap_mode) as fobj:
                obj = joblib.numpy_pickle._unpickle(fobj)
        else:
            with open(filename, 'rb') as f:
                with joblib.numpy_pickle_utils._read_fileobject(f, filename, mmap_mode) as fobj:
                    if isinstance(fobj, str):
                        # if the returned file object is a string, this means we
                        # try to load a pickle file generated with an version of
                        # Joblib so we load it with joblib compatibility function.
                        return joblib.numpy_pickle_compat.load_compatibility(fobj)

                    obj = joblib.numpy_pickle._unpickle(fobj, filename, mmap_mode)
        return obj


    def overload_joblib_load():
        setattr(joblib, "load", myload)


    def set_old_joblib_load():
        setattr(joblib, "load", old_joblib_load)


except ImportError as e:
    raise TPImportError(str(e))
