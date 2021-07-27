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

    old_joblib_load = joblib.numpy_pickle.load

    def myload():
        pass


    def overload_joblib_load():
        setattr(joblib.numpy_pickle, "open", myload)


    def set_old_joblib_load():
        setattr(joblib.numpy_pickle, "open", old_joblib_load)


except ImportError as e:
    raise TPImportError(str(e))
