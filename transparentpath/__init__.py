from .main.transparentpath import TransparentPath
from .main.transparentpath import TransparentPath as Path
from .main.transparentpath import TPImportError, TPTypeError, TPValueError, TPAttributeError, TPEnvironmentError,\
    TPIsADirectoryError, TPNotADirectoryError, TPFileNotFoundError, TPFileExistsError, TPMultipleExistenceError


try:
    from transparentpath.io.json import JSONEncoder
except ImportError:
    class JSONEncoder:
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "Support for json does not seem to be installed for TransparentPath.\n"
                "You can change that by running 'pip install transparentpath[json]'."
            )
