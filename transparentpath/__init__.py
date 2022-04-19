from .gcsutils.transparentpath import TransparentPath
from .gcsutils.transparentpath import TransparentPath as Path
from .gcsutils.transparentpath import TPMultipleExistenceError


try:
    from transparentpath.io.json import JSONEncoder
except ImportError:
    class JSONEncoder:
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "Support for json does not seem to be installed for TransparentPath.\n"
                "You can change that by running 'pip install transparentpath[json]'."
            )

from . import _version
__version__ = _version.get_versions()['version']
