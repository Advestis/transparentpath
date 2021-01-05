from .gcsutils.transparentpath import TransparentPath
from .gcsutils.transparentpath import TransparentPath as Path
from .io.json import errormessage


try:
    from transparentpath.io.json import JSONEncoder
except ImportError:
    class JSONEncoder:
        def __init__(self, *args, **kwargs):
            raise ImportError(errormessage)
