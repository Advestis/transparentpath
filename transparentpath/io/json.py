errormessage = (
    "Support for json does not seem to be installed for TransparentPath.\n"
    "You can change that by running 'pip install transparentpath[json]'."
)


class TPImportError(ImportError):
    def __init__(self, message: str = ""):
        self.message = f"Error in TransparentPath: {message}"
        super().__init__(self.message)


try:
    import json

    # noinspection PyPackageRequirements
    import numpy as np
    from typing import Any
    from datetime import date, datetime

    class JSONEncoder(json.JSONEncoder):
        """
        Extending the JSON encoder so it knows how to serialise a dataframe
        """

        def default(self, obj: Any):
            if hasattr(obj, "to_json"):
                if callable(obj.to_json):
                    try:
                        return obj.to_json(orient="split")
                    except TypeError:
                        return obj.to_json()
                else:
                    return obj.to_json
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, date):
                return obj.strftime("%Y-%m-%d")
            elif isinstance(obj, datetime):
                return obj.strftime("%Y-%m-%d %H:%M:%S")
            else:
                return json.JSONEncoder.default(self, obj)

    def read(self, *args, get_obj, update_cache, **kwargs):
        stringified = self.read_text(*args, get_obj=get_obj, update_cache=update_cache, **kwargs)
        dictified = json.loads(stringified)
        if isinstance(dictified, str):
            try:
                dictified = json.loads(dictified)
            except TypeError:
                pass
        return dictified

    def write(self, data: Any, overwrite: bool = True, present: str = "ignore", update_cache: bool = True, **kwargs):

        jsonified = json.dumps(data, cls=JSONEncoder)
        self.write_stuff(
            jsonified, "w", overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,
        )


except ImportError as e:
    raise TPImportError(str(e))
