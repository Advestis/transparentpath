errormessage = (
    "Support for json does not seem to be installed for TransparentPath.\n"
    "You can change that by running 'pip install transparentpath[json]'."
)


try:
    import json
    import warnings
    import numpy as np
    import pandas as pd
    from typing import Any
    from datetime import date, datetime
    import base64

    class JSONEncoder(json.JSONEncoder):
        """
        Extending the JSON encoder so it knows how to serialise a dataframe
        """

        def default(self, obj: Any):
            if obj.__class__.__name__ == "TransparentPath":
                return str(obj)
            if isinstance(obj, (pd.DataFrame, pd.Series)):
                dct = json.loads(obj.to_json(orient="split"))
                if isinstance(obj, pd.DataFrame):
                    dct["dtypes"] = dict(obj.dtypes.astype(str))
                else:
                    dct["dtype"] = str(obj.dtype)
                dct["datetimeindex"] = isinstance(obj.index, pd.DatetimeIndex)
                return dct
            if isinstance(obj, np.ndarray):
                return dict(__ndarray__=obj.tolist(), dtype=str(obj.dtype), shape=obj.shape)
            if isinstance(obj, (datetime, pd.Timestamp)):
                return obj.strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(obj, date):
                return obj.strftime("%Y-%m-%d")
            if isinstance(obj, pd.Timedelta):
                return {"__pd.Timedelta__": str(obj)}
            if hasattr(obj, "name"):
                return obj.name
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return super(JSONEncoder, self).default(self, obj)

    def json_obj_hook(dct):
        if isinstance(dct, dict) and "__ndarray__" in dct:
            return np.array(dct["__ndarray__"], dct["dtype"]).reshape(dct["shape"])
        if isinstance(dct, dict) and "__pd.Timedelta__" in dct and len(dct) == 1:
            return pd.Timedelta(dct["__pd.Timedelta__"])
        elif isinstance(dct, dict) and "columns" in dct and "data" in dct and "datetimeindex" in dct:
            possible_keys = ["data", "index", "dtypes", "columns", "datetimeindex"]
            if len(dct) > 5:  # not a pd.DataFrame
                return dct
            if any([k not in possible_keys for k in dct]):  # not a pd.DataFrame either
                return dct
            df = pd.DataFrame(dct["data"], columns=dct["columns"])
            columns_are_ints = isinstance(df.columns, pd.Index) and df.columns.dtype == "int64"
            if "index" in dct:
                index = dct["index"]
                if len(index) != 0 and isinstance(index[0], (list, tuple)):
                    df.index = pd.MultiIndex.from_tuples(index)
                else:
                    df.index = index
            if "dtypes" in dct:
                for column in dct["dtypes"]:
                    column_in_df = column
                    if columns_are_ints:
                        column_in_df = int(column)
                    if dct["dtypes"][column].startswith("datetime64") and df[column_in_df].dtype in (int, float):
                        df[column_in_df] = pd.to_datetime(df[column_in_df], unit="ms")
                    else:
                        df[column_in_df] = df[column_in_df].astype(dct["dtypes"][column])
            if dct["datetimeindex"] is True:
                if df.index.dtype in (int, float):
                    # noinspection PyTypeChecker
                    df.index = pd.to_datetime(df.index, unit="ms")
                else:
                    df.index = pd.DatetimeIndex(df.index)
            return df
        elif isinstance(dct, dict) and "data" in dct and "datetimeindex" in dct:
            possible_keys = ["data", "index", "dtype", "name", "datetimeindex"]
            if len(dct) > 5:  # not a pd.Series
                return dct
            if any([k not in possible_keys for k in dct]):  # not a pd.Series either
                return dct
            s = pd.Series(dct["data"])
            if "index" in dct:
                index = dct["index"]
                if len(index) != 0 and isinstance(index[0], (list, tuple)):
                    s.index = pd.MultiIndex.from_tuples(index)
                else:
                    s.index = index
            if "dtype" in dct:
                if dct["dtype"].startswith("datetime64") and s.dtype in (int, float):
                    s = pd.to_datetime(s, unit="ms")
                else:
                    s = s.astype(dct["dtype"])
            if "name" in dct:
                s.name = dct["name"]
            if dct["datetimeindex"] is True:
                if s.index.dtype in (int, float):
                    # noinspection PyTypeChecker
                    s.index = pd.to_datetime(s.index, unit="ms")
                else:
                    s.index = pd.DatetimeIndex(s.index)
            return s

        return dct

    def read(self, *args, get_obj, **kwargs):
        stringified = self.read_text(*args, get_obj=get_obj, **kwargs)
        dictified = json.loads(stringified, object_hook=json_obj_hook)
        if isinstance(dictified, str):
            try:
                dictified = json.loads(dictified)
            except TypeError:
                pass
        return dictified

    def write(self, data: Any, overwrite: bool = True, present: str = "ignore", **kwargs):

        if self.suffix != ".json":
            warnings.warn(
                f"path {self} does not have '.json' as suffix while using to_json. The path will be "
                "changed to a path with '.json' as suffix"
            )
            self.change_suffix(".json")
        jsonified = json.dumps(data, cls=JSONEncoder)
        self.write_stuff(
            jsonified,
            "w",
            overwrite=overwrite,
            present=present,
            **kwargs,
        )

    def to_plotly_json(self):
        """For compatibility with Plotly Dash"""
        return str(self)


except ImportError as e:
    raise ImportError(str(e))
