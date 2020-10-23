# transparentpath.jsonencoder package

## Submodules

## transparentpath.jsonencoder.jsonencoder module


### class transparentpath.jsonencoder.jsonencoder.JSONEncoder(\*, skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True, sort_keys=False, indent=None, separators=None, default=None)
Bases: `json.encoder.JSONEncoder`

Extending the JSON encoder so it knows how to serialise a dataframe


#### default(obj: Any)
Implement this method in a subclass such that it returns
a serializable object for `o`, or calls the base implementation
(to raise a `TypeError`).

For example, to support arbitrary iterators, you could
implement default like this:

```
def default(self, o):
    try:
        iterable = iter(o)
    except TypeError:
        pass
    else:
        return list(iterable)
    # Let the base class default method raise the TypeError
    return JSONEncoder.default(self, o)
```

## Module contents
