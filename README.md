---
permalink: /docs/index.html
---

**The complete documentation is available at https://advestis.github.io/transparentpath/**

# TransparentPath

A class that allows one to use a path in a local file system or a gcs file system (more or less) in almost the
same way one would use a pathlib.Path object.

## Requirements

You will need credential .json file, that you can set in the envvar GOOGLE_APPLICATION_CREDENTIALS.
If your python code is launched in a google cloud instance (VM, pods, etc...), GOOGLE_APPLICATION_CREDENTIALS should 
be set by default. 
 
## Installation

You can install this package with pip :

    pip install transparentpath-nightly

Or use it in a Dockerfile:

    FROM advestis/transparentpath-nightly
    ...

## Optional packages

The vanilla version allows you to declare paths and work with them. You can use them in the builtin `open` method. 
Optionally, you can also install support for several other packages like pandas, dask, etc... the currently 
available optionnal packages are accessible through the follownig commands: 

    pip install transparentpath-nightly[pandas]
    pip install transparentpath-nightly[parquet]
    pip install transparentpath-nightly[hdf5]
    pip install transparentpath-nightly[json]
    pip install transparentpath-nightly[excel]
    pip install transparentpath-nightly[dask]

you can install all of those at once

    pip install transparentpath-nightly[all]

## Usage

Set TransparentPath to point to GCS:
```python
from transparentpath import TransparentPath as Path
Path.set_global_fs("gcs", bucket="bucket_name")
mypath = Path("foo") / "bar"  # Will use GCS
local_path = Path("chien", fs="local")  # will NOT use GCS
other_path = mypath / "stuff"  # Will use GCS
other_path_2 = local_path / "stuff"  # Will NOT use GCS
```

or

```python
from transparentpath import TransparentPath as Path
mypath = Path("foo", fs='gcs', bucket="my_bucket_name")  # Will use GCS
local_path = Path("chien", fs="local")  # will NOT use GCS 
other_local_path = Path("foo2")  # will NOT use GCS
```

or

```python
# noinspection PyShadowingNames
from transparentpath import TransparentPath as Path
mypath = Path("gs://my_bucket_name/foo")  # Will use GCS
other_path = Path("foo2")  # will NOT use GCS
```

No matter whether you are using GCS or your local file system, the following commands are valid:

```python
from transparentpath import TransparentPath as Path
# Path.set_global_fs("gcs", bucket="bucket_name", project="project_name")
# The following lines will also work with the previous line uncommented 

# Reading a csv into a pandas' DataFrame and saving it as a parquet file
mypath = Path("foo") / "bar.csv"
df = mypath.read(index_col=0, parse_dates=True)
otherpath = mypath.with_suffix(".parquet")
otherpath.write(df)

# Reading and writing a HDF5 file works on GCS and on local:
import numpy as np
mypath = Path("foo") / "bar.hdf5"  # can be .h5 too
with mypath.read() as ifile:
    arr = np.array(ifile["store1"])

# Doing '..' from 'foo/bar.hdf5' will return 'foo'
# Then doing 'foo' + 'babar.hdf5' will return 'foo/babar.hdf5' ('+' and '/' are synonymes)
mypath.cd("..")  # Does not return a path but modifies inplace
with (mypath  + "babar.hdf5").write(None) as ofile:
    # Note here that we must explicitely give 'None' to the 'write' method in order for it
    # to return the open HDF5 file. We could also give a dict of {arr: "store1"} to directly
    # write the file.
    ofile["store1"] = arr


# Reading a text file. Can also use 'w', 'a', etc... also works with binaries.
mypath = Path("foo") / "bar.txt"
with open(mypath, "r") as ifile:
    lines = ifile.readlines()

# open is overriden to understand gs://
with open("gs://bucket/file.txt", "r") as ifile:
    lines = ifile.readlines()

mypath.is_file()
mypath.is_dir()  # Specific behavior on GCS. See 'Behavior' below.
mypath.is_file()
files = mypath.parent.glob("*.csv")  # Returns a Iterator[TransparentPath], can be casted to list
```

As you can see from the previous example, all methods returning a path from a TransparentPath return a 
TransparentPath.

### Dask

TransparentPath supports writing and reading Dask dataframes from and to csv, excel, parquet and HDF5, both locally and
remotely. You need to have dask-dataframe and dask-distributed installed, which will be the case if you ran `pip 
install transparentpath-nightly[dask]`. Writing Dask dataframes does not require any additionnal arguments to be passed
for the type will be checked before calling the appropriate writting method. Reading however requires you to pass 
the *use_dask* argument to the `read()` method. If the file to read is HDF5, you will also need to specify 
*set_names*, matching the argument *key* of Dask's `read_hdf()` method.

Note that if reading a remote HDF5, the file will be downloaded in your local tmp, then read. If not using Dask, the 
file is deleted after being read. But since Dask uses delayed processes, deleting the file might occure before the 
file is actually read, so the file is kept. Up to you to empty your /tmp directory if it is not done automatically 
by your system.


Do not hesitate to read the documentation in **docs/** for more details on each method.


## Behavior

All instances of TransparentPath are absolute, even if created with relative paths.

TransparentPaths are seen as instances of str: 

```python
from transparentpath import TransparentPath as Path
path = Path()
isinstance(path, str)  # returns True
```
 
This is required to allow
 
```python
from transparentpath import TransparentPath as Path
path = Path()
with open(path(), "w/r/a/b...") as ifile:
    ...
```
to work. If you want to check whether path is actually a TransparentPath and nothing else, use 

```python
from transparentpath import TransparentPath as Path
path = Path()
type(path) == Path  # returns True
```
instead.

Note that your script must be able to log to GCS somehow. As mentionned before, you can use a service account json 
file by setting the env var 
`GOOGLE_APPLICATION_CREDENTIALS=path_to_project_cred.json`
in your .bashrc. You can also do it from within your python code with `os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
=path_to_project_cred.json`. The last method is:

```python
from transparentpath import TransparentPath as Path
Path.set_global_fs("gcs", bucket="bucket", token="path_to_project_cred.json")
# AND/OR
path = Path("gs://bucket/file", token="path_to_project_cred.json")
```

If your code is running on a VM or pod on GCP, you do not need to provide any credentials.

Since the bucket name is provided in set_global_fs, you **must not** specify it in your paths unless you also 
include "gs://" in front of it. You should never create a path with a directory with the same name as your current 
bucket.

If your directories architecture on GCS is the same than localy up to some root directory, you can do:

```python
from transparentpath import TransparentPath as Path
Path.nas_dir = "/media/SERVEUR" # Example root path that differs between local and GCS architecture
Path.set_global_fs("gcs", bucket="my_bucket")
p = Path("/media/SERVEUR") / "chien" / "chat"  # Will be gs://my_bucket/chien/chat
```

If the line *Path.set_global_fs(...* is not commented out, the resulting path will be *gs://my_bucket/chien/chat*.
If the line *Path.set_global_fs(...* is commented out, the resulting path will be */media/SERVEUR/chien/chat*.

This allows you to create codes that can run identically both localy and on gcs, the only difference being
the line 'Path.set_global_fs(...'.

Any method or attribute valid in fsspec.implementations.local.LocalFileSystem, gcs.GCSFileSystem or pathlib.Path
can be used on a TransparentPath object.

## Warnings

### Warnings about GCS behaviour
if you use GCS:

  1. Remember that directories are not a thing on GCS.

  2. The is_dir() method exists but, on GCS, only makes sense if tested on a part of an existing path,
  i.e not on a leaf.

  3. You do not need the parent directories of a file to create the file : they will be created if they do not
  exist (that is not true localy however).

  4. If you delete a file that was alone in its parent directories, those directories disapear.

  5. Since most of the times we use is_dir() we want to check whether a directry exists to write in it,
  by default the is_dir() method will return True if the directory does not exists on GCS (see point 3)(will
  still return false if using a local file system). The only case is_dir() will return False is if a file with
  the same name exists (localy, behavior is straightforward). To actually check whether the directory exists (
  for, like, reading from it), add the kwarg 'exist=True' to is_dir() if using GCS.

  6. If a file exists at the same path than a directory, then the class is not able to know which one is the
  file and which one is the directory, and will raise a TPMultipleExistenceError upon object creation. Will also
  check for multiplicity at almost every method in case an exterior source created a duplicate of the
  file/directory. This case can't happen locally. However, it can happen on remote if the cache is not updated
  frequently. Donig this check can significantly increase computation time (if using glob on a directory
  containing a lot of files for example). You can deactivate it either globally (TransparentPath._do_check =
  False and TransparentPath._do_update_cache = False), for a specific path (pass nockeck=True at path
  creation), or for glob and ls by passing fast=True as additional argument.


### Speed

TransparentPath on GCS is slow because of the verification for multiple existance and the cache updating.
However one can tweak those a bit. As mentionned earlier, cache updating and multiple existence check can be
deactivated for all paths by doing

```python
from transparentpath import TransparentPath
TransparentPath._do_update_cache = False
TransparentPath._do_check = False
```

They can also be deactivated for one path only by doing

```python
p = TransparentPath("somepath", nocheck=True, notupdatecache=True)
```

It is also possible to specify when to do those check : at path creation, path usage (read, write, exists...) or 
both. Here to it can be set on all paths or only some : 

```python
TransparentPath._when_checked = {"created": True, "used": False}  # Default value
TransparentPath._when_updated = {"created": True, "used": False}  # Default value
p = TransparentPath("somepath", when_checked={"created": False, "used": False},
                    notupdatecache={"created": False, "used": False})
```

There is also an expiration time in seconds for check and update : the operation is not done if it was done not a
long time ago. Those expiration times are of 1 second by default and can be changed through :

```python
TransparentPath._check_expire = 10
TransparentPath._update_expire = 10
p = TransparentPath("somepath", check_expire=0, update_expire=0)
```

glob() and ls() have their own way to be accelerated : 

```python
p.glob("/*", fast=True)
p.ls("", fast=True)
```

Basically, fast=True means do not check and do not update the cache for all the items found by the method.

All paths created from another path will share its parent's attributes : 
 * fs_kind
 * bucket
 * notupdatecache
 * nocheck
 * when_checked
 * when_updated
 * update_expire
 * check_expire
 * token


### os.open

os.open is overloaded by TransparentPath to support giving a TransparentPath to it. If a method in a package you did 
not create uses the os.open() in a *with* statement, everything should work out of the box with a TransparentPath. 

However, if it uses the **output** of os.open, you will have to create a class to 
override this method and anything using its ouput. Indeed, os.open returns a file descriptor, not an IO, and I did 
not find a way to access file descriptors on gcs. For example, in the FileLock package, the acquire() method calls the
_acquire() method which calls os.open(), so I had to do that:

```python
from filelock import FileLock
from transparentpath import TransparentPath as Path

class MyFileLock(FileLock):
    def _acquire(self):
        tmp_lock_file = self._lock_file
        if not type(tmp_lock_file) == Path:
            tmp_lock_file = Path(tmp_lock_file)
        try:
            fd = tmp_lock_file.open("x")
        except (IOError, OSError, FileExistsError):
            pass
        else:
            self._lock_file_fd = fd
        return None
```

The original method was:

```python
import os
...
def _acquire(self):
    open_mode = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_TRUNC
    try:
        fd = os.open(self._lock_file, open_mode)
    except (IOError, OSError):
        pass
    else:
        self._lock_file_fd = fd
    return None
...
```

I tried to implement a working version of any method valid in pathlib.Path or in file systems, but futur changes
in any of those will not be taken into account quickly.
