---
permalink: /docs/index.html
---

**The complete documentation is available at https://advestis.github.io/transparentpath/**

# TransparentPath

A class that allows one to use a path in a local file system or a gcs file system (more or less) in almost the
same way one would use a pathlib.Path object.

## Requirements

You will need a working Google Cloud SDK. See https://cloud.google.com/sdk/install for more information.
 
 Basically
, you need to be able to run commands like that in your terminal :
 
    gsutil ls "gs://my_bucket"
 
 If your python code is launched in a google cloud instance (VM, pods, etc...) you should not have anything to do. 
 
## Installation

You can install this package with pip :

    pip install transparentpath

Or use it in a Dockerfile:

    FROM advestis/transparentpath
    ...

## Usage

Set TransparentPath to point to GCS:
```python
from transparentpath import TransparentPath as Path
Path.set_global_fs("gcs", bucket="bucket_name", project="project_name")
mypath = Path("foo") / "bar"  # Will use GCS
local_path = Path("chien", fs="local")  # will NOT use GCS 
other_path = mypath / "stuff"  # Will use GCS
```

or

```python
from transparentpath import TransparentPath as Path
mypath = Path("foo", fs='gcs', bucket="my_bucket_name", project="my_project")
# global file system is now GCS
local_path = Path("chien", fs="local")  # will NOT use GCS 
other_path = Path("foo2")  # will use GCS
```

or

```python
# noinspection PyShadowingNames
from transparentpath import TransparentPath as Path
mypath = Path("gs://my_bucket_name/foo", project="my_project")
# will use GCS
other_path = Path("foo2")
```

Set TransparentPath to point to your local machine:
```python
from transparentpath import TransparentPath as Path
# Nothing to do:)
mypath = Path("foo") / "bar"
# global file system is now local
remote_path = Path("foo", fs='gcs', bucket="my_bucket_name", project="my_project")
other_path = mypath / "stuff"  # will use local
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
mypath = Path("foo") / "bar.hdf5"  # could be .h5 too
with mypath.read() as ifile:
    arr = np.array(ifile["store1"])

# Doing '..' from 'foo/bar/hdf5' will return 'foo'
# Then doing 'foo' + 'babar.hdf5' will return 'foo/babar.hdf5' ('+' and '/' are synonymes)
with (mypath.cd("..")  + "babar.hdf5").write(None) as ofile:
    # Note here that we must explicitely give 'None' to the 'write' method in order for it
    # to return the open HDF5 file. We could also give a dict of {arr: "store1"} to directly
    # write the file.
    ofile["store1"] = arr


# Reading a text file. Can also use 'w', 'a', etc... also works with binaries.
mypath = Path("foo") / "bar.txt"
with open(mypath, "r") as ifile:
    lines = ifile.readlines()

mypath.is_file()
mypath.is_dir()  # Specific behavior on GCS. See 'Behavior' below.
mypath.is_file()
files = mypath.parent.glob("/*.csv")  # Returns a Iterator[TransparentPath], can be casted to list
```

### Dask

TransparentPath supports writing and reading Dask dataframes from and to csv, excel, parquet and HDF5, both locally and
remotely. Writing Dask dataframes does not require any additionnal arguments to be passed for the type will be checked
before calling the appropriate writting method. Reading however requires you to pass the *use_Dask* argument to the
`read()` method. If the file to read is HDF5, you will also need to specify *set_names*, mathcing the argument *key*
of Dask's `read_hdf()` method.

Note that if reading a remote HDF5, the file will be downloaded in /tmp, then read. If not using Dask, the file is
deleted after being read. but since Dask uses delayed processes, deleting the file might occure before the file is
actually read, so the file is kept. Up to you to empty your /tmp directory if it is not done automatically by your
system.


Do not hesitate to read the documentation in **docs/** for more details.


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
type(path) == Path
```

instead.

Note that your script must be able to log to GCS somehow. I generally use a service account with credentials
stored in a json file, and add the envirronement variable 'GOOGLE_APPLICATION_CREDENTIALS=path_to_project_cred.json'
in my .bashrc. I haven't tested any other method, but I guess that as long as gsutil works, TransparentPath will
too.

Since the bucket name is provided in set_fs or set_global_fs, you **must not** specify it in your paths.
Do not specify 'gs://' either, it is added when/if needed. Also, you should never create a directory with the same
name as your current bucket.

If your directories architecture on GCS is the same than localy up to some root directory, you can do:
```python
from transparentpath import TransparentPath as Path
Path.nas_dir = "/media/SERVEUR" # Example root path that differs between local and GCS architecture
Path.set_global_fs("gcs", bucket="my_bucket", project="my_project")
p = Path("/media/SERVEUR") / "chien" / "chat"  # Will be gs://my_bucket/chien/chat
```
If the line *Path.set_global_fs(...* is not commented out, the resulting path will be *gs://my_bucket/chien/chat*.
If the line *Path.set_global_fs(...* is commented out, the resulting path will be */media/SERVEUR/chien/chat*.

This allows you to create codes that can run identically both localy and on gcs, the only difference being
the line 'Path.set_global_fs(...'.

Any method or attribute valid in fsspec.implementations.local.LocalFileSystem, gcs.GCSFileSystem or pathlib.Path
can be used on a TransparentPath object. However, setting an attribute is not transparent : if, for
example, you want to change the path's name, you need to do

```python
from transparentpath import TransparentPath as Path
mypath = Path("chien.txt")
mypath.__path.name = "new_name.txt"  # instead of p.name = "new_name.txt"
```

*p.path* points to the underlying pathlib.Path object.

### Warnings about GCS behaviour
if you use GCS:

1. Remember that directories are not a thing on GCS.
2. The is_dir() method exists but, on GCS, only makes sense if tested on a part of an existing path, i.e not on a leaf.

3. You do not need the parent directories of a file to create the file : they will be created if they do not
exist (that is not true localy however).

4. If you delete a file that was alone in its parent directories, those directories disapear.

5. Since most of the times we use is_dir() we want to check whether a directry exists to write in it,
by default the is_dir() method will return True if the directory does not exists on GCS (see point 3)(will
still return false if using a local file system).

6. The only case is_dir() will return False is if a file with the same name exists (localy, behavior is
straightforward).

7. To actually check whether the directory exists (for, like, reading from it), add the kwarg 'exist=True' to
is_dir() if using GCS.

8. If a file exists with the same path than a directory, then the class is not able to know which one is the
file and which one is the directory, and will raise a MultipleExistenceError at object creation. Will also
check for multiplicity at almost every method in case an exterior source created a duplicate of the
file/directory.

If a method in a package you did not create uses the os.open(), you will have to create a class to override this
method and anything using its ouput. Indeed os.open returns a file descriptor, not an IO, and I did not find a
way to access file descriptors on gcs. For example, in the FileLock package, the acquire() method calls the
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