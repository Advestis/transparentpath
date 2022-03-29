[![doc](https://img.shields.io/badge/-Documentation-blue)](https://advestis.github.io/transparentpath)
[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

#### Status
![Pytests](https://github.com/Advestis/transparentpath/actions/workflows/pull-request.yml/badge.svg)
![push](https://github.com/Advestis/transparentpath/actions/workflows/push.yml/badge.svg)

![maintained](https://img.shields.io/badge/Maintained%3F-yes-green.svg)
![issues](https://img.shields.io/github/issues/Advestis/transparentpath.svg)
![pr](https://img.shields.io/github/issues-pr/Advestis/transparentpath.svg)


#### Compatibilities
![ubuntu](https://img.shields.io/badge/Ubuntu-supported--tested-success)
![unix](https://img.shields.io/badge/Other%20Unix-supported--untested-yellow)
![Windows](https://img.shields.io/badge/Windows-basic--untested-important)

![python](https://img.shields.io/pypi/pyversions/transparentpath)

This package is not maintained for python 3.6 anymore. The latest version available for python 3.6 is 0.1.129.
Please use python >= 3.7.


##### Contact
[![linkedin](https://img.shields.io/badge/LinkedIn-Advestis-blue)](https://www.linkedin.com/company/advestis/)
[![website](https://img.shields.io/badge/website-Advestis.com-blue)](https://www.advestis.com/)
[![mail](https://img.shields.io/badge/mail-maintainers-blue)](mailto:pythondev@advestis.com)


# TransparentPath

A class that allows one to use a path in a local file system or a Google Cloud Storage (GCS) file system in the
same way one would use a *pathlib.Path* object. One can use many different GCP projects at once.

## Requirements

You will need credentials, aither as a *.json* file, that you can set in the envvar GOOGLE_APPLICATION_CREDENTIALS, or
by running directly in a google cloud instance (VM, pods, etc...). 
 
## Installation

You can install this package with pip :

```bash
pip install transparentpath
```

## Optional packages

The vanilla version allows you to declare paths and work with them. You can use them in the builtin `open` method. 
Optionally, you can also install support for several other packages like pandas, dask, etc... the currently 
available optionnal packages are accessible through the follownig commands: 

```bash
pip install transparentpath[pandas]
pip install transparentpath[parquet]
pip install transparentpath[hdf5]
pip install transparentpath[json]
pip install transparentpath[excel]
pip install transparentpath[dask]
```

you can install all of those at once

```bash
pip install transparentpath[all]
```

## Usage

Create a path that points to GCS, and one that does not:
```python
from transparentpath import Path
# Or : from transparentpath import TransparentPath
p = Path("gs://mybucket/some_path", token="some/cred/file.json")
p2 = p / "foo"  # Will point to gs://mybucket/some_path/foo
p3 = Path("bar")  # Will point to local path "bar"
```

Set all paths to point to GCS by default:
```python
from transparentpath import Path
Path.set_global_fs("gcs", token="some/cred/file.json")
p = Path("mybucket") / "some_path" # Will point to gs://mybucket/some_path
p2 = p / "foo"  # Will point to gs://mybucket/some_path/foo
p3 = Path("bar", fs="local")  # Will point to local path "bar"
p4 = Path("other_bucket")  # Will point to gs://other_bucket (assuming other_bucket is a bucket on GCS)
p5 = Path("not_a_bucket")  # Will point to local path "not_a_bucket" (assuming it is indeed, not a bucket on GCS)
```

Set all paths to point to severral GCS projects by default:
```python
from transparentpath import Path
Path.set_global_fs("gcs", token="some/cred/file.json")
Path.set_global_fs("gcs", token="some/other/cred/file.json")
p = Path("mybucket") / "some_path" # Will point to gs://mybucket/some_path
p2 = p / "foo"  # Will point to gs://mybucket/some_path/foo
p3 = Path("bar", fs="local")  # Will point to local path "bar"
p4 = Path("other_bucket")  # Will point to gs://other_bucket (assuming other_bucket is a bucket on GCS)
p5 = Path("not_a_bucket")  # Will point to local path "not_a_bucket" (assuming it is indeed, not a bucket on GCS)
```
Here, *mybucket* and *other_bucket* can be on two different projects, as long as at least one of the
credential files can access them.

Set all paths to point to GCS by default, and specify a default bucket:
```python
from transparentpath import Path
Path.set_global_fs("gcs", bucket="mybucket", token="some/cred/file.json")
p = Path("some_path")  # Will point to gs://mybucket/some_path/
p2 = p / "foo"  # Will point to gs://mybucket/some_path/foo
p3 = Path("bar", fs="local")  # Will point to local path "bar"
p4 = Path("other_bucket")  # Will point to gs://mybucket/other_bucket
p5 = Path("not_a_bucket")  # Will point to gs://mybucket/not_a_bucket
```

The latest option is interesting if you have a code that should be able to run with paths being sometimes remote, sometimes local.
To do that, you can use the class attribute `nas_dir`. Then when a path is created, if it starts by `nas_dir`'s path, 
`nas_dir`'s path is replaced by the bucket name. This is useful if, for instance, you have a backup of a bucket locally at
 let's say */my/local/backup*. Then you can do:
```python
from transparentpath import Path
Path.nas_dir = "/my/local/backup"
Path.set_global_fs("gcs", bucket="mybucket", token="some/cred/file.json")
p = Path("some_path")  # Will point to gs://mybucket/some_path/
p3 = Path("/my/local/backup") / "some_path"  # Will ALSO point to gs://mybucket/some_path/
```
```python
from transparentpath import Path
Path.nas_dir = "/my/local/backup"
# Path.set_global_fs("gcs", bucket="mybucket", token="some/cred/file.json")
p = Path("some_path")  # Will point to /my/local/backup/some_path/
p3 = Path("/my/local/backup") / "some_path"  # Will ALSO point to /my/local/backup/some_path/
```

In all the previous examples, the `token` argument can be ommited if the environment variable
**GOOGLE_APPLICATION_CREDENTIALS** is set and point to a *.json* credential file, or if your code runs on a GCP machine
(VM, cluster...) with access to GCS.

No matter whether you are using GCS or your local file system, here is a sample of what TransparentPath can do:

```python
from transparentpath import Path
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
mypath.is_dir()
mypath.is_file()
files = mypath.parent.glob("*.csv")  # Returns a Iterator[TransparentPath], can be casted to list
```

As you can see from the previous example, all methods returning a path from a TransparentPath return a 
TransparentPath.

### Dask

TransparentPath supports writing and reading Dask dataframes from and to csv, excel, parquet and HDF5, both locally and
remotely. You need to have dask-dataframe and dask-distributed installed, which will be the case if you ran `pip 
install transparentpath[dask]`. Writing Dask dataframes does not require any additionnal arguments to be passed
for the type will be checked before calling the appropriate writting method. Reading however requires you to pass 
the *use_dask* argument to the `read()` method. If the file to read is HDF5, you will also need to specify 
*set_names*, matching the argument *key* of Dask's `read_hdf()` method.

Note that if reading a remote HDF5, the file will be downloaded in your local tmp, then read. If not using Dask, the 
file is deleted after being read. But since Dask uses delayed processes, deleting the file might occure before the 
file is actually read, so the file is kept. Up to you to empty your /tmp directory if it is not done automatically 
by your system.


## Behavior

All instances of TransparentPath are absolute, even if created with relative paths.

TransparentPaths are seen as instances of str: 

```python
from transparentpath import Path
path = Path()
isinstance(path, str)  # returns True
```
 
This is required to allow
 
```python
from transparentpath import Path
path = Path()
with open(path, "w/r/a/b...") as ifile:
    ...
```
to work. If you want to check whether path is actually a TransparentPath and nothing else, use 

```python
from transparentpath import Path
path = Path()
assert type(path) == Path
assert issubclass(path, Path)
```
instead.

Any method or attribute valid in fsspec.implementations.local.LocalFileSystem, gcs.GCSFileSystem, pathlib.Path or str
can be used on a TransparentPath object.

## Warnings

### Warnings about GCS behaviour
if you use GCS:

  1. Remember that directories are not a thing on GCS.

  2. You do not need the parent directories of a file on GCS to create the file : they will be created if they do not
  exist (that is not true localy however).

  3. If you delete a file that was alone in its parent directories, those directories disapear.

  4. If a file exists at the same path than a directory, then TransparentPath is not able to know which one is the
  file and which one is the directory, and will raise a TPMultipleExistenceError upon object creation. This
  check for multiplicity is done at almost every method in case an exterior source created a duplicate of the
  file/directory. This case can't happen locally. However, it can happen on remote if the cache is not updated
  frequently. Doing this check can significantly increase computation time (if using glob on a directory
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
p = TransparentPath(
  "somepath", when_checked={"created": False, "used": False}, notupdatecache={"created": False, "used": False}
)
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

Basically, `fast=True` means "do not check and do not update the cache" for all the items found by the method.


### builtin open

Builtin `open()` is overloaded by TransparentPath to support giving a TransparentPath to it. If a method in a package you did 
not create uses `open()` in a *with* statement, everything should work out of the box with a TransparentPath. 

However, if it uses the **output** of `open()`, you will have to create a class to 
override this method and anything using its ouput. Indeed, `open()` returns a file descriptor, not an IO, and I did 
not find a way to access file descriptors on gcs. For example, in the FileLock package, the `acquire()` method calls the
`_acquire()` method which calls `os.open()`, so I had to do that:

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
in any of those will not be taken into account quickly. You can report missing supports by opening an issue.
