# transparentpath.gcsutils package

## Submodules

## transparentpath.gcsutils.methodtranslator module


### class transparentpath.gcsutils.methodtranslator.MethodTranslator(first_name: str, second_name: str, kwarg_names: Dict[str, str] = None)
Bases: `object`


#### translate(\*args: Tuple, \*\*kwargs: Dict)
translate the method


* **Parameters**

    
    * **\*args** (*Tuple*) – 


    * **\*\*kwargs** (*Dict*) – 



* **Returns**

    The translated method name along with the given args and the
    translated kwargs



* **Return type**

    [str, Tuple, Dict]



#### translate_str(\*args: Tuple, \*\*kwargs: Dict)
Tranlate the method as a string


* **Parameters**

    
    * **\*args** (*Tuple*) – 


    * **\*\*kwargs** (*Dict*) – 



* **Returns**

    The string of the translated method
    new_method(arg1, arg2…, kwargs1=val1, translated_kwargs2=val2…)



* **Return type**

    str



### class transparentpath.gcsutils.methodtranslator.MultiMethodTranslator(first_name: str, cases: List[str], second_names: List[str], kwargs_names: [typing.List[typing.Dict[str, str]]] = None)
Bases: `object`


#### translate(case: str, \*args: Tuple, \*\*kwargs: Dict)
translate the method according to a case


* **Parameters**

    
    * **case** (*str*) – The name of the translation case to use


    * **\*args** (*Tuple*) – 


    * **\*\*kwargs** (*Dict*) – 



* **Returns**

    The translated method name along with the given args and the
    translated kwargs



* **Return type**

    [str, Tuple, Dict]



#### translate_str(case: str, \*args: Tuple, \*\*kwargs: Dict)
Tranlate the method as a string according to a case


* **Parameters**

    
    * **case** (*str*) – The name of the translation case to use


    * **\*args** (*Tuple*) – 


    * **\*\*kwargs** (*Dict*) – 



* **Returns**

    The string of the translated method
    new_method(arg1, arg2…, kwargs1=val1, translated_kwargs2=val2…)



* **Return type**

    str


## transparentpath.gcsutils.transparentpath module


### exception transparentpath.gcsutils.transparentpath.MultipleExistenceError(path, ls)
Bases: `Exception`

Exception raised when a path’s destination already contain more than
one element.


### class transparentpath.gcsutils.transparentpath.MyHDFFile(\*args, remote: Optional[transparentpath.gcsutils.transparentpath.TransparentPath] = None, \*\*kwargs)
Bases: `h5py._hl.files.File`


### class transparentpath.gcsutils.transparentpath.MyHDFStore(\*args, remote: Optional[transparentpath.gcsutils.transparentpath.TransparentPath] = None, \*\*kwargs)
Bases: `pandas.io.pytables.HDFStore`


### class transparentpath.gcsutils.transparentpath.Myzipfile(path, \*args, \*\*kwargs)
Bases: `zipfile.ZipFile`

Overload of ZipFile class to handle files on remote file system


### class transparentpath.gcsutils.transparentpath.TransparentPath(path: Union[pathlib.Path, transparentpath.gcsutils.transparentpath.TransparentPath, str] = '.', nocheck: bool = False, collapse: bool = True, fs: Optional[str] = None, bucket: Optional[str] = None, project: Optional[str] = None, \*\*kwargs)
Bases: `os.PathLike`

Class that allows one to use a path in a local file system or a gcs file
system (more or less) the same way one would use a pathlib.Path object.
All instences of TransparentPaths are absolute, even if created with
relative paths.

Doing ‘isinstance(path, pathlib.Path)’ or ‘isinstance(path, str)’ with
a TransparentPath will return True. If you want to check whether path
is actually a TransparentPath and nothing else, use ‘type(path) ==
TransparentPath’ instead.

If using a local file system, you do not have to set anything,
just instantiate your paths like that:

```python
>>> # noinspection PyShadowingNames
>>> from transparentpath import TransparentPath as Path
>>> mypath = path("foo")
>>> other_path = mypath / "bar"
```

If using GCS, you will have to provide a bucket, and a project name. You
can either use the class ‘set_global_fs’ method, or specify the
appropriate keywords when calling your first path. Then all the other
paths will use the same file system.

```python
>>> # noinspection PyShadowingNames
>>> from transparentpath import TransparentPath as Path
>>> # EITHER DO
>>> Path.set_global_fs('gcs', bucket="my_bucket_name", project="my_project")
>>> mypath = Path("foo")  # will use GCS
>>> # OR
>>> mypath = Path("foo", fs='gcs', bucket="my_bucket_name", project="my_project")
>>> other_path = Path("foo2")  # will use same file system as mypath
```

Note that your script must be able to log to GCP somehow. I use the
envirronement variable
‘GOOGLE_APPLICATION_CREDENTIALS=path_to_project_cred.json’ declared in
my .bashrc.

Since the bucket name is provided in set_fs, you do not need to
specify it in your paths. Do not specify ‘gs://’ either, it is added
when/if needed. Also, you should never create a directory with the same
name as your current bucket.

Any method or attribute valid in
fsspec.implementations.local.LocalFileSystem,
gcs.GCSFileSystem or pathlib.Path can be used on a TransparentPath
object. However, setting an attribute is not transparent : if for
example you want to change the path’s name, you need to do

```python
>>> p.path.name = "new_name"
```

instead of ‘p.name = “new_name”’.

TransparentPath has built in read and write methods that recognize the
file’s suffix to call the appropriate method (csv, parquet,
hdf5 or open). It has a built-in override of open, which allows you to
pass a TransparentPath to python’s open method.

WARNINGS if you use GCP:

    1: Remember that directories are not a thing on GCP.

    2: The is_dir() method exists but only makes sense if tested on
    a part of an existing path, i.e not on a leaf.

    3: You do not need the parent directories of a file to create
    the file

    4: If you delete a file that was alone in its parent directories,
    those directories disapear.

    5: Since most of the times we use is_dir() we want to check
    whether a directry exists to write in it, by default the is_dir(
    ) method will return True if the directory does not exists on
    GCP (see point 3).

    6: The only case is_dir() will return False is if a file with
    the same name exists (on local, behavior is straightforward).

    7: To actually check whether the directory exists, add the kwarg
    ‘exist=True’ to is_dir() if using GCP.

    8: If a file exists with the same path than a directory,
    then the class is not able to know which one is the file and
    which one is the directory, and will raise a
    MultipleExistenceError at object creation. Will also check for
    multiplicity at almost every method in case an exterior source
    created a duplicate of the file/directory.

If a method in a package you did not create uses the os.open(),
you will have to create a class to override this method and anything
using its ouput. Indeed os.open returns a file descriptor, not an IO,
and I did not find a way to access file descriptors on gcs.
For example, in the FileLock package, the acquire() method
calls the _acquire() method which calls os.open(), so I had to do that:

```python
>>> class MyFileLock(FileLock):
>>>     def _acquire(self):
>>>         tmp_lock_file = self._lock_file
>>>         if not type(tmp_lock_file) == Path:
>>>             tmp_lock_file = Path(tmp_lock_file)
>>>         try:
>>>             fd = tmp_lock_file.open("x")
>>>         except (IOError, OSError, FileExistsError):
>>>             pass
>>>         else:
>>>             self._lock_file_fd = fd
>>>         return None
```

The original method was:

```python
>>> def _acquire(self):
>>>     open_mode = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_TRUNC
>>>     try:
>>>         fd = os.open(self._lock_file, open_mode)
>>>     except (IOError, OSError):
>>>         pass
>>>     else:
>>>         self._lock_file_fd = fd
>>>     return None
```

I tried to implement a working version of any method valid in
pathlib.Path or in file systems, but futur changes in any of those will
not be taken into account quickly.


#### append(other: str)

#### bucket( = None)

#### cast_fast(path: str)

#### cast_slow(path: str)

#### cd(path: Optional[str] = None)
cd-like command
Will collapse double-dots (‘..’), so not compatible with symlinks.
If path is absolute (starts with ‘/’ or bucket name or is empty),
will return a path starting from root directory if FileSystem is
local, from bucket if it is GCS.
If passing None or “” , will have the same effect than “/” on
GCS, will return the current working directory on local.
If passing “.”, will return a path at the location of self.
Will raise an error if trying to access a path before root or bucket.


* **Parameters**

    **path** (*str*) – The path to cd to. Absolute, or relative to self.
    (Default value = None)



* **Returns**

    **newpath**



* **Return type**

    the absolute TransparentPath we cded to.



#### check_multiplicity()
Checks if several objects correspond to the path.
Raises MultipleExistenceError if so, does nothing if not.


#### cwd( = '/home/pcotte/Documents/git/transparentpath')

#### do_nothing()
does nothing (you don’t say)


#### exist()
To prevent typo of ‘exist()’ without an -s


#### exists()

#### fs_kind( = '')

#### fss( = {})

#### get(loc: Union[str, pathlib.Path, transparentpath.gcsutils.transparentpath.TransparentPath])
used to get a remote file to local.
self must be a gcs TransparentPath. If loc is a TransparentPath,
it must be local. If it is a pathlib.Path or a str, it will be
casted into a local TransparentPath.


#### get_absolute()
Returns self, since all TransparentPaths are absolute


* **Returns**

    self



* **Return type**

    TransparentPath



#### glob(wildcard: str = '/\*', fast: bool = False)
Returns a list of TransparentPath matching the wildcard pattern

By default, the wildcard is ‘/

```
*
```

’. The ‘/’ is important if your path is a dir and you
want to glob inside the dir.


* **Parameters**

    
    * **wildcard** (*str*) – The wilcard pattern to match, relative to self (Default value =
    “\*”)


    * **fast** (*bool*) – If True, does not check multiplicity when converting output
    paths to TransparentPath, significantly speeding up the process
    (Default value = False)



* **Returns**

    The list of items matching the pattern



* **Return type**

    Iterator[TransparentPath]



#### is_dir(exist: bool = False)
Check if self is a directory
On GCP, leaves are never directories even if created with mkdir.
But since a non-existing directory does not prevent from writing in
it, return False only if the path exists and is not a directory on
GCP. If it does not exist, returns True.


* **Parameters**

    **exist** (*bool*) – If not specified and if using GCS, is_dir() returns True if the
    directory does not exist and no file with the same path exist.
    Otherwise, only returns True if the directory really exists.



* **Returns**

    


* **Return type**

    bool



#### is_file()
Check if self is a file
On GCP, leaves are always files even if created with mkdir.


* **Returns**

    


* **Return type**

    bool



#### ls(path: str = '', fast: bool = False)
ls-like method. Returns an Iterator of absolute TransparentPaths.


* **Parameters**

    
    * **path** (*str*) – relative path to ls. (Default value = “”)


    * **fast** (*bool*) – If True, does not check multiplicity when converting output
    paths to TransparentPath, significantly speeding up the process
    (Default value = False)



* **Returns**

    


* **Return type**

    Iterator[TransparentPath]



#### method_path_concat( = [])

#### method_without_self_path( = ['end_transaction', 'get_mapper', 'read_block', 'start_transaction', 'connect', 'load_tokens'])

#### mkbucket(name: Optional[str] = None)

#### mkdir(present: str = 'ignore', \*\*kwargs)
Creates the directory corresponding to self if does not exist
Remember that leaves are always files on GCP, so can not create a
directory on GCP. Thus, the function will have no effect on GCP.


* **Parameters**

    
    * **present** (*str*) – What to do if there is already something at self. Can be “raise”
    or “ignore” (Default value = “ignore”)


    * **kwargs** – The kwargs to pass to file system’s mkdir method



* **Returns**

    


* **Return type**

    None



#### mv(other: Union[str, pathlib.Path, transparentpath.gcsutils.transparentpath.TransparentPath])
Used to move two files on the same file system.


#### nas_dir( = '/media/SERVEUR')

#### open(\*arg, \*\*kwargs)
Uses the file system open method


* **Parameters**

    
    * **arg** – Any args valid for the builtin open() method


    * **kwargs** – Any kwargs valid for the builtin open() method



* **Returns**

    The IO buffer object



* **Return type**

    IO



#### project( = None)

#### put(dst: Union[str, pathlib.Path, transparentpath.gcsutils.transparentpath.TransparentPath])
used to push a local file to the cloud.
self must be a local TransparentPath. If dst is a TransparentPath,
it must be on GCS. If it is a pathlib.Path or a str, it will be
casted into a GCS TransparentPath, so a gcs file system must have
been set up once before.


#### read(\*args, get_obj: bool = False, use_pandas: bool = False, update_cache: bool = True, \*\*kwargs)
Method used to read the content of the file located at self
Will raise FileNotFound error if there is no file. Calls a specific
method to read self based on the suffix of self.path:

> 1: .csv : will use pandas’s read_csv

> 2: .parquet : will use pandas’s read_parquet with pyarrow engine

> 3: .hdf5 or .h5 : will use h5py.File. Since it does not support
> remote file systems, the file will be downloaded locally in a tmp
> filen read, then removed.

> 4: .json : will use open() method to get file content then json.loads to get a dict

> 5: .xlsx : will use pd.read_excel

> 6: any other suffix : will return a IO buffer to read from, or the
> string contained in the file if get_obj is False.


* **Parameters**

    
    * **get_obj** (*bool*) – only relevant for files that are not csv, parquet nor HDF5. If True
    returns the IO Buffer, else the string contained in the IO Buffer (
    Default value = False)


    * **use_pandas** (*bool*) – Must pass it as True if hdf file was written using HDFStore and not h5py.File (Default value = False)


    * **update_cache** (*bool*) – FileSystem objects do not necessarily follow changes on the
    system if they were not perfermed by them directly. If
    update_cache is True, the FileSystem will update its cache
    before trying to read anything. If False, it won’t, potentially
    saving some time but this might result in a FileNotFoundError.
    (Default value = True)


    * **args** – any args to pass to the underlying reading method


    * **kwargs** – any kwargs to pass to the underlying reading method



* **Returns**

    


* **Return type**

    Any



#### read_csv(update_cache: bool = True, \*\*kwargs)

#### read_excel(update_cache: bool = True, \*\*kwargs)

#### read_hdf5(update_cache: bool = True, use_pandas: bool = False, \*\*kwargs)
Reads a HDF5 file. Must have been created by h5py.File
Since h5py.File does not support GCS, first copy it in a tmp file.


* **Parameters**

    
    * **update_cache** (*bool*) – FileSystem objects do not necessarily follow changes on the
    system if they were not perfermed by them directly. If
    update_cache is True, the FileSystem will update its cache
    before trying to read anything. If False, it won’t, potentially
    saving some time but this might result in a FileNotFoundError.
    (Default value = True)


    * **use_pandas** (*bool*) – To use HDFStore instead of h5py.File (Default value = False)


    * **kwargs** – The kwargs to pass to h5py.File method



* **Returns**

    


* **Return type**

    Opened h5py.File



#### read_parquet(update_cache: bool = True, \*\*kwargs)

#### read_text(\*args, get_obj: bool = False, update_cache: bool = True, \*\*kwargs)

#### rm(absent: str = 'raise', ignore_kind: bool = False, \*\*kwargs)
Removes the object pointed to by self if exists.
Remember that leaves are always files on GCP, so rm will remove
the path if it is a leaf on GCP


* **Parameters**

    
    * **absent** (*str*) – What to do if trying to remove an item that does not exist. Can
    be ‘raise’ or ‘ignore’ (Default value = ‘raise’)


    * **ignore_kind** (*bool*) – If True, will remove anything pointed by self. If False,
    will raise an error if self points to a file and ‘recursive’ was
    specified in kwargs, or if self point to a dir and ‘recursive’
    was not specified (Default value = False)


    * **kwargs** – The kwargs to pass to file system’s rm method



* **Returns**

    


* **Return type**

    None



#### rmbucket(name: Optional[str] = None)

#### rmdir(absent: str = 'raise', ignore_kind: bool = False)
Removes the directory corresponding to self if exists
Remember that leaves are always files on GCP, so rmdir will never
remove a leaf on GCP


* **Parameters**

    
    * **absent** (*str*) – What to do if trying to remove an item that does not exist. Can
    be ‘raise’ or ‘ignore’ (Default value = ‘raise’)


    * **ignore_kind** (*bool*) – If True, will remove anything pointed by self. If False,
    will raise an error if self points to a file and ‘recursive’ was
    specified in kwargs, or if self point to a dir and ‘recursive’
    was not specified (Default value = False)



#### set_fs(fs: str, bucket: Optional[str] = None, project: Optional[str] = None, nas_dir: Optional[Union[transparentpath.gcsutils.transparentpath.TransparentPath, pathlib.Path, str]] = None)
Can be called to set the file system, if ‘fs’ keyword was not
given at object creation.
If not called, default file system is that of TransparentPath. If
TransparentPath has no file system yet, creates a local one by
default. If the first parameter is False, the file system is local,
and the other two parameters are not needed. If the first parameter
is True, file system is GCS and the other two parameters are needed.


* **Parameters**

    
    * **fs** (*str*) – ‘gcs’ will use GCSFileSystem, ‘local’ will use LocalFileSystem


    * **bucket** (*str*) – The bucket name if using gcs (Default value =  None)


    * **project** (*str*) – The project name if using gcs (Default value = None)


    * **nas_dir** (*Union**[**TransparentPath**, **Path**, **str**]*) – If specified, TransparentPath will delete any occurence of
    ‘nas_dir’ at the beginning of created paths if fs is gcs.



* **Returns**

    


* **Return type**

    None



#### classmethod set_global_fs(fs: str, bucket: Optional[str] = None, project: Optional[str] = None, make_main: bool = True, nas_dir: Optional[Union[transparentpath.gcsutils.transparentpath.TransparentPath, pathlib.Path, str]] = None)
To call before creating any instance to set the file system.

If not called, default file system is local. If the first parameter
is False, the file system is local, and the other two parameters are
not needed. If the first parameter is True, file system is GCS and
the other two parameters are needed.


* **Parameters**

    
    * **fs** (*str*) – ‘gcs’ will use GCSFileSystem, ‘local’ will use LocalFileSystem


    * **bucket** (*str*) – The bucket name if using gcs (Default value =  None)


    * **project** (*str*) – The project name if using gcs (Default value = None)


    * **make_main** (*bool*) – If True, any instance created after this call to set_global_fs
    will be fs. If False, just add the new file system to cls.fss,
    but do not use it as default file system. (Default value = True)


    * **nas_dir** (*Union**[**TransparentPath**, **Path**, **str**]*) – If specified, TransparentPath will delete any occurence of
    ‘nas_dir’ at the beginning of created paths if fs is gcs.



* **Returns**

    


* **Return type**

    None



#### static set_nas_dir(obj, nas_dir)

#### stat()
Calls file system’s stat method and translates the key to
os.stat_result() keys


#### to_csv(data: Union[pandas.core.frame.DataFrame, pandas.core.series.Series], overwrite: bool = True, present: str = 'ignore', update_cache: bool = True, \*\*kwargs)

#### to_excel(data: Union[pandas.core.frame.DataFrame, pandas.core.series.Series], overwrite: bool = True, present: str = 'ignore', update_cache: bool = True, \*\*kwargs)

#### to_hdf5(data: Any = None, set_name: str = None, update_cache: bool = True, use_pandas: bool = False, \*\*kwargs)

* **Parameters**

    
    * **data** (*Any*) – The data to store. Can be None, in that case an opened file is returned (Default value = None)


    * **set_name** (*str*) – The name of the dataset (Default value = None)


    * **update_cache** (*bool*) – FileSystem objects do not necessarily follow changes on the
    system if they were not perfermed by them directly. If
    update_cache is True, the FileSystem will update its cache
    before trying to read anything. If False, it won’t, potentially
    saving some time but this might result in a FileExistError.
    (Default value = True)


    * **use_pandas** (*bool*) – To use pd.HDFStore object instead of h5py.File (Default = False)


    * **\*\*kwargs** – 



* **Returns**

    


* **Return type**

    Union[None, pd.HDFStore, h5py.File]



#### to_json(data: Any, overwrite: bool = True, present: str = 'ignore', update_cache: bool = True, \*\*kwargs)

#### to_parquet(data: Union[pandas.core.frame.DataFrame, pandas.core.series.Series], overwrite: bool = True, present: str = 'ignore', update_cache: bool = True, columns_to_string: bool = True, to_dataframe: bool = True, \*\*kwargs)

#### touch(present: str = 'ignore', create_parents: bool = True, \*\*kwargs)
Creates the file corresponding to self if does not exist.
Raises FileExistsError if there already is an object that is not a
file at self.
Default behavior is to create parent directories of the file if
needed. This can be canceled by passing ‘create_parents=False’, but
only if not using GCS, since directories are not a thing on GCS.


* **Parameters**

    
    * **present** (*str*) – What to do if there is already something at self. Can be “raise”
    or “ignore” (Default value = “ignore”)


    * **create_parents** (*bool*) – If False, raises an error if parent directories are absent.
    Else, create them. Always True on GCS. (Default value = True)


    * **kwargs** – The kwargs to pass to file system’s touch method



* **Returns**

    


* **Return type**

    None



#### transform_path(method_name: str, \*args: Tuple)
File system methods take self.path as first argument, so add its
absolute path as first argument of args. Some, like ls or
glob, are given a relative path to append to self.path, so we need to
change the first element of args from args[0] to self.path / args[0]


* **Parameters**

    
    * **method_name** (*str*) – The method name, to check whether it needs to append self.path
    or not


    * **args** (*Tuple*) – The args to pass to the method



* **Returns**

    Either the unchanged args, or args with the first element
    prepended by self, or args with a new first element (self)



* **Return type**

    Tuple



#### translations( = {'mkdir': <transparentpath.gcsutils.methodtranslator.MultiMethodTranslator object>})

#### unlink(\*\*kwargs)
Alias of rm, to match pathlib.Path method


#### unset( = True)

#### update_cache()
Calls FileSystem’s invalidate_cache() to discard the cache then
calls a non-distruptive method (fs.info(bucket)) to update it.

If local, on need to update the chache. Not even sure it needs to be
invalidated…


#### with_suffix(suffix: str)
Returns a new TransparentPath object with a changed suffix
Uses the with_suffix method of pathlib.Path


* **Parameters**

    **suffix** (*str*) – suffix to use, with the dot (‘.pdf’, ‘.py’, etc ..)



* **Returns**

    


* **Return type**

    TransparentPath



#### write(\*args, data: Any = None, set_name: str = 'data', use_pandas: bool = False, overwrite: bool = True, present: str = 'ignore', update_cache: bool = True, \*\*kwargs)
Method used to write the content of the file located at self
Calls a specific method to write data based on the suffix of
self.path:

> 1: .csv : will use pandas’s to_csv

> 2: .parquet : will use pandas’s to_parquet with pyarrow engine

> 3: .hdf5 or .h5 : will use h5py.File. Since it does not support
> remote file systems, the file will be created locally in a tmp
> filen read, then uploaded and removed locally.

> 4: .json : will use jsonencoder.JSONEncoder class. Works with DataFrames and np.ndarrays too.

> 5: .xlsx : will use pandas’s to_excel

> 5: any other suffix : uses self.open to write to an IO Buffer


* **Parameters**

    
    * **data** (*Any*) – The data to write


    * **set_name** (*str*) – Name of the dataset to write. Only relevant if using HDF5 (
    Default value = ‘data’)


    * **use_pandas** (*bool*) – Must pass it as True if hdf file must be written using HDFStore and not h5py.File


    * **overwrite** (*bool*) – If True, any existing file will be overwritten. Only relevant
    for csv, hdf5 and parquet files, since others use the ‘open’
    method, which args already specify what to do (Default value =
    True).


    * **present** (*str*) – Indicates what to do if overwrite is False and file is present.
    Here too, only relevant for csv, hsf5 and parquet files.


    * **update_cache** (*bool*) – FileSystem objects do not necessarily follow changes on the
    system if they were not perfermed by them directly. If
    update_cache is True, the FileSystem will update its cache
    before trying to read anything. If False, it won’t, potentially
    saving some time but this might result in a FileExistError.
    (Default value = True)


    * **args** – any args to pass to the underlying writting method


    * **kwargs** – any kwargs to pass to the underlying reading method



* **Returns**

    


* **Return type**

    Union[None, pd.HDFStore, h5py.File]



#### write_bytes(data: Any, \*args, overwrite: bool = True, present: str = 'ignore', update_cache: bool = True, \*\*kwargs)

#### write_stuff(data: Any, \*args, overwrite: bool = True, present: str = 'ignore', update_cache: bool = True, \*\*kwargs)

### transparentpath.gcsutils.transparentpath.collapse_ddots(path: Union[pathlib.Path, transparentpath.gcsutils.transparentpath.TransparentPath, str])
Collapses the double-dots (..) in the path


* **Parameters**

    **path** (*Union**[**Path**, **TransparentPath**, **str**]*) – The path containing double-dots



* **Returns**

    The collapsed path. Same type as input.



* **Return type**

    Union[Path, TransparentPath]



### transparentpath.gcsutils.transparentpath.get_fs(gcs: str, project: str, bucket: str)
Gets the FileSystem object of either gcs or local (Default)


* **Parameters**

    
    * **gcs** (*str*) – Returns GCSFileSystem if ‘gcs’’, LocalFilsSystem if ‘local’.


    * **project** (*str*) – project name for GCS


    * **bucket** (*str*) – bucket name for GCS



* **Returns**

    The FileSystem object and the string ‘gcs’ or ‘local’



* **Return type**

    Union[gcsfs.GCSFileSystem, LocalFileSystem]



### transparentpath.gcsutils.transparentpath.myisinstance(obj1, obj2)
Will return True when testing whether a TransparentPath is a str
and False when testing whether a pathlib.Path is a Transparent.


### transparentpath.gcsutils.transparentpath.myopen(\*args, \*\*kwargs)
## Module contents
