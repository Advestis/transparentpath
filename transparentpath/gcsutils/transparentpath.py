import builtins
import os
from datetime import datetime
from pathlib import Path
from typing import Union, Tuple, Any, Iterator, Optional, Iterable, List, Callable

from .methodtranslator import MultiMethodTranslator
import gcsfs
from fsspec.implementations.local import LocalFileSystem
from inspect import signature

remote_prefix = "gs://"


def errormessage(which) -> str:
    return (
        f"Support for {which} does not seem to be installed for TransparentPath.\n"
        f"You can change that by running 'pip install transparentpath[{which}]'."
    )


def errorfunction(which) -> Callable:
    # noinspection PyUnusedLocal
    def _errorfunction(*args, **kwargs):
        raise ImportError(errormessage(which))

    return _errorfunction


# So I can use it in myisinstance
class TransparentPath:
    def __fspath__(self) -> str:
        """Implemented later"""
        pass


builtins_isinstance = builtins.isinstance


def mysmallisinstance(obj1: Any, obj2) -> bool:
    """Will return True when testing whether a TransparentPath is a str (required to use open(TransparentPath()))
    or a TransparentPath, and False in every other cases (even pathlib.Path). """

    if type(obj1) == TransparentPath:
        if obj2 == TransparentPath or obj2 == str:
            return True
        else:
            return False

    if obj2 == TransparentPath:
        if type(obj1) == TransparentPath:
            return True
        else:
            return False

    return builtins_isinstance(obj1, obj2)


def myisinstance(obj1: Any, obj2) -> bool:
    """Will return True when testing whether a TransparentPath is a str (required to use open(TransparentPath()))
    and False when testing whether a pathlib.Path is a TransparentPath."""

    if not (builtins_isinstance(obj2, list) or builtins_isinstance(obj2, set) or builtins_isinstance(obj2, tuple)):
        return mysmallisinstance(obj1, obj2)
    else:
        is_instance = False
        for _type in obj2:
            is_instance |= mysmallisinstance(obj1, _type)
        return is_instance


setattr(builtins, "isinstance", myisinstance)


def collapse_ddots(path: Union[Path, TransparentPath, str]) -> TransparentPath:
    """Collapses the double-dots (..) in the path

    Parameters
    ----------
    path: Union[Path, TransparentPath, str]
        The path containing double-dots


    Returns
    -------
    TransparentPath
        The collapsed path.

    """
    # noinspection PyUnresolvedReferences
    thetype = path.fs_kind if type(path) == TransparentPath else None
    # noinspection PyUnresolvedReferences
    thebucket = path.bucket if type(path) == TransparentPath else None
    # noinspection PyUnresolvedReferences
    theproject = path.project if type(path) == TransparentPath else None

    newpath = Path(path) if type(path) == str else path

    if str(newpath) == ".." or str(newpath) == "/..":
        raise ValueError("Can not go before root")

    while ".." in newpath.parts:
        # noinspection PyUnresolvedReferences
        newnewpath = Path(newpath.parts[0])
        for part in newpath.parts[1:]:
            if part == "..":
                newnewpath = newnewpath.parent
            else:
                newnewpath /= part
        newpath = newnewpath

    if str(newpath) == str(path):
        return path
    return (
        TransparentPath(newpath, collapse=False, nocheck=True, fs=thetype, bucket=thebucket, project=theproject)
        if thetype is not None
        else newpath
    )


def get_fs(
    fs_kind: str, project: str, bucket: str, token: Optional[Union[str, dict]] = None
) -> Union[gcsfs.GCSFileSystem, LocalFileSystem]:
    """Gets the FileSystem object of either gcs or local (Default)


    Parameters
    ----------
    fs_kind: str
        Returns GCSFileSystem if 'gcs_*', LocalFilsSystem if 'local'.

    project: str
        project name for GCS

    bucket: str
        bucket name for GCS

    token: Optional[Union[str, dict]]
        credentials (default value = None)

    Returns
    -------
    Union[gcsfs.GCSFileSystem, LocalFileSystem]
        The FileSystem object and the string 'gcs' or 'local'

    """
    if "gcs" in fs_kind:
        bucket = bucket.replace("/", "")
        if token is None:
            fs = gcsfs.GCSFileSystem(project=project, asynchronous=False)
        else:
            print("coucou")
            fs = gcsfs.GCSFileSystem(project=project, asynchronous=False, token=token)
        # Will raise RefreshError if connection fails
        fs.glob(bucket)
        check_buckets(fs, bucket)
        return fs
    else:
        return LocalFileSystem()


def get_buckets(fs: gcsfs.GCSFileSystem) -> List[str]:
    """Return list of all buckets under the current project."""
    if "" not in fs.dircache:
        items = []
        page = fs.call("GET", "b/", project=fs.project, json_out=True)

        assert page["kind"] == "storage#buckets"
        items.extend(page.get("items", []))
        next_page_token = page.get("nextPageToken", None)

        while next_page_token is not None:
            page = fs.call("GET", "b/", project=fs.project, pageToken=next_page_token, json_out=True,)

            assert page["kind"] == "storage#buckets"
            items.extend(page.get("items", []))
            next_page_token = page.get("nextPageToken", None)
        fs.dircache[""] = [{"name": i["name"] + "/", "size": 0, "type": "directory"} for i in items]
    return [b["name"] for b in fs.dircache[""]]


def check_buckets(fs: Union[gcsfs.GCSFileSystem, LocalFileSystem], bucket: str):
    if isinstance(fs, gcsfs.GCSFileSystem):
        buckets = get_buckets(fs)
        if f"{bucket}/" not in buckets:
            raise NotADirectoryError(
                f"Bucket {bucket} does not exist in "
                f"project {fs.project}, or you can not "
                f"see it. Available buckets are:\n"
                f"{buckets}"
            )


def check_kwargs(method: Callable, kwargs: dict):
    """Takes as argument a method and some kwargs. Will look in the method signature and return in two separate dict
    the kwargs that are in the signature and those that are not.

    If the method does not return any signature or if it explicitely accepts **kwargs, does not do anything
    """
    unexpected_kwargs = []
    s = ""
    try:
        sig = signature(method)
        if "kwargs" in sig.parameters or "kwds" in sig.parameters:
            return
        for arg in kwargs:
            if arg not in sig.parameters:
                unexpected_kwargs.append(f"{arg}={kwargs[arg]}")

        if len(unexpected_kwargs) > 0:
            s = f"You provided unexpected kwargs for method {method.__name__}:"
            s = "\n  - ".join([s] + unexpected_kwargs)
    except ValueError:
        return

    if s != "":
        raise ValueError(s)


def get_index_and_date_from_kwargs(**kwargs: dict) -> Tuple[int, bool, dict]:
    index_col = kwargs.get("index_col", None)
    parse_dates = kwargs.get("parse_dates", None)
    if index_col is not None:
        del kwargs["index_col"]
    if parse_dates is not None:
        del kwargs["parse_dates"]
    # noinspection PyTypeChecker
    return index_col, parse_dates, kwargs


class MultipleExistenceError(Exception):
    """Exception raised when a path's destination already contain more than
    one element.
    """

    def __init__(self, path, ls):
        self.path = path
        self.ls = ls
        self.message = (
            f"Multiple objects exist at path {path}.\nHere is "
            f"the output of ls in the parent directory:\n"
            f" {self.ls}"
        )
        super().__init__(self.message)

    def __str__(self):
        return self.message


# noinspection PyRedeclaration
class TransparentPath(os.PathLike):  # noqa : F811
    # noinspection PyUnresolvedReferences,PyRedeclaration
    """Class that allows one to use a path in a local file system or a gcs file system (more or less) in almost the
    same way one would use a pathlib.Path object. All instances of TransparentPath are absolute, even if created with
    relative paths.

    Doing 'isinstance(path, str)' with a TransparentPath will return True (required to allow 'open(TransparentPath()'
    to work). If you want to check whether path is actually a TransparentPath and nothing else, use 'type(path) ==
    TransparentPath' instead.

    If using a local file system, you do not have to set anything,
    just instantiate your paths like that:

    >>> # noinspection PyShadowingNames
    >>> from transparentpath import TransparentPath as Path
    >>> mypath = Path("foo") / "bar"
    >>> other_path = mypath / "stuff"

    If using GCS, you will have to provide a bucket, and a project name. You can either use the class 'set_global_fs'
    method, specify the appropriate keywords when calling your first path, or giving a path that starts with
    'gs://bucketname/'. Then all the other paths will use the same file system.

    So either do

    >>> # noinspection PyShadowingNames
    >>> from transparentpath import TransparentPath as Path
    >>> Path.set_global_fs('gcs', bucket="my_bucket_name", project="my_project")  # doctest: +SKIP
    >>> # will use GCS
    >>> mypath = Path("foo")  # doctest: +SKIP
    >>> # will use GCS
    >>> other_path = Path("foo2")  # doctest: +SKIP

    Or

    >>> # noinspection PyShadowingNames
    >>> from transparentpath import TransparentPath as Path
    >>> mypath = Path("foo", fs='gcs', bucket="my_bucket_name", project="my_project")  # doctest: +SKIP
    >>> # will use GCS
    >>> other_path = Path("foo2")  # doctest: +SKIP

    Or

    >>> # noinspection PyShadowingNames
    >>> from transparentpath import TransparentPath as Path
    >>> mypath = Path("gs://my_bucket_name/foo", project="my_project")  # doctest: +SKIP
    >>> # will use GCS
    >>> other_path = Path("foo2")  # doctest: +SKIP

    However, no matter how you initialise a remote file, gs://bucketname will never figure in its name. It will not be
    stored in the underlying pathlib.Path object, and calling str(mypath) will not show it either. However, you can
    retreive the full path including gs://bucketname if any by calling

    >>> mypath.__fspath__()  # doctest: +SKIP
    gs://my_bucket_name/foo

    Note that your script must be able to log to GCS somehow. I generally use a service account with credentials
    stored in a json file, and add the envirronement variable 'GOOGLE_APPLICATION_CREDENTIALS=path_to_project_cred.json'
    in my .bashrc. I haven't tested any other method, but I guess that as long as gsutil works, TransparentPath will
    too.
    When trying to instantiate a remote path, TransparentPath will try to call the 'ls()' method of gcsfs. If it
    fails, it will raise an error.

    Since the bucket name is provided in set_fs or set_global_fs, you **must not** specify it in your paths.
    Do not specify 'gs://' either, it is added when/if needed. Also, you should never create a directory with the same
    name as your current bucket.

    If your directories architecture on GCS is the same than localy up to some root directory, you can do:

    >>> # noinspection PyShadowingNames
    >>> from transparentpath import TransparentPath as Path
    >>> Path.nas_dir = "/media/SERVEUR" # it is the default value, but reset here for example
    >>> Path.set_global_fs("gcs", bucket="my_bucket", project="my_project")  # doctest: +SKIP
    >>> p = Path("/media/SERVEUR") / "chien" / "chat"  # Will be gs://my_bucket/chien/chat  # doctest: +SKIP

    If the line 'Path.set_global_fs(...' is not commented out, the resulting path will be 'gs://my_bucket/chien/chat'.
    If the line 'Path.set_global_fs(...' is commented out, the resulting path will be '/media/SERVEUR/chien/chat'.
    This allows you to create codes that can run identically both localy and on gcs, the only difference being
    the line 'Path.set_global_fs(...'.

    Any method or attribute valid in fsspec.implementations.local.LocalFileSystem, gcs.GCSFileSystem or pathlib.Path
    can be used on a TransparentPath object.

    instead of 'p.name = "new_name"'. 'p.path' points to the underlying pathlib.Path object.

    TransparentPath has built-in read and write methods that recognize the file's suffix to call the appropriate
    method (csv, parquet, hdf5, json or open). It has a built-in override of open, which allows you to pass a
    TransparentPath to python's open method.

    WARNINGS if you use GCS:
        1: Remember that directories are not a thing on GCS.

        2: The is_dir() method exists but, on GCS, only makes sense if tested on a part of an existing path,
        i.e not on a leaf.

        3: You do not need the parent directories of a file to create the file : they will be created if they do not
        exist (that is not true localy however).

        4: If you delete a file that was alone in its parent directories, those directories disapear.

        5: Since most of the times we use is_dir() we want to check whether a directry exists to write in it,
        by default the is_dir() method will return True if the directory does not exists on GCS (see point 3)(will
        still return false if using a local file system).

        6: The only case is_dir() will return False is if a file with the same name exists (localy, behavior is
        straightforward).

        7: To actually check whether the directory exists (for, like, reading from it), add the kwarg 'exist=True' to
        is_dir() if using GCS.

        8: If a file exists with the same path than a directory, then the class is not able to know which one is the
        file and which one is the directory, and will raise a MultipleExistenceError at object creation. Will also
        check for multiplicity at almost every method in case an exterior source created a duplicate of the
        file/directory. (This case can't happen locally. However, it can happen on remote if the cache is not updated
        frequently)

    If a method in a package you did not create uses the os.open(), you will have to create a class to override this
    method and anything using its ouput. Indeed os.open returns a file descriptor, not an IO, and I did not find a
    way to access file descriptors on gcs. For example, in the FileLock package, the acquire() method calls the
    _acquire() method which calls os.open(), so I had to do that:

    >>> from filelock import FileLock
    >>> from transparentpath import TransparentPath as Tp
    >>>
    >>> class MyFileLock(FileLock):
    >>>     def _acquire(self):
    >>>         tmp_lock_file = self._lock_file
    >>>         if not type(tmp_lock_file) == Tp:
    >>>             tmp_lock_file = Tp(tmp_lock_file)
    >>>         try:
    >>>             fd = tmp_lock_file.open("x")
    >>>         except (IOError, OSError, FileExistsError):
    >>>             pass
    >>>         else:
    >>>             self._lock_file_fd = fd
    >>>         return None

    The original method was:

    >>> def _acquire(self):  # doctest: +SKIP
    >>>     open_mode = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_TRUNC  # doctest: +SKIP
    >>>     try:  # doctest: +SKIP
    >>>         fd = os.open(self._lock_file, open_mode)  # doctest: +SKIP
    >>>     except (IOError, OSError):  # doctest: +SKIP
    >>>         pass  # doctest: +SKIP
    >>>     else:  # doctest: +SKIP
    >>>         self._lock_file_fd = fd  # doctest: +SKIP
    >>>     return None  # doctest: +SKIP

    I tried to implement a working version of any method valid in pathlib.Path or in file systems, but futur changes
    in any of those will not be taken into account quickly.
    """

    @classmethod
    def reinit(cls):
        cls.fss = {}
        cls.fs_kind = ""
        cls.project = None
        cls.bucket = None
        cls.nas_dir = "/media/SERVEUR"
        cls.unset = True
        cls.cwd = os.getcwd()
        cls.token = None
        cls._do_update_cache = True
        cls._do_check = True
        cls.LOCAL_SEP = os.path.sep

    @classmethod
    def show_state(cls):
        print("fss: ", cls.fss)
        print("fs_kind: ", cls.fs_kind)
        print("project: ", cls.project)
        print("bucket: ", cls.bucket)
        print("nas_dir: ", cls.nas_dir)
        print("unset: ", cls.unset)
        print("cwd: ", cls.cwd)
        print("token: ", cls.token)
        print("_do_update_cache: ", cls._do_update_cache)
        print("_do_check: ", cls._do_check)
        print("LOCAL_SEP: ", cls.LOCAL_SEP)

    @classmethod
    def get_state(cls):
        state = {
            "fss": cls.fss,
            "fs_kind": cls.fs_kind,
            "project": cls.project,
            "bucket": cls.bucket,
            "nas_dir": cls.nas_dir,
            "unset": cls.unset,
            "cwd": cls.cwd,
            "token": cls.token,
            "_do_update_cache": cls._do_update_cache,
            "_do_check": cls._do_check,
            "LOCAL_SEP": cls.LOCAL_SEP,
        }
        return state

    fss = {}
    fs_kind = ""
    project = None
    bucket = None
    nas_dir = "/media/SERVEUR"
    unset = True
    cwd = os.getcwd()
    token = None
    _do_update_cache = True
    _do_check = True
    LOCAL_SEP = os.path.sep

    _attributes = ["fs", "path", "fs_kind", "project", "bucket"]

    method_without_self_path = [
        "end_transaction",
        "get_mapper",
        "read_block",
        "start_transaction",
        "connect",
        "load_tokens",
    ]

    method_path_concat = []

    translations = {
        "mkdir": MultiMethodTranslator(
            "mkdir", ["local", "gcs"], ["mkdir", "self._do_nothing"], [{"parents": "create_parents"}, {"parents": ""}],
        ),
    }

    @classmethod
    def set_global_fs(
        cls,
        fs: str,
        bucket: Optional[str] = None,
        project: Optional[str] = None,
        make_main: bool = True,
        nas_dir: Optional[Union[TransparentPath, Path, str]] = None,
        token: Optional[Union[dict, str]] = None,
    ) -> None:
        """To call before creating any instance to set the file system.

        If not called, default file system is local. If the first parameter is 'local', the file system is local,
        and 'bucket' and 'project' are not needed. If the first parameter is 'gcs', file system is GCS and 'bucket' and
        'project' are needed.


        Parameters
        ----------
        fs: str
            'gcs' will use GCSFileSystem, 'local' will use LocalFileSystem

        bucket: str
            The bucket name if using gcs (Default value =  None)

        project: str
            The project name if using gcs (Default value = None)

        make_main: bool
            If True, any instance created after this call to set_global_fs will be fs. If False, just add the new
            file system to cls.fss, but do not use it as default file system. (Default value = True)

        nas_dir: Union[TransparentPath, Path, str]
            If specified, TransparentPath will delete any occurence of 'nas_dir' at the beginning of created paths if fs
            is gcs (Default value = None).

        token: Optional[Union[dict, str]]
            credentials (default value = None)

        Returns
        -------
        None

        """

        if "gcs" not in fs and fs != "local":
            raise ValueError(f"Unknown value {fs} for parameter 'gcs'")

        main_fs = None
        if cls.fs_kind is not None and not make_main:
            main_fs = cls.fs_kind
        cls.fs_kind = fs
        cls.bucket = bucket

        if project is not None:
            cls.project = project

        if token is not None:
            cls.token = token

        TransparentPath._set_nas_dir(cls, nas_dir)

        if "gcs" in cls.fs_kind:
            if cls.bucket is None:
                raise ValueError("Need to provide a bucket name!")
            if cls.project is None:
                raise ValueError("Need to provide a project name!")
            cls.fs_kind = f"gcs_{cls.project}"

        cls.fss[cls.fs_kind] = get_fs(cls.fs_kind, cls.project, cls.bucket, cls.token)
        TransparentPath.unset = False

        if main_fs is not None:
            cls.fs_kind = main_fs

    def __init__(
        self,
        path: Union[Path, TransparentPath, str] = ".",
        nocheck: bool = False,
        collapse: bool = True,
        fs: Optional[str] = None,
        bucket: Optional[str] = None,
        project: Optional[str] = None,
        token: Optional[Union[dict, str]] = None,
        **kwargs,
    ):
        """Creator of the TranparentPath object

        Parameters
        ----------
        path: Union[pathlib.Path, TransparentPath, str]
            The path of the object (Default value = '.')

        nocheck: bool
            If True, will not call check_multiplicity (quicker but less secure). (Default value = False)

        collapse: bool
            If True, will collapse any double dots ('..') in path. (Default value = True)

        fs: Optional[str]
            The file system to use, 'local' or 'gcs'. If None, uses the default one set by set_global_fs if any,
            or 'local' (Default = None)

        project: Optional[str]
            The project name if using GCS. If None and using GCS, then a GCSFileSystem must have been set earlier
            using set_global_fs() (Default = None)

        bucket: Optional[str]
            The bucket name if using GCS If None and using GCS, then a GCSFileSystem must have been set earlier
            using set_global_fs() (Default = None)

        token: Optional[Union[dict, str]]
            The identification token to use. See GCSFileSystem initialisation.

        kwargs:
            Any optional kwargs valid in pathlib.Path

        """

        if path is None:
            path = "."

        if (
            not (type(path) == type(Path("dummy")))  # noqa: E721
            and not (type(path) == str)
            and not (type(path) == TransparentPath)
        ):
            raise TypeError(f"Unsupported type {type(path)} for path")

        # I never remember whether I should use fs='local' or fs_kind='local'. That way I don't need to.
        if "fs_kind" in kwargs and fs is None:
            fs = kwargs["fs_kind"]
            del kwargs["fs_kind"]

        # Copy path completely if it is a TransparentPath and we did not
        # ask for a new file system
        if type(path) == TransparentPath and fs is None:
            # noinspection PyUnresolvedReferences
            self.project = path.project
            # noinspection PyUnresolvedReferences
            self.bucket = path.bucket
            # noinspection PyUnresolvedReferences
            self.fs_kind = path.fs_kind
            # noinspection PyUnresolvedReferences
            self.fs = path.fs
            # noinspection PyUnresolvedReferences
            self.nas_dir = path.nas_dir
            # noinspection PyUnresolvedReferences
            self.__path = path.path
            # noinspection PyUnresolvedReferences
            self.sep = path.sep
            # noinspection PyUnresolvedReferences
            self.token = token
            return

        # In case we initiate a path containing 'gs://'
        if remote_prefix in str(path):

            if project is None:
                if TransparentPath.project is None:
                    raise ValueError("You specified a path starting with 'gs://' but did not specify any GCP project")
                project = TransparentPath.project

            if fs == "local":
                raise ValueError(
                    "You specified a path starting with 'gs://' but ask for it to be local. This is not possible."
                )
            fs = f"gcs_{project}"
            splitted = str(path).split(remote_prefix)
            if len(splitted) == 0:
                if bucket is None and TransparentPath.bucket is None:
                    raise ValueError(
                        "If using a path starting with 'gs://', you must include the bucket name unless it"
                        "is specified with bucket= or if TransparentPath already has been set to use a"
                        "specified bucket"
                    )
                path = str(path).replace(remote_prefix, "")

            else:
                bucket_from_path = splitted[1].split("/")[0]
                if bucket is not None:
                    if bucket != bucket_from_path:
                        raise ValueError(
                            f"Bucket name {bucket_from_path} was found in your path name, but it does "
                            f"not match the bucket name you specified with bucket={bucket}"
                        )
                else:
                    bucket = bucket_from_path
                if TransparentPath.bucket is None:
                    TransparentPath.bucket = bucket_from_path
                path = str(path).replace(remote_prefix, "").replace(bucket_from_path, "")
                if path.startswith("/"):
                    path = path[1:]

        self.__path = Path(str(path).encode("utf-8").decode("utf-8"), **kwargs)

        self.project = project if project is not None else TransparentPath.project
        self.bucket = bucket if bucket is not None else TransparentPath.bucket
        self.token = token if token is not None else TransparentPath.token
        self.fs_kind = fs if fs is not None else TransparentPath.fs_kind
        if self.fs_kind == "":
            self.fs_kind = "local"
        self.fs = None
        self.nas_dir = TransparentPath.nas_dir

        if "gcs" in self.fs_kind:
            if self.project is None:
                raise ValueError("If File System is to be GCS, please provide the project name")
            if self.bucket is None:
                raise ValueError("If File System is to be GCS, please provide the bucket name")
            self.fs_kind = f"gcs_{self.project}"

        # Set the file system of this class's instance. If an instance of same file system exists in class's fss,
        # will use it. Else, will create a new one and share it with class for future re-use.
        # If no fs was specified and no file system was previously set for class, will use local by default and set it
        # for class.
        self._set_fs()

        if self.fs_kind == "local":
            self.__path = self.__path.absolute()

        if collapse:
            self.__path = collapse_ddots(self.__path)

        if self.fs_kind == "local":

            # ON LOCAL

            if len(self.__path.parts) > 0 and self.__path.parts[0] == "..":
                raise ValueError("The path can not start with '..'")

        else:

            # ON GCS

            # Remove occurences of nas_dir at beginning of path, if any
            if self.nas_dir is not None and (
                str(self.__path).startswith(os.path.abspath(self.nas_dir) + os.sep) or str(self.__path) == self.nas_dir
            ):
                self.__path = self.__path.relative_to(self.nas_dir)

            if str(self.__path) == "." or str(self.__path) == "/":
                self.__path = Path(self.bucket)
            elif len(self.__path.parts) > 0:
                if self.__path.parts[0] == "..":
                    raise ValueError("Trying to access a path before bucket")
                if str(self.__path)[0] == "/":
                    self.__path = Path(str(self.__path)[1:])

                if not str(self.__path.parts[0]) == self.bucket:
                    self.__path = Path(self.bucket) / self.__path
            else:
                self.__path = Path(self.bucket) / self.__path
            if len(self.__path.parts) > 1 and self.bucket in self.__path.parts[1:]:
                raise ValueError("You should never use your bucket name as a directory or file name.")

        if nocheck is False and TransparentPath._do_check:
            self._check_multiplicity()

    @property
    def path(self):
        return self.__path

    @path.setter
    def path(self, value):
        raise AttributeError("Can not set protected attribute 'path'")

    def __contains__(self, item: str) -> bool:
        """Overload of 'in' operator
        Use __fspath__ instead of str(self) so that any method trying to assess whether the path is on gcs using
        '//' in path will return True.
        """
        return item in self.__fspath__()

    # noinspection PyUnresolvedReferences
    def __eq__(self, other: TransparentPath) -> bool:
        """Two paths are equal if their absolute pathlib.Path (double dots collapsed) are the same, and all other
        attributes are the same."""
        if not isinstance(other, TransparentPath):
            return False
        p1 = collapse_ddots(self)
        p2 = collapse_ddots(other)
        if p1.__fspath__() != p2.__fspath__():
            return False
        if p1.fs_kind != p2.fs_kind:
            return False
        return True

    def __lt__(self, other: TransparentPath) -> bool:
        return str(self) < str(other)

    def __gt__(self, other: TransparentPath) -> bool:
        return str(self) > str(other)

    def __le__(self, other: TransparentPath) -> bool:
        return str(self) <= str(other)

    def __ge__(self, other: TransparentPath) -> bool:
        return str(self) >= str(other)

    def __add__(self, other: str) -> TransparentPath:
        """Alias of truediv

        You can do :
        >>> from transparentpath import TransparentPath
        >>> p = TransparentPath("/chat")
        >>> p + "chien"
        /chat/chien

        If you want to add a string without having a '/' poping, use 'append':
        >>> from transparentpath import TransparentPath
        >>> p = TransparentPath("/chat")
        >>> p.append("chien")
        /chatchien
        """
        return self.__truediv__(other)

    def __iadd__(self, other: str) -> TransparentPath:
        return self.__itruediv__(other)

    def __radd__(self, other):
        raise TypeError(
            "You cannot div/add by a TransparentPath because they are all absolute path, which would result in a path "
            "before root "
        )

    def __truediv__(self, other: str) -> TransparentPath:
        """Overload of the division ('/') method
        TransparentPath behaves like pathlib.Path in regard to the division :
        it appends the denominator to the numerator.

        Parameters
        ----------
        other: str
            The relative path to append to self

        Returns
        -------
        TransparentPath
            The appended path
        """

        if other.startswith(self.sep):
            other = other[1:]

        if type(other) == str:
            return TransparentPath(self.__path / other, fs=self.fs_kind, project=self.project, bucket=self.bucket)
        else:
            raise TypeError(f"Can not divide a TransparentPath by a {type(other)}, only by a string.")

    def __itruediv__(self, other: str) -> TransparentPath:
        """itruediv will be an actual itruediv only if other is a str"""

        if other.startswith(self.sep):
            other = other[1:]

        if type(other) == str:
            self.__path /= other
            return self
        else:
            raise TypeError(f"Can not divide a TransparentPath by a {type(other)}, only by a string.")

    def __rtruediv__(self, other: Union[TransparentPath, Path, str]):
        raise TypeError(
            "You cannot div/add by a TransparentPath because they are all absolute path, which would result in a path "
            "before root "
        )

    def __str__(self) -> str:
        """Will not contain gs://, even if the file system is GCS. To get the path with gs://, use self.__fspath__()"""
        return self.__fspath__()

    def __repr__(self) -> str:
        return str(self.__path)

    def __fspath__(self) -> str:
        if self.fs_kind == "local":
            return str(self.__path)
        else:
            return remote_prefix + str(self.__path)

    def __hash__(self) -> int:
        """Uniaue hash number.

         Two TransarentPath will have a same hash number if their fspath are the same and fs_kind (which will inlude
         the project name if remote) are the same."""
        hash_number = int.from_bytes((self.fs_kind + self.__fspath__()).encode(), "little") + int.from_bytes(
            self.fs_kind.encode(), "little"
        )
        return hash_number

    def __getattr__(self, obj_name: str) -> Any:
        """Overload of the __getattr__ method
        Is called when trying to fetch a method or attribute not implemeneted in the class. If it is a method,
        will then execute _obj_missing to check if the method has been translated, or exists in the file system
        object. If it is an attribute, will check whether it exists in pathlib.Path objects, and if so add it to
        self. If this new attribute is a pathlib.Path, casts it into a TransparentPath.


        Parameters
        ----------
        obj_name: str
            The method or attribute name


        Returns
        --------
        Any
            What the method/attribute 'obj_name' is supposed to return/be

        """

        if callable(self):
            raise AttributeError(f"{obj_name} does not belong to TransparentPath")

        if obj_name in TransparentPath._attributes:
            raise AttributeError(
                f"Attribute {obj_name} is expected to belong to TransparentPath but is not found. Something somehow "
                f"tried to access this attribute before a proper call to __init__. "
            )

        if obj_name in TransparentPath.translations:
            return lambda *args, **kwargs: self._obj_missing(obj_name, "translate", *args, **kwargs)

        elif obj_name in dir(self.fs):
            obj = getattr(self.fs, obj_name)
            if not callable(obj):
                exec(f"self.{obj_name} = obj")
            else:
                return lambda *args, **kwargs: self._obj_missing(obj_name, "fs", *args, **kwargs)

        elif obj_name in dir(self.__path):
            obj = getattr(self.__path, obj_name)
            if not callable(obj):
                # Fetch the self.path's attributes to set it to self
                if type(obj) == type(self.__path):  # noqa: E721
                    setattr(
                        self, obj_name, TransparentPath(obj, fs=self.fs_kind, bucket=self.bucket, project=self.project)
                    )
                    return TransparentPath(obj, fs=self.fs_kind, bucket=self.bucket, project=self.project)
                elif isinstance(obj, Iterable):
                    obj = self.cast_iterable(obj)
                    setattr(self, obj_name, obj)
                    return obj
                else:
                    setattr(self, obj_name, obj)
                    return obj
            elif self.fs_kind == "local":
                return lambda *args, **kwargs: self._obj_missing(obj_name, "pathlib", *args, **kwargs)
            else:
                raise AttributeError(f"{obj_name} is not an attribute nor a method of TransparentPath")

        elif obj_name in dir(""):
            obj = getattr("", obj_name)
            if not callable(obj):
                # Fetch the string's attributes to set it to self
                setattr(self, obj_name, obj)
                return obj
            else:
                return lambda *args, **kwargs: self._obj_missing(obj_name, "str", *args, **kwargs)
        else:
            raise AttributeError(f"{obj_name} is not an attribute nor a method of TransparentPath")

    # /////////////// #
    # PRIVATE METHODS #
    # /////////////// #

    @staticmethod
    def _set_nas_dir(obj, nas_dir):
        if nas_dir is not None:
            if isinstance(nas_dir, TransparentPath):
                obj.nas_dir = nas_dir.__path
            elif isinstance(nas_dir, str):
                obj.nas_dir = Path(nas_dir)
            else:
                obj.nas_dir = nas_dir

    def _update_cache(self):
        """Calls FileSystem's invalidate_cache() to discard the cache then calls a non-distruptive method (fs.info(
        bucket)) to update it.

        If local, on need to update the chache. Not even sure it needs to be invalidated...
        """
        self.fs.invalidate_cache()
        if "gcs" in self.fs_kind:
            try:
                self.fs.info(self.bucket)
            except FileNotFoundError:
                # noinspection PyStatementEffect
                self.buckets

    def _cast_fast(self, path: str) -> TransparentPath:
        return TransparentPath(path, fs=self.fs_kind, nocheck=True, bucket=self.bucket, project=self.project)

    def _cast_slow(self, path: str) -> TransparentPath:
        return TransparentPath(path, fs=self.fs_kind, nocheck=False, bucket=self.bucket, project=self.project)

    def _set_fs(self) -> None:
        """ Create a new filesystem objet from self, or get the existing one
        """

        if "gcs" in self.fs_kind:
            if self.bucket is None:
                raise ValueError("Need to provide a bucket name!")
            if self.project is None:
                raise ValueError("Need to provide a project name!")
            self.fs_kind = f"{self.fs_kind}"

        # If class has same kind of file system instance, use it
        use_common = False

        if self.fs_kind in TransparentPath.fss:
            use_common = True

        if use_common:
            self.fs = TransparentPath.fss[self.fs_kind]
            self.nas_dir = TransparentPath.nas_dir
        # Else, init a new file system instance and share it with
        # TransparentPath
        else:
            self.fs = get_fs(self.fs_kind.replace(f"_{self.project}", ""), self.project, self.bucket, self.token)
            TransparentPath.fss[self.fs_kind] = self.fs
            # If TransparentPath's main fs_kind was not set, set it
            if TransparentPath.unset:
                TransparentPath.fs_kind = self.fs_kind
                TransparentPath.unset = False
            if TransparentPath.bucket is None:
                TransparentPath.bucket = self.bucket
            if TransparentPath.project is None:
                TransparentPath.project = self.project

        check_buckets(self.fs, self.bucket)

        if self.fs_kind == "local":
            self.sep = TransparentPath.LOCAL_SEP
        else:
            self.sep = "/"

    def _obj_missing(self, obj_name: str, kind: str, *args, **kwargs) -> Any:
        """Method to catch any call to a method/attribute missing from the class.
        Tries to call the object on the class's FileSystem object or the instance's self.path (a pathlib.Path object) if
        FileSystem is local 


        Parameters
        ----------
        obj_name: str
            The missing object's name

        kind: str
            Either 'fs', 'pathlib' or 'translate'

        args
            args to pass to the object

        kwargs
            kwargs to pass to the object


        Returns
        -------
        Any
            What the missing object is supposed to return

        """

        if TransparentPath._do_check:
            self._check_multiplicity()
        # Append the absolute path to self.path according to whether the object
        # needs it and whether we are in gcs or local
        new_args = self._transform_path(obj_name, *args)

        # Object is a method and exists in FileSystem object but has a
        # different name or its kwargs have different names, so use the
        # MethodTranslator class
        if kind == "translate":
            translated, new_args, new_kwargs = TransparentPath.translations[obj_name].translate(
                self.fs_kind, *new_args, **kwargs
            )
            if "self" in translated:
                the_method = getattr(TransparentPath, translated.split("self.")[1])
                to_ret = the_method(self, *args, **new_kwargs)
                return to_ret
            else:
                the_method = getattr(self.fs, translated)
                to_ret = the_method(*new_args, **new_kwargs)
            return to_ret

        # Here, could be a method or an attribute.
        # It exists in FileSystem and has same name and same kwargs (if is a
        # method).
        elif kind == "fs":
            the_obj = getattr(self.fs, obj_name)
            if callable(the_obj):
                to_ret = the_obj(*new_args, **kwargs)
            else:
                return the_obj
            return to_ret

        # Method does not exist in FileSystem, but exists in pathlib,
        # so try that instead. Do not use new_args in that case, we do not need
        # absolute path
        elif kind == "pathlib":
            # If arrives there, then it must be a method. If it had been an
            # attribute, it would have been caught in __getattr__.
            the_method = getattr(Path, obj_name)
            to_ret = the_method(self.__path, *args, **kwargs)
            return to_ret
        elif kind == "str":
            # If arrives there, then it must be a method, and of str. If it had been an
            # attribute, it would have been caught in __getattr__.
            the_method = getattr(str, obj_name)
            to_ret = the_method(str(self), *args, **kwargs)
            return to_ret
        else:
            raise ValueError(f"Unknown value {kind} for attribute kind")

    def _transform_path(self, method_name: str, *args: Tuple) -> Tuple:
        """
        File system methods take self.path as first argument, so add its absolute path as first argument of args. 
        Some, like ls or glob, are given a relative path to append to self.path, so we need to change the first 
        element of args from args[0] to self.path / args[0]

        Parameters
        ----------
        method_name: str
            The method name, to check whether it needs to append self.path
            or not

        args: Tuple
            The args to pass to the method

        Returns
        -------
        Tuple
            Either the unchanged args, or args with the first element
            prepended by self, or args with a new first element (self)
        """
        new_args = [self]
        if method_name in TransparentPath.method_without_self_path:
            return args
        elif method_name in TransparentPath.method_path_concat:
            # Assumes first given arg in args must be concatenated with
            # absolute self.path
            if len(args) > 0:
                new_args = [str(self.__path / str(args[0]))]
                if len(args) > 1:
                    new_args.append(args[1:])
            new_args = tuple(new_args)
        else:
            # noinspection PyTypeChecker
            new_args = tuple([str(self.__path)] + list(args))
        return new_args

    def _check_multiplicity(self) -> None:
        """Checks if several objects correspond to the path.
        Raises MultipleExistenceError if so, does nothing if not.
        """
        if TransparentPath._do_update_cache:
            self._update_cache()
        if str(self.__path) == self.bucket or str(self.__path) == "/":
            return
        if not self.exists():
            return

        thels = self.fs.ls(str(collapse_ddots(self.__path / "..")))
        if len(thels) > 1:
            thels = [Path(apath).name for apath in thels if Path(apath).name == self.name]
            if len(thels) > 1:
                raise MultipleExistenceError(self, thels)

    def _do_nothing(self) -> None:
        """ does nothing (you don't say) """
        pass

    # ////////////// #
    # PUBLIC METHODS #
    # ////////////// #

    def get_absolute(self) -> TransparentPath:
        """Returns self, since all TransparentPaths are absolute

        Returns
        -------
        TransparentPath
            self

        """
        return self

    @property
    def absolute(self) -> TransparentPath:
        """Returns self, since all TransparentPaths are absolute

        Returns
        -------
        TransparentPath
            self

        """
        return self

    def mkbucket(self, name: Optional[str] = None) -> None:
        raise NotImplementedError

    def rmbucket(self, name: Optional[str] = None) -> None:
        raise NotImplementedError

    def is_dir(self, exist: bool = False) -> bool:
        """Check if self is a directory


        Parameters
        ----------
        exist: bool
            If False and if using GCS, is_dir() returns True if the directory does not exist and no file with
            the same path exist. Otherwise, only returns True if the directory really exists (Default value = False).


        Returns
        -------
        bool

        """
        if TransparentPath._do_check:
            self._check_multiplicity()
        if self.fs_kind == "local":
            return self.__path.is_dir()
        else:
            if exist and not self.exists():
                return False
            if self.is_file():
                return False
            return True

    def is_file(self) -> bool:
        """Check if self is a file
        On GCS, leaves are always files even if created with mkdir.


        Returns
        -------
        bool

        """

        if TransparentPath._do_check:
            self._check_multiplicity()
        if not self.exists():
            return False

        if self.fs_kind == "local":
            return self.__path.is_file()
        else:
            # GCS is shit and sometimes needs to be checked twice
            if self.info()["type"] == "file" and self.info()["type"] == "file":
                return True
            else:
                return False

    def unlink(self, **kwargs):
        """Alias of rm, to match pathlib.Path method"""
        self.rm(**kwargs)

    def rm(self, absent: str = "raise", ignore_kind: bool = False, **kwargs) -> None:
        """Removes the object pointed to by self if exists.
        Remember that leaves are always files on GCS, so rm will remove the path if it is a leaf on GCS


        Parameters
        ----------
        absent: str
            What to do if trying to remove an item that does not exist. Can
            be 'raise' or 'ignore' (Default value = 'raise')

        ignore_kind: bool
            If True, will remove anything pointed by self. If False,
            will raise an error if self points to a file and 'recursive' was
            specified in kwargs, or if self points to a dir and 'recursive'
            was not specified (Default value = False)

        kwargs
            The kwargs to pass to file system's rm method


        Returns
        -------
        None

        """

        if TransparentPath._do_check:
            self._check_multiplicity()

        if absent != "raise" and absent != "ignore":
            raise ValueError(f"Unexpected value for argument 'absent' : {absent}")

        # Asked to remove a directory...
        recursive = kwargs.get("recursive", False)

        if recursive:
            if not self.is_dir(exist=True):
                # ...but self points to something that is not a directory!
                if self.exists():
                    # Delete anyway
                    if ignore_kind:
                        del kwargs["recursive"]
                        self.rm(absent, **kwargs)
                    # or raise
                    else:
                        raise NotADirectoryError("The path does not point to a directory!")
                # ...but self points to something that does not exist!
                else:
                    if absent == "raise":
                        raise NotADirectoryError("There is no directory here!")
                    else:
                        return
            # ...deletes the directory
            else:
                try:
                    self.fs.rm(self.__fspath__(), **kwargs)
                except OSError as e:
                    if "not found" in str(e).lower():
                        # It is possible that another parallel program deleted the object, in that case just pass
                        pass
                    else:
                        raise e
        # Asked to remove a file...
        else:
            # ...but self points to a directory!
            if self.is_dir(exist=True):
                # Delete anyway
                if ignore_kind:
                    kwargs["recursive"] = True
                    self.rm(absent=absent, ignore_kind=True, **kwargs)
                # or raise
                else:
                    raise IsADirectoryError("The path points to a directory")
            else:
                # ... but nothing is at self
                if not self.exists():
                    if absent == "raise":
                        raise FileNotFoundError(f"Could not find file {self}")
                    else:
                        return
                else:
                    try:
                        self.fs.rm(self.__fspath__(), **kwargs)
                    except OSError as e:
                        if "not found" in str(e).lower():
                            # It is possible that another parallel program deleted the object, in that case just pass
                            pass
                        else:
                            raise e

    def rmdir(self, absent: str = "raise", ignore_kind: bool = False) -> None:
        """Removes the directory corresponding to self if exists
        Remember that leaves are always files on GCS, so rmdir will never remove a leaf on GCS


        Parameters
        ----------
        absent: str
            What to do if trying to remove an item that does not exist. Can
            be 'raise' or 'ignore' (Default value = 'raise')

        ignore_kind: bool
            If True, will remove anything pointed by self. If False,
            will raise an error if self points to a file and 'recursive' was
            specified in kwargs, or if self point to a dir and 'recursive'
            was not specified (Default value = False)

        """
        self.rm(absent=absent, ignore_kind=ignore_kind, recursive=True)

    def glob(self, wildcard: str = "*", fast: bool = False) -> Iterator[TransparentPath]:
        """Returns a list of TransparentPath matching the wildcard pattern.

        By default, the wildcard is '*'. It means 'thepath/*', so will glob in the directory.

        WARNING : on GCS, directories are not detected by glob.

        Parameters
        -----------
        wildcard: str
            The wilcard pattern to match, relative to self (Default value = "*")

        fast: bool
            If True, does not check multiplicity when converting output paths to TransparentPath, significantly
            speeding up the process (Default value = False)


        Returns
        --------
        Iterator[TransparentPath]
            The list of items matching the pattern

        """

        if TransparentPath._do_check:
            self._check_multiplicity()

        if not self.is_dir(exist=True):
            raise NotADirectoryError("The path must be a directory if you want to glob in it")

        if wildcard.startswith("/") or wildcard.startswith("\\"):
            wildcard = wildcard[1:]

        if wildcard.startswith("**/*"):
            wildcard = wildcard.replace("**/*", "**")

        path_to_glob = (self.__path / wildcard).__fspath__()

        if fast:
            to_ret = map(self._cast_fast, self.fs.glob(path_to_glob))
        else:
            to_ret = map(self._cast_slow, self.fs.glob(path_to_glob))
        return to_ret

    def with_suffix(self, suffix: str) -> TransparentPath:
        """Returns a new TransparentPath object with a changed suffix
        Uses the with_suffix method of pathlib.Path


        Parameters
        -----------
        suffix: str
            suffix to use, with the dot ('.pdf', '.py', etc ..)

        Returns
        --------
        TransparentPath

        """
        if not suffix.startswith("."):
            suffix = f".{suffix}"
        return TransparentPath(
            self.__path.with_suffix(suffix), fs=self.fs_kind, bucket=self.bucket, project=self.project
        )

    def ls(self, path_to_ls: str = "", fast: bool = False) -> Iterator[TransparentPath]:
        """ Unlike glob, if on GCS, will also see directories.


        Parameters
        -----------
        path_to_ls: str
            Path to ls, relative to self (default value = "")
        fast: bool
            If True, does not check multiplicity when converting output
            paths to TransparentPath, significantly speeding up the process
            (Default value = False)


        Returns
        --------
        Iterator[TransparentPath]

        """

        if TransparentPath._do_check:
            self._check_multiplicity()

        if not self.is_dir(exist=True):
            raise NotADirectoryError("The path must be a directory if you want to ls in it")

        if fast:
            to_ret = map(self._cast_fast, self.fs.ls(str(self / path_to_ls)))
        else:
            to_ret = map(self._cast_slow, self.fs.ls(str(self / path_to_ls)))
        return to_ret

    def cd(self, path: Optional[str] = None) -> None:
        """cd-like command. Works inplace
        
        Will collapse double-dots ('..'), so not compatible with symlinks. If path is absolute (starts with '/' or 
        bucket name or is empty), will return a path starting from root directory if FileSystem is local, from bucket 
        if it is GCS. If passing None or "" , will have the same effect than "/" on GCS, will return the current 
        working directory on local. If passing ".", will return a path at the location of self. Will raise an error 
        if trying to access a path before root or bucket. 


        Parameters
        ----------
        path: str
            The path to cd to. Absolute, or relative to self.
            (Default value = None)


        Returns
        -------
        None: works inplace

        """

        # Will collapse any '..'

        if not isinstance(path, str) or isinstance(path, TransparentPath):
            raise TypeError("Can only pass a string")

        path = path.replace(remote_prefix, "")

        if TransparentPath._do_update_cache:
            self._update_cache()
        if "gcs" in self.fs_kind and str(path) == self.bucket or path == "" or path == "/":
            self.__path = Path(self.bucket)
            return

        # If asked to cd to home, return path script calling directory
        if path == "" or path is None:
            self.__path = Path()
            return

        # noinspection PyUnresolvedReferences
        self.__path = self.__path / path

        if self.fs_kind == "local":
            # If asked for an absolute path
            if path.startswith("/"):
                self.__path = Path(path)
                return
            # noinspection PyUnresolvedReferences
            if len(self.__path.parts) == 0:
                return
            # noinspection PyUnresolvedReferences
            if self.__path.parts[0] == "..":
                raise ValueError("The first part of a path can not be '..'")
        else:
            # If asked for an absolute path
            if path.startswith("/"):
                self.__path = Path(self.bucket) / path[1:]
                return
            # noinspection PyUnresolvedReferences
            if self.__path == 1:  # On gcs, first part is bucket
                return
            # noinspection PyUnresolvedReferences
            if self.__path.parts[1] == "..":
                raise ValueError("Trying to access a path before bucket")

        # noinspection PyUnresolvedReferences
        self.__path = collapse_ddots(self.__path)

    def touch(self, present: str = "ignore", **kwargs) -> None:
        """Creates the file corresponding to self if does not exist.

        Raises FileExistsError if there already is an object that is not a file at self. Default behavior is to
        create parent directories of the file if needed. This can be canceled by passing 'create_parents=False', but
        only if not using GCS, since directories are not a thing on GCS.


        Parameters
        ----------
        present: str
            What to do if there is already something at self. Can be "raise" or "ignore" (Default value = "ignore")

        kwargs
            The kwargs to pass to file system's touch method


        Returns
        -------
        None

        """

        if TransparentPath._do_update_cache:
            self._update_cache()

        if present != "raise" and present != "ignore":
            raise ValueError(f"Unexpected value for argument 'present' : {present}")

        if self.exists():
            if self.is_file() and present == "raise":
                raise FileExistsError
            elif not self.is_file():
                raise FileExistsError(f"There is already an object at {self} which is not a file.")
            else:
                return

        for parent in self.parents:
            p = TransparentPath(parent, fs=self.fs_kind, bucket=self.bucket, project=self.project)
            if p.is_file():
                raise FileExistsError(f"A parent directory can not be created because there is already a file at {p}")
            elif not p.exists():
                p.mkdir()

        self.fs.touch(self.__fspath__(), **kwargs)

    def mkdir(self, present: str = "ignore", **kwargs) -> None:
        """Creates the directory corresponding to self if does not exist

        Remember that leaves are always files on GCS, so can not create a directory on GCS. Thus, the function will
        have no effect on GCS.


        Parameters
        ----------
        present: str
            What to do if there is already something at self. Can be "raise" or "ignore" (Default value = "ignore")

        kwargs
            The kwargs to pass to file system's mkdir method


        Returns
        -------
        None

        """

        if present != "raise" and present != "ignore":
            raise ValueError(f"Unexpected value for argument 'present' : {present}")

        if self.exists():
            if self.is_dir() and present == "raise":
                raise FileExistsError(f"There is already a directory at {self}")
            if not self.is_dir():
                raise FileExistsError(f"There is already an object at {self} and it is not a  directory")
            return

        for parent in self.parents:
            if TransparentPath(parent, fs=self.fs_kind, bucket=self.bucket, project=self.project).is_file():
                raise FileExistsError(
                    "A parent directory can not be "
                    "created because there is already a"
                    " file at"
                    f" {TransparentPath(parent, fs=self.fs_kind, bucket=self.bucket, project=self.project)}"
                )

        if self.fs_kind == "local":
            # Use _obj_missing instead of callign mkdir directly because
            # file systems mkdir has some kwargs with different name than
            # pathlib.Path's  mkdir, and this is handled in _obj_missing
            self._obj_missing("mkdir", kind="translate", **kwargs)
        else:
            # Does not mean anything to create a directory on GCS
            pass

    def stat(self) -> dict:
        """Calls file system's stat method and translates the key to os.stat_result() keys"""
        key_translation = {
            "size": "st_size",
            "timeCreated": "st_ctime",
            "updated": "st_mtime",
            "created": "st_ctime",
            "mode": "st_mode",
            "uid": "st_uid",
            "gid": "st_gid",
            "mtime": "st_mtime",
        }

        stat = self.fs.stat(self.__fspath__())
        statkeys = list(stat.keys())
        for key in statkeys:
            if key in key_translation:
                if key == "timeCreated" or key == "updated":
                    dt = datetime.strptime(stat[key], "%Y-%m-%dT%H:%M:%S.%fZ")
                    stat[key] = dt.timestamp()
                if key == "created" or key == "mtime":
                    stat[key] = int(stat[key])
                stat[key_translation[key]] = stat.pop(key)

        for key in key_translation.values():
            if key not in stat:
                stat[key] = None

        return stat

    def append(self, other: str) -> TransparentPath:
        return TransparentPath(str(self) + other, fs=self.fs_kind, bucket=self.bucket, project=self.project)

    def walk(self) -> Iterator[Tuple[TransparentPath, List[TransparentPath], List[TransparentPath]]]:
        """Like os.walk, except all outputs are TransparentPaths (so, absolute paths)

        Returns
        -------
        Iterator[Tuple[TransparentPath, List[TransparentPath], List[TransparentPath]]]
            root, dirs and files, like os.walk
        """
        outputs = self.fs.walk(self.__fspath__())
        for output in outputs:
            root = TransparentPath(output[0], fs=self.fs_kind, bucket=self.bucket, project=self.project)
            dirs = [root / p for p in output[1]]
            files = [root / p for p in output[2]]
            yield root, dirs, files

    def exist(self):
        """To prevent typo of 'exist()' without an -s"""
        return self.exists()

    def exists(self):
        if TransparentPath._do_update_cache:
            self._update_cache()
        return self.fs.exists(self.__fspath__())

    @property
    def buckets(self):
        return get_buckets(self.fs)

    def cast_iterable(self, iter_: Iterable):
        """Used by self.walk"""
        if isinstance(iter_, Path) or isinstance(iter_, TransparentPath):
            return TransparentPath(iter_, fs=self.fs_kind, bucket=self.bucket, project=self.project)
        elif isinstance(iter_, str):
            return iter_
        elif not isinstance(iter_, Iterable):
            return iter_
        else:
            to_ret = [self.cast_iterable(item) for item in iter_]
            return to_ret

    def read(
        self,
        *args,
        get_obj: bool = False,
        use_pandas: bool = False,
        update_cache: bool = True,
        use_dask: bool = False,
        **kwargs,
    ) -> Any:
        """Method used to read the content of the file located at self

        Will raise FileNotFound error if there is no file. Calls a specific method to read self based on the suffix
        of self.path:
            1: .csv : will use pandas's read_csv
            2: .parquet : will use pandas's read_parquet with pyarrow engine
            3: .hdf5 or .h5 : will use h5py.File or pd.HDFStore (if use_pandas = True). Since it does not support
            remote file systems, the file will be downloaded localy in a tmp file read, then removed.
            4: .json : will use open() method to get file content then json.loads to get a dict
            5: .xlsx : will use pd.read_excel
            6: any other suffix : will return a IO buffer to read from, or the string contained in the file if
            get_obj is False.

        For any of the reading method, the appropriate packages need to have been installed by calling
        `pip install transparentpath[something]`
        The possibilities for 'something' are 'pandas-csv', 'pandas-parquet', 'pandas-excel', 'hdf5', 'json', 'dask'.
        You can install all possible packages by putting 'all' in place of 'something'.

        The default installation of transperantpath is 'vanilla', which will only support read and write of text
         or binary files, and the use of with open(...).

        Parameters
        ----------
        get_obj: bool
            Only relevant for files that are not csv, parquet nor HDF5. If True returns the IO Buffer,
            else the string contained in the IO Buffer (Default value = False)
        use_pandas: bool
            Must pass it as True if hdf5 file was written using HDFStore and not h5py.File (Default value = False)
        update_cache: bool
            FileSystem objects do not necessarily follow changes on the system in real time if they were not
            perfermed by them directly. If update_cache is True, the FileSystem will update its cache before trying
            to read anything. If False, it won't, potentially saving some time but this might result in a
            FileNotFoundError. (Default value = True)
        use_dask: bool
            To return a Dask DataFrame instead of a pandas DataFrame. Only makes sense if file suffix is xlsx, csv,
            parquet. (Default value = False)
        args:
            any args to pass to the underlying reading method
        kwargs:
            any kwargs to pass to the underlying reading method

        Returns
        -------
        Any
        """

        if TransparentPath._do_check:
            self._check_multiplicity()

        if use_dask:
            self.check_dask()
        else:
            if not self.is_file():
                raise FileNotFoundError(f"Could not find file {self}")

        if self.suffix == ".csv":
            return self.read_csv(update_cache=update_cache, use_dask=use_dask, **kwargs)
        elif self.suffix == ".parquet":
            index_col = None
            if "index_col" in kwargs:
                index_col = kwargs["index_col"]
                del kwargs["index_col"]
            # noinspection PyNoneFunctionAssignment
            content = self.read_parquet(update_cache=update_cache, use_dask=use_dask, **kwargs)
            if index_col:
                # noinspection PyUnresolvedReferences
                content.set_index(content.columns[index_col])
            return content
        elif self.suffix == ".hdf5" or self.suffix == ".h5":
            return self.read_hdf5(update_cache=update_cache, use_pandas=use_pandas, use_dask=use_dask, **kwargs)
        elif self.suffix == ".json":
            return self.read_json(*args, get_obj=get_obj, update_cache=update_cache, **kwargs)
        elif self.suffix in [".xlsx", ".xls", ".xlsm"]:
            return self.read_excel(update_cache=update_cache, use_dask=use_dask, **kwargs)
        else:
            return self.read_text(*args, get_obj=get_obj, update_cache=update_cache, **kwargs)

    # noinspection PyUnresolvedReferences
    def write(
        self,
        data: Any,
        *args,
        set_name: str = "data",
        use_pandas: bool = False,
        overwrite: bool = True,
        present: str = "ignore",
        update_cache: bool = True,
        make_parents: bool = False,
        **kwargs,
    ) -> Union[None, "pd.HDFStore", "h5py.File"]:
        """Method used to write the content of the file located at self
        Calls a specific method to write data based on the suffix of self.path:
            1: .csv : will use pandas's to_csv
            2: .parquet : will use pandas's to_parquet with pyarrow engine
            3: .hdf5 or .h5 : will use h5py.File. Since it does not support remote file systems, the file will be
            created localy in a tmp filen written to, then uploaded and removed localy.
            4: .json : will use jsonencoder.JSONEncoder class. Works with DataFrames and np.ndarrays too.
            5: .xlsx : will use pandas's to_excel
            5: any other suffix : uses self.open to write to an IO Buffer
        Parameters
        ----------
        data: Any
            The data to write
        set_name: str
            Name of the dataset to write. Only relevant if using HDF5 (Default value = 'data')
        use_pandas: bool
            Must pass it as True if hdf file must be written using HDFStore and not h5py.File
        overwrite: bool
            If True, any existing file will be overwritten. Only relevant for csv, hdf5 and parquet files,
            since others use the 'open' method, which args already specify what to do (Default value = True).
        present: str
            Indicates what to do if overwrite is False and file is present. Here too, only relevant for csv,
            hsf5 and parquet files.
        update_cache: bool
            FileSystem objects do not necessarily follow changes on the system if they were not perfermed by them
            directly. If update_cache is True, the FileSystem will update its cache before trying to read anything.
            If False, it won't, potentially saving some time but this might result in a FileExistError. (Default
            value = True)
        make_parents: bool
            If True and if the parent arborescence does not exist, it is created. (Default value = False)
        args:
            any args to pass to the underlying writting method
        kwargs:
            any kwargs to pass to the underlying reading method
        Returns
        -------
        Union[None, pd.HDFStore, h5py.File]
        """

        if TransparentPath._do_check:
            self._check_multiplicity()

        if make_parents and not self.parent.is_dir():
            self.parent.mkdir()

        if self.suffix != ".hdf5" and self.suffix != ".h5" and data is None:
            data = args[0]
            args = args[1:]

        if self.suffix == ".csv":
            ret = self.to_csv(data=data, overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,)
            if ret is not None:
                # To skip the assert at the end of the function. Indeed if something is returned it means we used
                # Dask, which will have written files with a different name than self, so the assert would fail.
                return
        elif self.suffix == ".parquet":
            self.to_parquet(
                data=data, overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,
            )
            if "dask" in str(type(data)):
                # noinspection PyUnresolvedReferences
                assert self.with_suffix("").is_dir(exist=True)
                return
        elif self.suffix == ".hdf5" or self.suffix == ".h5":
            ret = self.to_hdf5(
                data=data, set_name=set_name, use_pandas=use_pandas, update_cache=update_cache, **kwargs,
            )
            if ret is not None:
                return ret
        elif self.suffix == ".json":
            self.to_json(
                data=data, overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,
            )
        elif self.suffix == ".txt":
            self.write_stuff(
                *args, data=data, overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,
            )
        elif self.suffix == ".xlsx":
            self.to_excel(
                data=data, overwrite=overwrite, present=present, update_cache=update_cache, use_dask=use_dask, **kwargs,
            )
        else:
            self.write_bytes(
                *args, data=data, overwrite=overwrite, present=present, update_cache=update_cache, **kwargs,
            )
        assert self.is_file()

    # READ CSV

    def read_csv(self, *args, **kwargs):
        use_dask = False
        if "use_dask" in kwargs:
            use_dask = kwargs["use_dask"]
            del kwargs["use_dask"]
        if use_dask:
            return self.read_csv_dask(*args, **kwargs)
        else:
            return self.read_csv_classic(*args, **kwargs)

    def read_csv_dask(self, *args, **kwargs):
        raise ImportError(errormessage("dask"))

    def read_csv_classic(self, *args, **kwargs):
        raise ImportError(errormessage("pandas"))

    # READ HDF5

    def read_hdf5(self, *args, **kwargs):
        use_dask = False
        if "use_dask" in kwargs:
            use_dask = kwargs["use_dask"]
            del kwargs["use_dask"]
        if use_dask:
            return self.read_hdf5_dask(*args, **kwargs)
        else:
            return self.read_hdf5_classic(*args, **kwargs)

    def read_hdf5_dask(self, *args, **kwargs):
        raise ImportError(errormessage("dask,hdf5"))

    def read_hdf5_classic(self, *args, **kwargs):
        raise ImportError(errormessage("hdf5"))

    # READ EXCEL

    def read_excel(self, *args, **kwargs):
        use_dask = False
        if "use_dask" in kwargs:
            use_dask = kwargs["use_dask"]
            del kwargs["use_dask"]
        if use_dask:
            return self.read_excel_dask(*args, **kwargs)
        else:
            return self.read_excel_classic(*args, **kwargs)

    def read_excel_dask(self, *args, **kwargs):
        raise ImportError(errormessage("dask,excel"))

    def read_excel_classic(self, *args, **kwargs):
        raise ImportError(errormessage("excel"))

    # READ PARQUET

    def read_parquet(self, *args, **kwargs):
        use_dask = False
        if "use_dask" in kwargs:
            use_dask = kwargs["use_dask"]
            del kwargs["use_dask"]
        if use_dask:
            return self.read_parquet_dask(*args, **kwargs)
        else:
            return self.read_parquet_classic(*args, **kwargs)

    def read_parquet_dask(self, *args, **kwargs):
        raise ImportError(errormessage("dask,parquet"))

    def read_parquet_classic(self, *args, **kwargs):
        raise ImportError(errormessage("parquet"))

    # READ JSON

    def read_json(self, *args, **kwargs):
        raise ImportError(errormessage("json"))

    # WRITE CSV

    def to_csv(self, data, *args, **kwargs):
        if "dask" in str(type(data)):
            return self.to_csv_dask(data, *args, **kwargs)
        else:
            self.to_csv_classic(data, *args, **kwargs)

    def to_csv_classic(self, *args, **kwargs):
        raise ImportError(errormessage("pandas"))

    def to_csv_dask(self, *args, **kwargs):
        raise ImportError(errormessage("dask"))

    # WRITE HDF5

    def to_hdf5(self, data, *args, **kwargs):
        if "dask" in str(type(data)):
            return self.to_hdf5_dask(data, *args, **kwargs)
        else:
            return self.to_hdf5_classic(data, *args, **kwargs)

    def to_hdf5_classic(self, *args, **kwargs):
        raise ImportError(errormessage("hdf5"))

    def to_hdf5_dask(self, *args, **kwargs):
        raise ImportError(errormessage("dask,hdf5"))

    # WRITE EXCEL

    def to_excel(self, data, *args, **kwargs):
        if "dask" in str(type(data)):
            self.to_excel_dask(data, *args, **kwargs)
        else:
            self.to_excel_classic(data, *args, **kwargs)

    def to_excel_classic(self, *args, **kwargs):
        raise ImportError(errormessage("excel"))

    def to_excel_dask(self, *args, **kwargs):
        raise ImportError(errormessage("dask,excel"))

    # WRITE EXCEL

    def to_parquet(self, data, *args, **kwargs):
        if "dask" in str(type(data)):
            self.to_parquet_dask(data, *args, **kwargs)
        else:
            self.to_parquet_classic(data, *args, **kwargs)

    def to_parquet_classic(self, *args, **kwargs):
        raise ImportError(errormessage("parquet"))

    def to_parquet_dask(self, *args, **kwargs):
        raise ImportError(errormessage("dask,parquet"))

    def to_json(self, data, *args, **kwargs):
        raise ImportError(errormessage("json"))

    def check_dask(self, *args, **kwargs):
        raise ImportError(errormessage("dask"))


# Do imports from detached files here because some of them import TransparentPath and need it fully declared.

from ..io.io import put, get, mv, cp, overload_open, read_text, write_stuff, write_bytes

overload_open()
setattr(TransparentPath, "put", put)
setattr(TransparentPath, "get", get)
setattr(TransparentPath, "mv", mv)
setattr(TransparentPath, "cp", cp)
setattr(TransparentPath, "read_text", read_text)
setattr(TransparentPath, "write_stuff", write_stuff)
setattr(TransparentPath, "write_bytes", write_bytes)

try:
    from ..io import zipfile
except ImportError:
    pass

try:
    # noinspection PyUnresolvedReferences
    from transparentpath.io.json import read, write

    setattr(TransparentPath, "read_json", read)
    setattr(TransparentPath, "to_json", write)
except ImportError:
    pass

try:
    # noinspection PyUnresolvedReferences
    from ..io.pandas import read, write

    setattr(TransparentPath, "read_csv_classic", read)
    setattr(TransparentPath, "to_csv_classic", write)
except ImportError:
    pass

try:
    # noinspection PyUnresolvedReferences
    from ..io.hdf5 import read, write

    setattr(TransparentPath, "read_hdf5_classic", read)
    setattr(TransparentPath, "to_hdf5_classic", write)
except ImportError:
    pass

try:
    # noinspection PyUnresolvedReferences
    from ..io.parquet import read, write

    setattr(TransparentPath, "read_parquet_classic", read)
    setattr(TransparentPath, "to_parquet_classic", write)
except ImportError:
    pass

try:
    from ..io.dask import (
        read_csv,
        write_csv,
        read_hdf5,
        write_hdf5,
        read_excel,
        write_excel,
        read_parquet,
        write_parquet,
    )

    setattr(TransparentPath, "read_parquet_dask", read_csv)
    setattr(TransparentPath, "to_parquet_dask", write_csv)
    setattr(TransparentPath, "read_parquet_dask", read_hdf5)
    setattr(TransparentPath, "to_parquet_dask", write_hdf5)
    setattr(TransparentPath, "read_parquet_dask", read_excel)
    setattr(TransparentPath, "to_parquet_dask", write_excel)
    setattr(TransparentPath, "read_parquet_dask", read_parquet)
    setattr(TransparentPath, "to_parquet_dask", write_parquet)
except ImportError:
    pass
