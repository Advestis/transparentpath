import builtins
import os
from datetime import datetime
from pathlib import Path
from typing import Union, Tuple, Any, Iterator, Optional

import pandas as pd
from .methodtranslator import MultiMethodTranslator
import gcsfs
from fsspec.implementations.local import LocalFileSystem


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


def get_fs(fs_kind: str, project: str, bucket: str) -> Union[gcsfs.GCSFileSystem, LocalFileSystem]:
    """Gets the FileSystem object of either gcs or local (Default)


    Parameters
    ----------
    fs_kind: str
        Returns GCSFileSystem if 'gcs_*', LocalFilsSystem if 'local'.

    project: str
        project name for GCS

    bucket: str
        bucket name for GCS

    Returns
    -------
    Union[gcsfs.GCSFileSystem, LocalFileSystem]
        The FileSystem object and the string 'gcs' or 'local'

    """
    if "gcs" in fs_kind:
        bucket = bucket.replace("/", "")
        fs = gcsfs.GCSFileSystem(project=project)
        # Will raise RefreshError if connection fails
        fs.glob(bucket)
        if f"{bucket}/" not in fs.buckets:
            raise NotADirectoryError(
                f"Bucket {bucket} does not exist in "
                f"project {project}, or you can not "
                f"see it. Available buckets are:\n"
                f"{fs.buckets}"
            )
        return fs
    else:
        return LocalFileSystem()


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
        file/directory.

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

    fss = {}
    fs_kind = ""
    project = None
    bucket = None
    nas_dir = "/media/SERVEUR"
    unset = True
    cwd = os.getcwd()
    cli = None

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

        TransparentPath._set_nas_dir(cls, nas_dir)

        if "gcs" in cls.fs_kind:
            if cls.bucket is None:
                raise ValueError("Need to provide a bucket name!")
            if cls.project is None:
                raise ValueError("Need to provide a project name!")
            cls.fs_kind = f"gcs_{cls.project}"

        cls.fss[cls.fs_kind] = get_fs(cls.fs_kind, cls.project, cls.bucket)
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

        fs: str
            The file system to use, 'local' or 'gcs'. If None, uses the default one set by set_global_fs if any,
            or 'local' (Default = None)

        project: str
            The project name if using GCS. If None and using GCS, then a GCSFileSystem must have been set earlier
            using set_global_fs() (Default = None)

        bucket: str
            The bucket name if using GCS If None and using GCS, then a GCSFileSystem must have been set earlier
            using set_global_fs() (Default = None)

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
            return

        # In case we initiate a path containing 'gs://'
        if "gs://" in str(path):

            if project is None:
                if TransparentPath.project is None:
                    raise ValueError("You specified a path starting with 'gs://' but did not specify any GCP project")
                project = TransparentPath.project

            if fs == "local":
                raise ValueError(
                    "You specified a path starting with 'gs://' but ask for it to be local. This is not possible."
                )
            fs = f"gcs_{project}"
            splitted = str(path).split("gs://")
            if len(splitted) == 0:
                if bucket is None and TransparentPath.bucket is None:
                    raise ValueError(
                        "If using a path starting with 'gs://', you must include the bucket name unless it"
                        "is specified with bucket= or if TransparentPath already has been set to use a"
                        "specified bucket"
                    )
                path = str(path).replace("gs://", "")

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
                path = str(path).replace("gs://", "").replace(bucket_from_path, "")
                if path.startswith("/"):
                    path = path[1:]

        self.__path = Path(str(path).encode("utf-8").decode("utf-8"), **kwargs)

        self.project = project if project is not None else TransparentPath.project
        self.bucket = bucket if bucket is not None else TransparentPath.bucket
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

        if nocheck is False:
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
        """You can do :
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
        if type(other) == str:
            return TransparentPath(self.__path / other, fs=self.fs_kind, project=self.project, bucket=self.bucket)
        else:
            raise TypeError(f"Can not divide a TransparentPath by a {type(other)}, only by a string.")

    def __itruediv__(self, other: str) -> TransparentPath:
        """itruediv will be an actual itruediv only if other is a str"""
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
            return "gs://" + str(self.__path)

    def __hash__(self) -> int:
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
            self.fs.buckets
            self.fs.info(self.bucket)

    def _cast_fast(self, path: str) -> TransparentPath:
        return TransparentPath(path, fs=self.fs_kind, nocheck=True, bucket=self.bucket, project=self.project)

    def _cast_slow(self, path: str) -> TransparentPath:
        return TransparentPath(path, fs=self.fs_kind, nocheck=False, bucket=self.bucket, project=self.project)

    def _set_fs(self) -> None:
        """ Create a new filesystem objet from self, or get the existing one


        Returns
        -------
        None

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
            if "gcs" in self.fs_kind and f"{self.bucket}/" not in self.fs.buckets:
                raise NotADirectoryError(
                    f"Bucket {self.bucket} does not exist in "
                    f"project {self.project}, or you can not "
                    f"see it. Available buckets are:\n"
                    f"{self.fs.buckets}"
                )
        # Else, init a new file system instance and share it with
        # TransparentPath
        else:
            self.fs = get_fs(self.fs_kind.replace(f"_{self.project}", ""), self.project, self.bucket)
            TransparentPath.fss[self.fs_kind] = self.fs
            # If TransparentPath's main fs_kind was not set, set it
            if TransparentPath.unset:
                TransparentPath.fs_kind = self.fs_kind
                TransparentPath.unset = False
            if TransparentPath.bucket is None:
                TransparentPath.bucket = self.bucket
            if TransparentPath.project is None:
                TransparentPath.project = self.project

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
            specified in kwargs, or if self point to a dir and 'recursive'
            was not specified (Default value = False)

        kwargs
            The kwargs to pass to file system's rm method


        Returns
        -------
        None

        """
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
                self.fs.rm(self.__fspath__(), **kwargs)
            assert not self.is_dir(exist=True)
        # Asked to remove a file...
        else:
            # ...but self points to a directory!
            if self.is_dir(exist=True):
                # Delete anyway
                if ignore_kind:
                    self.rm(absent=absent, recursive=True, **kwargs)
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
                    self.fs.rm(self.__fspath__(), **kwargs)
            assert not self.is_file()

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
        """Returns a list of TransparentPath matching the wildcard pattern

        By default, the wildcard is '*'. It means 'thepath/*', so will glob in the directory.

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

        self._check_multiplicity()

        if not self.is_dir(exist=True):
            raise NotADirectoryError("The path must be a directory if you want to glob in it")

        if wildcard.startswith("/") or wildcard.startswith("\\"):
            wildcard = wildcard[1:]

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
        return TransparentPath(
            self.__path.with_suffix(suffix), fs=self.fs_kind, bucket=self.bucket, project=self.project
        )

    def ls(self, path: str = "", fast: bool = False) -> Iterator[TransparentPath]:
        """ls-like method. Returns an Iterator of absolute TransparentPaths.


        Parameters
        -----------
        path: str
            relative path to ls. (Default value = "")

        fast: bool
            If True, does not check multiplicity when converting output
            paths to TransparentPath, significantly speeding up the process
            (Default value = False)


        Returns
        --------
        Iterator[TransparentPath]

        """
        self._update_cache()
        thepath = self / path
        # noinspection PyUnresolvedReferences
        if not thepath.exists():
            raise FileNotFoundError(f"Could not find location {self}")

        # noinspection PyUnresolvedReferences
        if thepath.is_file():
            raise NotADirectoryError("Can not ls on a file!")

        if fast:
            to_ret = map(self._cast_fast, self.fs.ls(str(thepath)),)
        else:
            to_ret = map(self._cast_slow, self.fs.ls(str(thepath)),)

        return to_ret

    def cd(self, path: Optional[str] = None) -> TransparentPath:
        """cd-like command
        
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
        newpath: the absolute TransparentPath we cded to.

        """

        # Will collapse any '..'

        self._update_cache()
        if "gcs" in self.fs_kind and str(path) == self.bucket:
            return TransparentPath(fs=self.fs_kind, bucket=self.bucket, project=self.project)

        # If asked to cd to home, return path script calling directory
        if path == "" or path is None:
            return TransparentPath(fs=self.fs_kind, bucket=self.bucket, project=self.project)
        # If asked for an absolute path
        if path[0] == "/":
            return TransparentPath(path, fs=self.fs_kind, bucket=self.bucket, project=self.project)

        newpath = self / path

        if self.fs_kind == "local":
            # noinspection PyUnresolvedReferences
            if len(newpath.parts) == 0:
                return TransparentPath(newpath, fs=self.fs_kind, bucket=self.bucket, project=self.project)
            # noinspection PyUnresolvedReferences
            if newpath.parts[0] == "..":
                raise ValueError("The first part of a path can not be '..'")
        else:
            # noinspection PyUnresolvedReferences
            if len(newpath.parts) == 1:  # On gcs, first part is bucket
                return TransparentPath(newpath, fs=self.fs_kind, bucket=self.bucket, project=self.project)
            # noinspection PyUnresolvedReferences
            if newpath.parts[1] == "..":
                raise ValueError("Trying to access a path before bucket")

        newpath = collapse_ddots(newpath)

        return TransparentPath(newpath, fs=self.fs_kind, bucket=self.bucket, project=self.project)

    def touch(self, present: str = "ignore", create_parents: bool = True, **kwargs) -> None:
        """Creates the file corresponding to self if does not exist.

        Raises FileExistsError if there already is an object that is not a file at self. Default behavior is to
        create parent directories of the file if needed. This can be canceled by passing 'create_parents=False', but
        only if not using GCS, since directories are not a thing on GCS.


        Parameters
        ----------
        present: str
            What to do if there is already something at self. Can be "raise" or "ignore" (Default value = "ignore")

        create_parents: bool
            If False, raises an error if parent directories are absent. Else, create them. Always True on GCS. (
            Default value = True)

        kwargs
            The kwargs to pass to file system's touch method


        Returns
        -------
        None

        """

        self._update_cache()
        if present != "raise" and present != "ignore":
            raise ValueError(f"Unexpected value for argument 'present' : {present}")

        if present == "raise" and self.exists() and not self.is_file():
            raise FileExistsError(f"There is already an object at {self} which is not a file.")

        if not self.exists():
            for parent in self.parents:
                p = TransparentPath(parent, fs=self.fs_kind, bucket=self.bucket, project=self.project)
                if p.is_file():
                    raise FileExistsError(
                        f"A parent directory can not be created because there is already a file at {p}"
                    )
                # Is True on GCS even if there is no directory, because we
                # do not specify exist=True
                if not p.is_dir():
                    if not create_parents:
                        raise NotADirectoryError(f"Parent directory {p} not found")
                    else:
                        p.mkdir()
        # No need to specify exist=True here, since we know file exists.
        elif self.is_dir():
            raise IsADirectoryError("Can not touch a directory")

        self.fs.touch(self.__fspath__(), **kwargs)
        assert self.is_file()

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

        if present == "raise" and self.exists():
            raise FileExistsError(f"There is already an object at {self}")

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

    def stat(self):
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

        return pd.Series(stat)

    def append(self, other: str) -> TransparentPath:
        return TransparentPath(str(self) + other, fs=self.fs_kind, bucket=self.bucket, project=self.project)

    def put(self, dst: Union[str, Path, TransparentPath]):
        """used to push a local file to the cloud.

        self must be a local TransparentPath. If dst is a TransparentPath, it must be on GCS. If it is a pathlib.Path
        or a str, it will be casted into a GCS TransparentPath, so a gcs file system must have been set up once
        before. """

        if "gcs" not in "".join(TransparentPath.fss):
            raise ValueError("You need to set up a gcs file system before using the put() command.")
        if not self.fs_kind == "local":
            raise ValueError(
                "The calling instance of put() must be local. "
                "To move on gcs a file already on gcs, use mv("
                "). To move a file from gcs, to local, use get("
                "). "
            )
        # noinspection PyUnresolvedReferences
        if type(dst) == TransparentPath and "gcs" not in dst.fs_kind:
            raise ValueError(
                "The second argument can not be a local "
                "TransparentPath. To move a file "
                "localy, use the mv() method."
            )
        if type(dst) != TransparentPath:
            dst = TransparentPath(dst, fs="gcs")
        self.fs.put(self.__fspath__(), dst)

    def get(self, loc: Union[str, Path, TransparentPath]):
        """used to get a remote file to local.

        self must be a gcs TransparentPath. If loc is a TransparentPath, it must be local. If it is a pathlib.Path or
        a str, it will be casted into a local TransparentPath. """

        if "gcs" not in self.fs_kind:
            raise ValueError(
                "The calling instance of get() must be on GCS. To move a file localy, use the mv() method."
            )
        # noinspection PyUnresolvedReferences
        if type(loc) == TransparentPath and loc.fs_kind != "local":
            raise ValueError(
                "The second argument can not be a GCS "
                "TransparentPath. To move on gcs a file already"
                "on gcs, use mv(). To move a file from gcs, to"
                " local, use get()"
            )
        if type(loc) != TransparentPath:
            loc = TransparentPath(loc, fs="local", bucket=self.bucket, project=self.project)
        self.fs.get(self.__fspath__(), loc.__fspath__())

    def mv(self, other: Union[str, Path, TransparentPath]):
        """Used to move two files on the same file system."""

        if not type(other) == TransparentPath:
            other = TransparentPath(other, fs=self.fs_kind, bucket=self.bucket, project=self.project)
        if other.fs_kind != self.fs_kind:
            raise ValueError(
                "mv() can only move two TransparentPath on the "
                "same file system. To get a remote file to "
                "local, use get(). To push a local file to "
                "remote, use put()."
            )
        self.fs.mv(self.__fspath__(), other)

    def exist(self):
        """To prevent typo of 'exist()' without an -s"""
        return self.exists()

    def exists(self):
        self._update_cache()
        return self.fs.exists(self.__fspath__())
