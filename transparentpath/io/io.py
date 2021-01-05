import builtins
from typing import IO, Union, Any
from pathlib import Path
from ..gcsutils.transparentpath import TransparentPath


builtins_open = builtins.open


def myopen(*args, **kwargs) -> IO:
    """Method overloading builtins' 'open' method, allowing to open files on GCS using TransparentPath."""
    if len(args) == 0:
        raise ValueError("open method needs arguments.")
    thefile = args[0]
    if type(thefile) == str and "gs://" in thefile:
        if "gcs" not in TransparentPath.fss:
            raise OSError("You are trying to access a file on GCS without having set a proper GCSFileSystem first.")
        thefile = TransparentPath(thefile.replace(f"gs://{TransparentPath.bucket}", ""), fs="gcs")
    if isinstance(thefile, TransparentPath):
        return thefile.open(*args[1:], **kwargs)
    elif (
        isinstance(thefile, str) or isinstance(thefile, Path) or isinstance(thefile, int) or isinstance(thefile, bytes)
    ):
        return builtins_open(*args, **kwargs)
    else:
        raise ValueError(f"Unknown type {type(thefile)} for path argument")


setattr(builtins, "open", myopen)


def overload_open():
    setattr(builtins, "open", myopen)


def set_old_open():
    setattr(builtins, "open", builtins_open)


def put(self, dst: Union[str, Path, TransparentPath]):
    """used to push a local file to the cloud. Does not remove the local file.

    self must be a local TransparentPath. If dst is a TransparentPath, it must be on GCS. If it is a pathlib.Path
    or a str, it will be casted into a GCS TransparentPath, so a gcs file system must have been set up before. """

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
            "The second argument can not be a local " "TransparentPath. To move a file " "localy, use the mv() method."
        )
    if type(dst) != TransparentPath:
        dst = TransparentPath(dst, fs="gcs")
    if self.is_dir():
        for item in self.glob("/*"):
            # noinspection PyUnresolvedReferences
            item.put(dst / item.name)
    else:
        with open(self, "rb") as f1:
            with open(dst, "wb") as f2:
                data = True
                while data:
                    data = f1.read(self.blocksize)
                    f2.write(data)


def get(self, loc: Union[str, Path, TransparentPath]):
    """used to get a remote file to local. Does not remove the remote file.

    self must be a remote TransparentPath. If loc is a TransparentPath, it must be local. If it is a pathlib.Path or
    a str, it will be casted into a local TransparentPath. """

    if "gcs" not in self.fs_kind:
        raise ValueError("The calling instance of get() must be on GCS. To move a file localy, use the mv() method.")
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
    """Used to move a file or a directory. Works between any filesystems."""

    if not type(other) == TransparentPath:
        other = TransparentPath(other, fs=self.fs_kind, bucket=self.bucket, project=self.project)

    if other.fs_kind != self.fs_kind:
        if self.fs_kind == "local":
            self.put(other)
            self.rm(absent="raise", ignore_kind=True)
        else:
            self.get(other)
            self.rm(absent="raise", ignore_kind=True)
        return

    # Do not use filesystem's move, for it is coded by apes and is not able to use recursive properly
    # self.fs.mv(self.__fspath__(), other, **kwargs)

    if self.is_file():
        self.fs.mv(self.__fspath__(), other)
        return

    for stuff in list(self.glob("**/*", fast=True)):
        # noinspection PyUnresolvedReferences
        if not stuff.is_file():
            continue
        # noinspection PyUnresolvedReferences
        relative = stuff.split(f"/{self.name}/")[-1]
        newpath = other / relative
        newpath.parent.mkdir(recursive=True)
        self.fs.mv(stuff.__fspath__(), newpath)


def cp(self, other: Union[str, Path, TransparentPath]):
    """Used to copy a file or a directory on the same filesystem."""

    if not type(other) == TransparentPath:
        other = TransparentPath(other, fs=self.fs_kind, bucket=self.bucket, project=self.project)
    if other.fs_kind != self.fs_kind:
        if self.fs_kind == "local":
            self.put(other)
        else:
            self.get(other)
        return

    # Do not use filesystem's copy if self is not a file, for it was coded by apes and is not able to use recursive
    # properly

    if self.is_file():
        self.fs.cp(self.__fspath__(), other)
        return

    for stuff in list(self.glob("**/*", fast=True)):
        # noinspection PyUnresolvedReferences
        if not stuff.is_file():
            continue
        # noinspection PyUnresolvedReferences
        relative = stuff.split(f"/{self.name}/")[-1]
        newpath = other / relative
        newpath.parent.mkdir(recursive=True)
        self.fs.cp(stuff.__fspath__(), newpath)


def read_text(self, *args, get_obj: bool = False, update_cache: bool = True, **kwargs) -> Union[str, IO]:
    # noinspection PyProtectedMember
    if update_cache and self.__class__._do_update_cache:
        self._update_cache()

    byte_mode = True
    if len(args) == 0:
        byte_mode = False
        args = ("rb",)
    if "b" not in args[0]:
        byte_mode = False
        args[0] += "b"
    if get_obj:
        return self.open(*args, **kwargs)

    with self.open(*args, **kwargs) as f:
        to_ret = f.read()
        if not byte_mode:
            to_ret = to_ret.decode()
    return to_ret


def write_stuff(
    self, data: Any, *args, overwrite: bool = True, present: str = "ignore", update_cache: bool = True, **kwargs
) -> None:
    # noinspection PyProtectedMember
    if update_cache and self.__class__._do_update_cache:
        self._update_cache()
    if not overwrite and self.is_file() and present != "ignore":
        raise FileExistsError()

    args = list(args)
    if len(args) == 0:
        args = ("w",)

    with self.open(*args, **kwargs) as f:
        f.write(data)


def write_bytes(
    self, data: Any, *args, overwrite: bool = True, present: str = "ignore", update_cache: bool = True, **kwargs,
) -> None:

    args = list(args)
    if len(args) == 0:
        args = ("wb",)
    if "b" not in args[0]:
        args[0] += "b"

    self.write_stuff(data, *tuple(args), overwrite=overwrite, present=present, update_cache=update_cache, **kwargs)