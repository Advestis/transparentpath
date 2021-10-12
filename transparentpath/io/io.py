import builtins
import tempfile
from typing import IO, Union, Any
from pathlib import Path
from ..main.transparentpath import TransparentPath, TPValueError, TPFileExistsError, TPFileNotFoundError


builtins_open = builtins.open


def myopen(*args, **kwargs) -> IO:
    """Method overloading builtins' 'open' method, allowing to open files on GCS using TransparentPath."""
    if len(args) == 0:
        raise TPValueError("open method needs arguments.")
    thefile = args[0]
    if type(thefile) == str and "gs://" in thefile:
        thefile = TransparentPath(thefile)
    if isinstance(thefile, TransparentPath):
        if thefile.when_checked["used"] and not thefile.nocheck:
            # noinspection PyProtectedMember
            thefile._check_multiplicity()
        return thefile.open(*args[1:], **kwargs)
    elif (
        isinstance(thefile, str) or isinstance(thefile, Path) or isinstance(thefile, int) or isinstance(thefile, bytes)
    ):
        return builtins_open(*args, **kwargs)
    else:
        raise TPValueError(f"Unknown type {type(thefile)} for path argument")


def overload_open():
    setattr(builtins, "open", myopen)


def set_old_open():
    setattr(builtins, "open", builtins_open)


def put(self, dst: Union[str, Path, TransparentPath]):
    self.upload(dst)


def upload(self, dst: Union[str, Path, TransparentPath]):
    """
    used to upload a local file (self) to a remote path (dst). Does not remove the local file.

    self must be a local TransparentPath. If dst is a TransparentPath, it must be remote. If it is a pathlib.Path
    or a str, it will be casted into a GCS TransparentPath, so a remote file system must have been set up before.
    """
    if not self.fs_kind == "local":
        raise TPValueError(
            "The calling instance of put() must be local. "
            "To move on remote a file already on remote, use mv("
            "). To move a file from remote, to local, use get("
            "). "
        )
    # noinspection PyUnresolvedReferences
    if type(dst) == TransparentPath and dst.fs_kind == "local":
        raise TPValueError(
            "The second argument can not be a local TransparentPath. To move a file localy, use the mv() method."
        )
    if type(dst) != TransparentPath:

        prefix = ""
        for _fs in TransparentPath.remote_prefix:
            if str(dst).startswith(TransparentPath.remote_prefix[_fs]):
                prefix = TransparentPath.remote_prefix[_fs]
                break

        if prefix == "":
            existing_fs_names = "-".join(list(TransparentPath.fss.keys()))
            if "gcs_" not in existing_fs_names and "s3_" not in existing_fs_names:
                raise TPValueError("No global remote file system set up. Please give put() a TransparentPath or a str"
                                   "starting with the remote file system prefix.")
            if "gcs_" in existing_fs_names and "s3_" in existing_fs_names:
                raise TPValueError("More than one global remote file system set up. Please give put() a TransparentPath"
                                   "  or a str starting with the remote file system prefix.")
            if "gcs_" in existing_fs_names:
                dst = TransparentPath(dst, fs_kind="gcs")
            if "s3_" in existing_fs_names:
                dst = TransparentPath(dst, fs_kind="s3")
        else:
            dst = TransparentPath(dst)

    # noinspection PyProtectedMember
    if not self.exist():
        raise TPFileNotFoundError(f"No such file or directory: {self}")

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
    self.download(loc)


def download(self, loc: Union[str, Path, TransparentPath]):
    """used to download a remote file (self) to local (loc). Does not remove the remote file.

    self must be a remote TransparentPath. If loc is a TransparentPath, it must be local. If it is a pathlib.Path or
    a str, it will be casted into a local TransparentPath. """

    if self.fs_kind == "local":
        raise TPValueError("The calling instance of get() must be remote. To move a file localy, use the mv() method.")
    # noinspection PyUnresolvedReferences
    if type(loc) == TransparentPath and loc.fs_kind != "local":
        raise TPValueError(
            "The second argument can not be a remote "
            "TransparentPath. To move on remote a file already"
            "on remote, use mv(). To move a file from remote, to"
            " local, use get()"
        )
    if type(loc) != TransparentPath:
        loc = TransparentPath(
            loc,
            fs_kind="local",
            notupdatecache=self.notupdatecache,
            nocheck=self.nocheck,
            when_checked=self.when_checked,
            when_updated=self.when_updated,
            update_expire=self.update_expire,
            check_expire=self.check_expire,
        )

    # noinspection PyProtectedMember
    if not self.exist():
        raise TPFileNotFoundError(f"No such file or directory: {self}")

    if self.is_dir(exist=True):
        self.fs.get(self.__fspath__(), loc.__fspath__(), recursive=True)
    else:
        self.fs.get(self.__fspath__(), loc.__fspath__())


def mv(self, other: Union[str, Path, TransparentPath]):
    """
    Moves self to other. Works between any file systems
    """

    self.cp(other)
    self.rm(absent="raise", ignore_kind=True)


def cp(self, other: Union[str, Path, TransparentPath]):
    """
    Copies self to other. Works between any file systems.
    If other is not a TransparentPath, it will be assumed that it is on the same file system than self.
    """

    # noinspection PyProtectedMember
    if not self.exist():
        raise TPFileNotFoundError(f"No such file or directory: {self}")

    if not type(other) == TransparentPath:
        other = TransparentPath(
            other,
            fs_kind=self.fs_kind,
            bucket=self.bucket,
            notupdatecache=self.notupdatecache,
            nocheck=self.nocheck,
            when_checked=self.when_checked,
            when_updated=self.when_updated,
            update_expire=self.update_expire,
            check_expire=self.check_expire,
            token=self.token,
        )

    if self.fs_kind == "local":
        self.upload(other)  # upload self in other
        return
    if other.fs_kind == "local":
        self.download(other)  # download self in other
        return

    # Do not use filesystem's copy if self is not a file, for it was coded by apes and is not able to use recursive
    # properly

    if self.is_file():
        if self.fs_name == other.fs_name:
            self.fs.cp(self.__fspath__(), other)
            return
        else:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=self.suffix)
            tmp.close()  # deletes the tmp file
            self.download(tmp.name)  # downloads self at tmp's address
            TransparentPath(tmp.name, fs_kind="local").upload(other)
            return

    for stuff in list(self.glob("**/*", fast=True)):
        if not stuff.is_file() and not stuff.is_dir():
            continue
        # noinspection PyUnresolvedReferences
        relative = stuff.split(f"/{self.name}/")[-1]
        newpath = other / relative
        newpath.parent.mkdir(recursive=True)
        # noinspection PyUnresolvedReferences
        stuff.cp(newpath)


def read_text(self, *args, get_obj: bool = False, **kwargs) -> Union[str, IO]:
    if not self.is_file():
        raise TPFileNotFoundError(f"Could not find file {self}")

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


def write_stuff(self, data: Any, *args, overwrite: bool = True, present: str = "ignore", **kwargs) -> None:

    if not overwrite and self.is_file() and present != "ignore":
        raise TPFileExistsError()

    args = list(args)
    if len(args) == 0:
        args = ("w",)

    with self.open(*args, **kwargs) as f:
        f.write(data)


def write_bytes(self, data: Any, *args, overwrite: bool = True, present: str = "ignore", **kwargs,) -> None:

    args = list(args)
    if len(args) == 0:
        args = ("wb",)
    if "b" not in args[0]:
        args[0] += "b"

    self.write_stuff(data, *tuple(args), overwrite=overwrite, present=present, **kwargs)
