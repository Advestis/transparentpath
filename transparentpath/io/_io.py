import builtins
import tempfile
from pathlib import Path
from typing import IO, Union, Any

from ..gcsutils.transparentpath import TransparentPath

builtins_open = builtins.open


def myopen(*args, **kwargs) -> IO:
    """Method overloading builtins' 'open' method, allowing to open files on GCS and scaleway and by ssh using
    TransparentPath. """
    if len(args) == 0:
        raise ValueError("open method needs arguments.")
    thefile = args[0]
    if type(thefile) == str and ("gs://" in thefile or "s3://" in thefile):
        thefile = TransparentPath(thefile)
    if isinstance(thefile, TransparentPath):
        if thefile.when_checked["used"] and not thefile.nocheck:
            # noinspection PyProtectedMember
            thefile._check_multiplicity()
        return thefile.open(*args[1:], **kwargs)
    elif (
            isinstance(thefile, str) or isinstance(thefile, Path) or isinstance(thefile, int) or isinstance(thefile,
                                                                                                            bytes)
    ):
        return builtins_open(*args, **kwargs)
    else:
        raise ValueError(f"Unknown type {type(thefile)} for path argument")


def overload_open():
    setattr(builtins, "open", myopen)


def set_old_open():
    setattr(builtins, "open", builtins_open)


def put(self, dst: Union[str, Path, TransparentPath]):
    """used to push a local file to the cloud. Does not remove the local file.

    self must be a local TransparentPath. If dst is a TransparentPath, it must be on remote. If it is a pathlib.Path
    or a str, it will be casted into a GCS TransparentPath, so a remote file system must have been set up before. """

    if not self.fs_kind == "local":
        raise ValueError(
            "The calling instance of put() must be local. "
            "To move on remote a file already on remote, use mv("
            "). To move a file from remote, to local, use get("
            "). "
        )
    # noinspection PyUnresolvedReferences
    if type(dst) == TransparentPath and dst.fs_kind == "local":
        raise ValueError(
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
                raise ValueError(
                    "No global remote file system set up. Please give put() a TransparentPath or a str"
                    " starting with the remote file system prefix."
                )
            if "gcs_" in existing_fs_names and "s3_" in existing_fs_names:
                raise ValueError(
                    "More than one global remote file system set up. Please give put() a TransparentPath"
                    " or a str starting with the remote file system prefix."
                )
            if "gcs_" in existing_fs_names:
                dst = TransparentPath(dst, fs_kind="gcs")
            if "s3_" in existing_fs_names:
                dst = TransparentPath(dst, fs_kind="scw")
        else:
            dst = TransparentPath(dst)

    # noinspection PyProtectedMember
    if not self.exist():
        raise FileNotFoundError(f"No such file or directory: {self}")
    existing_fs_names = "-".join(list(TransparentPath.fss.keys()))
    print(existing_fs_names)
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
    """used to get a remote file (self) to local (loc). Does not remove the remote file.

    self must be a remote TransparentPath. If loc is a TransparentPath, it must be local. If it is a pathlib.Path or
    a str, it will be casted into a local TransparentPath. """

    def recursive(source, destination):
        if not source.exists():
            raise FileNotFoundError(f"Element {source} does not exist")
        if source.isfile():
            source.get(destination)
        elif source.isdir():
            for element in source.glob("*", fast=True):
                recursive(element, destination / element.name)
        else:
            raise ValueError(f"Element {source} exists is neither a file nor a dir, then what is it ?")

    if self.fs_kind == "local":
        raise ValueError("The calling instance of get() must be remote. To move a file localy, use the mv() method.")
    # noinspection PyUnresolvedReferences
    if type(loc) == TransparentPath and loc.fs_kind != "local":
        raise ValueError(
            "The second argument can not be a remote "
            "TransparentPath. To move on remote a file already"
            "on remote, use mv(). To move a file from remote, to"
            " local, use get()"
        )
    if type(loc) != TransparentPath:
        loc = TransparentPath(
            loc,
            fs="local",
            notupdatecache=self.notupdatecache,
            nocheck=self.nocheck,
            when_checked=self.when_checked,
            when_updated=self.when_updated,
            update_expire=self.update_expire,
            check_expire=self.check_expire,
        )

    # noinspection PyProtectedMember
    if not self.exist():
        raise FileNotFoundError(f"No such file or directory: {self}")

    if self.is_dir(exist=True):
        # Recursive fs.get does not find all existing elements, it seems, so overload it
        recursive(self, loc)
    else:
        self.fs.get(self.__fspath__(), loc.__fspath__())


def mv(self, other: Union[str, Path, TransparentPath]):
    """Used to move a file or a directory. Works between any filesystems."""

    if not type(other) == TransparentPath:
        other = TransparentPath(
            other,
            fs=self.fs_kind,
            bucket=self.bucket,
            notupdatecache=self.notupdatecache,
            nocheck=self.nocheck,
            when_checked=self.when_checked,
            when_updated=self.when_updated,
            update_expire=self.update_expire,
            check_expire=self.check_expire,
            token=self.token,
        )

    if other.fs_kind != self.fs_kind:
        # TODO : adapt to s3 -> Isn't it already adapted ? To be tested
        if self.fs_kind == "local":
            self.put(other)
            self.rm(absent="raise", ignore_kind=True)
        else:
            self.get(other)
            self.rm(absent="raise", ignore_kind=True)
        return

    # Do not use filesystem's move, for it is coded by apes and is not able to use recursive properly
    # self.fs.mv(self.__fspath__(), other, **kwargs)

    # noinspection PyProtectedMember
    if not self.exist():
        raise FileNotFoundError(f"No such file or directory: {self}")

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

    # noinspection PyProtectedMember
    if not self.exist():
        raise FileNotFoundError(f"No such file or directory: {self}")

    if not type(other) == TransparentPath:
        other = TransparentPath(
            other,
            fs=self.fs_kind,
            bucket=self.bucket,
            notupdatecache=self.notupdatecache,
            nocheck=self.nocheck,
            when_checked=self.when_checked,
            when_updated=self.when_updated,
            update_expire=self.update_expire,
            check_expire=self.check_expire,
            token=self.token,
        )
    if other.fs_kind != self.fs_kind:
        if self.fs_kind == "local":
            self.put(other)
        else:
            self.get(other)
        return

    # Do not use filesystem's copy if self is not a file, for it was coded by apes and is not able to use recursive
    # properly

    if self.is_file():
        if self.fs_kind == other.fs_kind:
            self.fs.copy(self.__fspath__(), other)
            return
        else:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=self.suffix)
            tmp.close()  # deletes the tmp file
            self.download_object(tmp.name)  # downloads self at tmp's address
            TransparentPath(tmp.name, fs_kind="local").upload(other)
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


def read_text(self, *args, size: int = -1, get_obj: bool = False, **kwargs) -> Union[str, IO]:
    if not self.is_file():
        raise FileNotFoundError(f"Could not find file {self}")

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
        to_ret = f.read(size)
        if not byte_mode:
            to_ret = to_ret.decode()
    return to_ret


def write_stuff(self, data: Any, *args, overwrite: bool = True, present: str = "ignore", **kwargs) -> None:
    if not overwrite and self.is_file() and present != "ignore":
        raise FileExistsError()

    args = list(args)
    if len(args) == 0:
        args = ("w",)

    with self.open(*args, **kwargs) as f:
        f.write(data)


def write_bytes(self, data: Any, *args, overwrite: bool = True, present: str = "ignore", **kwargs, ) -> None:
    args = list(args)
    if len(args) == 0:
        args = ("wb",)
    if "b" not in args[0]:
        args[0] += "b"

    self.write_stuff(data, *tuple(args), overwrite=overwrite, present=present, **kwargs)
