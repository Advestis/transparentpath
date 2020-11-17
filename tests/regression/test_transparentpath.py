import gcsfs
import pandas as pd
import numpy as np
import dask.dataframe as dd
import os
from pathlib import Path
from fsspec.implementations.local import LocalFileSystem
from transparentpath import TransparentPath

project = "sandbox-281209"
bucket = "code_tests_sand"

df_csv = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[1, 2], [3, 4]])
dd_csv = dd.from_pandas(df_csv, npartitions=1)
df_parquet = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[5, 6], [7, 8]])
dd_parquet = dd.from_pandas(df_parquet, npartitions=1)
df_hdf5 = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[9, 10], [11, 12]])
dd_hdf5 = dd.from_pandas(df_hdf5, npartitions=1)
arr_hdf5 = np.array([[1, 2], [3, 4]])
df_excel = pd.DataFrame(columns=["foo", "bar"], index=["a", "b"], data=[[13, 14], [15, 16]])
dd_excel = dd.from_pandas(df_excel, npartitions=1)
s = "foo\nbar\ndog"


# noinspection PyMethodMayBeStatic
class Init(object):
    def reinit(self):
        TransparentPath.fss = {}
        TransparentPath.unset = True
        TransparentPath.fs_kind = ""
        TransparentPath.project = None
        TransparentPath.bucket = None

    def before_init(self):
        assert TransparentPath.fss == {}
        assert TransparentPath.unset
        assert TransparentPath.fs_kind == ""
        assert TransparentPath.project is None
        assert TransparentPath.bucket is None

    def init_gcs_fail(self):
        self.before_init()
        rose = False
        try:
            TransparentPath.set_global_fs("gcs")
        except ValueError as e:
            if "Need to provide a bucket name!" in str(e) or "Need to provide a project name!" in str(e):
                rose = True
        assert rose
        self.reinit()

    def init_gcs_success(self, reinit=True):
        self.before_init()
        TransparentPath.set_global_fs("gcs", project=project, bucket=bucket)
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 1
        assert TransparentPath.fs_kind == "gcs"
        assert list(TransparentPath.fss.keys())[0] == "gcs"
        assert isinstance(TransparentPath.fss["gcs"], gcsfs.GCSFileSystem)
        if reinit:
            self.reinit()

    def init_gcs_path_after_class(self):
        self.init_gcs_success(reinit=False)
        p = TransparentPath("chien")
        assert str(p.path) == f"{bucket}/chien"
        assert str(p) == f"gs://{bucket}/chien"
        assert p.__fspath__() == f"gs://{bucket}/chien"

        assert p.fs_kind == "gcs"
        assert p.fs == TransparentPath.fss["gcs"]
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 1
        assert TransparentPath.fs_kind == "gcs"
        assert list(TransparentPath.fss.keys())[0] == "gcs"
        assert isinstance(TransparentPath.fss["gcs"], gcsfs.GCSFileSystem)
        p2 = p / ".."
        assert str(p2) == f"gs://{p2.bucket}"
        p2 = TransparentPath()
        assert str(p2) == f"gs://{p2.bucket}"
        p2 = TransparentPath("/")
        assert str(p2) == f"gs://{p2.bucket}"
        self.reinit()

        self.init_gcs_success(reinit=False)
        failed = False
        try:
            TransparentPath(f"gs://{bucket + 'chat'}/chien")
        except ValueError:
            failed = True
        assert failed

        failed = False
        try:
            TransparentPath(f"gs://{bucket}/chien", fs="local")
        except ValueError:
            failed = True
        assert failed

        TransparentPath(f"gs://{bucket}/chien")
        assert str(p.path) == f"{bucket}/chien"
        assert str(p) == f"gs://{bucket}/chien"
        assert p.__fspath__() == f"gs://{bucket}/chien"

        assert p.fs_kind == "gcs"
        assert p.fs == TransparentPath.fss["gcs"]
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 1
        assert TransparentPath.fs_kind == "gcs"
        assert list(TransparentPath.fss.keys())[0] == "gcs"
        assert isinstance(TransparentPath.fss["gcs"], gcsfs.GCSFileSystem)
        self.reinit()

    def init_local(self, reinit=True):
        try:
            self.before_init()
        except Exception as e:
            if reinit:
                self.reinit()
                self.before_init()
            else:
                raise e
        TransparentPath.set_global_fs("local")
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 1
        assert TransparentPath.fs_kind == "local"
        assert list(TransparentPath.fss.keys())[0] == "local"
        assert isinstance(TransparentPath.fss["local"], LocalFileSystem)
        if reinit:
            self.reinit()

    def init_local_path_after_class(self):
        self.init_local(reinit=False)
        p = TransparentPath("chien")
        assert str(p.path) == f"{os.getcwd()}/chien"
        assert str(p) == f"{os.getcwd()}/chien"
        assert p.__fspath__() == f"{os.getcwd()}/chien"

        assert p.fs_kind == "local"
        assert p.fs == TransparentPath.fss["local"]
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 1
        assert TransparentPath.fs_kind == "local"
        assert list(TransparentPath.fss.keys())[0] == "local"
        assert isinstance(TransparentPath.fss["local"], LocalFileSystem)
        self.reinit()

    def init_gcs_path_before_class_fail(self):
        self.before_init()
        rose = False
        try:
            TransparentPath("chien", fs="gcs")
        except ValueError as e:
            if "bucket" in str(e) or "project" in str(e):
                rose = True
        assert rose
        self.reinit()

    def init_gcs_path_before_class_success(self):
        self.before_init()
        p = TransparentPath("chien", fs="gcs", project=project, bucket=bucket)
        assert str(p.path) == f"{bucket}/chien"
        assert str(p) == f"gs://{bucket}/chien"
        assert p.__fspath__() == f"gs://{bucket}/chien"

        assert p.fs_kind == "gcs"
        assert p.fs == TransparentPath.fss["gcs"]
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 1
        assert TransparentPath.fs_kind == "gcs"
        assert list(TransparentPath.fss.keys())[0] == "gcs"
        assert isinstance(TransparentPath.fss["gcs"], gcsfs.GCSFileSystem)
        self.reinit()

        self.before_init()
        failed = False
        try:
            TransparentPath(f"gs://{bucket}/chien", project=project, bucket=bucket + "chien")
        except ValueError:
            failed = True
        assert failed

        failed = False
        try:
            TransparentPath(f"gs://{bucket}/chien", project=project, fs="local", bucket=bucket)
        except ValueError:
            failed = True
        assert failed

        failed = False
        try:
            TransparentPath(f"gs://{bucket}/chien")
        except ValueError:
            failed = True
        assert failed

        p = TransparentPath(f"gs://{bucket}/chien", project=project)
        assert str(p.path) == f"{bucket}/chien"
        assert str(p) == f"gs://{bucket}/chien"
        assert p.__fspath__() == f"gs://{bucket}/chien"

        assert p.fs_kind == "gcs"
        assert p.fs == TransparentPath.fss["gcs"]
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 1
        assert TransparentPath.fs_kind == "gcs"
        assert list(TransparentPath.fss.keys())[0] == "gcs"
        assert isinstance(TransparentPath.fss["gcs"], gcsfs.GCSFileSystem)
        self.reinit()

    def init_local_path_before_class(self):
        self.before_init()
        p = TransparentPath("chien", fs="local")
        assert str(p.path) == f"{os.getcwd()}/chien"
        assert str(p) == f"{os.getcwd()}/chien"
        assert p.__fspath__() == f"{os.getcwd()}/chien"

        assert p.fs_kind == "local"
        assert p.fs == TransparentPath.fss["local"]
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 1
        assert TransparentPath.fs_kind == "local"
        assert list(TransparentPath.fss.keys())[0] == "local"
        assert isinstance(TransparentPath.fss["local"], LocalFileSystem)
        self.reinit()

    def init_local_class_then_gcs_path(self):
        self.before_init()
        self.init_local(False)
        p = TransparentPath("chien", fs="gcs", project=project, bucket=bucket)
        assert str(p.path) == f"{bucket}/chien"
        assert str(p) == f"gs://{bucket}/chien"
        assert p.__fspath__() == f"gs://{bucket}/chien"

        assert p.fs_kind == "gcs"
        assert p.fs == TransparentPath.fss["gcs"]
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 2
        assert TransparentPath.fs_kind == "local"
        assert "gcs" in list(TransparentPath.fss.keys()) and "local" in list(TransparentPath.fss.keys())
        assert isinstance(TransparentPath.fss["gcs"], gcsfs.GCSFileSystem)
        assert isinstance(TransparentPath.fss["local"], LocalFileSystem)
        self.reinit()

        self.before_init()
        self.init_local(False)
        failed = False
        try:
            TransparentPath(f"gs://{bucket}/chien", project=project, bucket=bucket + "chien")
        except ValueError:
            failed = True
        assert failed

        failed = False
        try:
            TransparentPath(f"gs://{bucket}/chien", project=project, fs="local", bucket=bucket)
        except ValueError:
            failed = True
        assert failed

        failed = False
        try:
            TransparentPath(f"gs://{bucket}/chien")
        except ValueError:
            failed = True
        assert failed

        p = TransparentPath(f"gs://{bucket}/chien", project=project)
        assert str(p.path) == f"{bucket}/chien"
        assert str(p) == f"gs://{bucket}/chien"
        assert p.__fspath__() == f"gs://{bucket}/chien"

        assert p.fs_kind == "gcs"
        assert p.fs == TransparentPath.fss["gcs"]
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 2
        assert TransparentPath.fs_kind == "local"
        assert "gcs" in list(TransparentPath.fss.keys()) and "local" in list(TransparentPath.fss.keys())
        assert isinstance(TransparentPath.fss["gcs"], gcsfs.GCSFileSystem)
        assert isinstance(TransparentPath.fss["local"], LocalFileSystem)
        self.reinit()

    def init_gcs_class_then_local_path(self):
        self.before_init()
        self.init_gcs_success(False)
        p = TransparentPath("chien", fs="local")
        assert str(p.path) == f"{os.getcwd()}/chien"
        assert str(p) == f"{os.getcwd()}/chien"
        assert p.__fspath__() == f"{os.getcwd()}/chien"

        assert p.fs_kind == "local"
        assert p.fs == TransparentPath.fss["local"]
        assert not TransparentPath.unset
        assert len(TransparentPath.fss) == 2
        assert TransparentPath.fs_kind == "gcs"
        assert "gcs" in list(TransparentPath.fss.keys()) and "local" in list(TransparentPath.fss.keys())
        assert isinstance(TransparentPath.fss["gcs"], gcsfs.GCSFileSystem)
        assert isinstance(TransparentPath.fss["local"], LocalFileSystem)
        self.reinit()


class Checks:
    def __init__(self):
        self.p = TransparentPath("chien/chat")
        assert not isinstance(self.p.path, str)
        assert isinstance(self.p, str)
        assert type(self.p) != str

    def raise_missing_attr(self):
        rose = False
        try:
            self.p.onecyltbgfhal
        except AttributeError:
            rose = True
        assert rose

    def parent(self):
        assert type(self.p.parent) == type(self.p)  # noqa: E721

    def is_dir(self):
        self.p.rm(absent="ignore", ignore_kind=True)
        assert not self.p.exists()
        if self.p.fs_kind == "gcs":
            assert self.p.is_dir()
            assert not self.p.is_dir(exist=True)
        else:
            assert not self.p.is_dir()
        (self.p / "cheval").touch()
        assert self.p.exists()
        assert self.p.is_dir()
        assert self.p.is_dir(exist=True)
        self.p.rm(absent="ignore", ignore_kind=True)
        (self.p / "cheval").rm(absent="ignore", ignore_kind=True)

    def is_file(self):
        self.p.rm(absent="ignore", ignore_kind=True)
        assert not self.p.exists()
        assert not self.p.is_file()
        self.p.touch()
        assert self.p.exists()
        assert self.p.is_file()
        assert not self.p.is_dir()
        assert not self.p.is_dir(exist=True)
        self.p.rm(absent="ignore", ignore_kind=True)

    def duplicate_instanciation(self):
        self.p.rm(absent="ignore", ignore_kind=True)
        self.p.touch()
        rose = False
        try:
            (self.p / "truc").touch()
        except FileExistsError:
            rose = True
        assert rose
        assert not (self.p / "truc").is_file()
        self.p.rm(absent="ignore", ignore_kind=True)

    def duplicate_method(self):
        self.p.rm(absent="ignore", ignore_kind=True)
        (self.p / "truc").touch()
        rose = False
        try:
            self.p.touch()
        except IsADirectoryError:
            rose = True
        assert rose
        self.p.rm(absent="ignore", ignore_kind=True)
        (self.p / "truc").rm(absent="ignore", ignore_kind=True)

    def get_put_mv(self):
        if "gcs" not in TransparentPath.fss:
            TransparentPath.set_global_fs(
                "gcs", project=project, bucket=bucket, make_main=False,
            )
        self.p.rm(absent="ignore", ignore_kind=True)
        self.p.touch()
        if self.p.fs_kind == "gcs":
            self.p.get("zob")
            assert Path("zob").is_file()
            Path("zob").unlink()
        else:
            self.p.put("zob")
            rp = TransparentPath("zob", fs="gcs")
            assert rp.is_file()
            rp.rm()

        self.p.mv("zob")
        rp = self.p.cd() / "zob"
        assert rp.is_file()
        rp.rm()


# noinspection PyMethodMayBeStatic
class Write:
    def write_csv(self):
        pcsv = TransparentPath("chien/chat.csv")
        pcsv.rm(absent="ignore", ignore_kind=True)
        assert not pcsv.is_file()
        pcsv.write(df_csv)
        assert pcsv.is_file()

    def write_excel(self):
        pexcel = TransparentPath("chien/chat.xlsx")
        pexcel.rm(absent="ignore", ignore_kind=True)
        assert not pexcel.is_file()
        pexcel.write(df_excel)
        assert pexcel.is_file()

    def write_parquet(self):
        pparquet = TransparentPath("chien/chat.parquet")
        pparquet.rm(absent="ignore", ignore_kind=True)
        assert not pparquet.is_file()
        pparquet.write(df_parquet)
        assert pparquet.is_file()

    def write_hdf5(self):
        phdf5 = TransparentPath("chien/chat.hdf5")
        phdf5.rm(absent="ignore", ignore_kind=True)
        assert not phdf5.is_file()
        with phdf5.write(None, use_pandas=True) as f:
            f["data"] = df_hdf5
        assert phdf5.is_file()

        phdf5 = TransparentPath("chien/chat2.hdf5")
        phdf5.rm(absent="ignore", ignore_kind=True)
        assert not phdf5.is_file()
        # noinspection PyTypeChecker
        phdf5.write({"df1": df_hdf5, "df2": 2 * df_hdf5}, use_pandas=True)
        assert phdf5.is_file()

        phdf5 = TransparentPath("chien/chat3.hdf5")
        phdf5.rm(absent="ignore", ignore_kind=True)
        assert not phdf5.is_file()
        # noinspection PyTypeChecker
        with phdf5.write(None) as f:
            f["data"] = arr_hdf5
        assert phdf5.is_file()

    def write_txt(self):
        ptxt = TransparentPath("chien/chat.txt")
        ptxt.rm(absent="ignore", ignore_kind=True)
        assert not ptxt.is_file()
        ptxt.write(s)
        assert ptxt.is_file()

    def cust_open(self):
        ptxt = TransparentPath("chien/chat2.txt")
        ptxt.rm(absent="ignore", ignore_kind=True)
        assert not ptxt.is_file()
        with open(ptxt, "w") as ofile:
            ofile.write("zob")
        assert ptxt.is_file()
        ptxt.rm()

    def write_dask(self):
        def dask_csv():
            pcsv = TransparentPath("chien/chat_dask.csv")
            pcsv.rm(absent="ignore", ignore_kind=True)
            assert not pcsv.is_file()
            pcsv.write(dd_csv)
            assert pcsv.is_file()

        def dask_parquet():
            pparquet = TransparentPath("chien/chat_dask.parquet")
            pparquet.rm(absent="ignore", ignore_kind=True)
            pparquet.with_suffix("").rm(absent="ignore", ignore_kind=True, recursive=True)
            assert not pparquet.is_file()
            pparquet.write(dd_parquet)
            assert pparquet.with_suffix("").is_dir(exist=True)

        def dask_hdf5():
            phdf5 = TransparentPath("chien/chat_dask.hdf5")
            phdf5.rm(absent="ignore", ignore_kind=True)
            assert not phdf5.is_file()
            phdf5.write(dd_hdf5, set_name="data")
            assert phdf5.is_file()

        def dask_hdf5_2():
            phdf5_2 = TransparentPath("chien/chat_dask_2.hdf5")
            phdf5_2.rm(absent="ignore", ignore_kind=True)
            assert not phdf5_2.is_file()
            try:
                phdf5_2.write(dd_hdf5, set_name="data", use_pandas=True)
                assert phdf5_2.is_file()
            except NotImplementedError:
                pass

        def dask_excel():
            pexcel = TransparentPath("chien/chat_dask.xlsx")
            pexcel.rm(absent="ignore", ignore_kind=True)
            assert not pexcel.is_file()
            pexcel.write(dd_excel)
            assert pexcel.is_file()

        dask_csv()
        print("      ...write_dask_csv tested")
        dask_parquet()
        print("      ...write_dask_parquet tested")
        dask_hdf5()
        print("      ...write_dask_hdf5 tested")
        dask_hdf5_2()
        print("      ...write_dask_hdf5_2 tested")
        dask_excel()
        print("      ...write_dask_excel tested")


# noinspection PyMethodMayBeStatic
class Read:
    def read_csv(self):
        pcsv = TransparentPath("chien/chat.csv")
        df2 = pcsv.read(index_col=0)
        pd.testing.assert_frame_equal(df_csv, df2)
        pcsv.rm()

    def read_excel(self):
        pexcel = TransparentPath("chien/chat.xlsx")
        df2 = pexcel.read(index_col=0)
        pd.testing.assert_frame_equal(df_excel, df2)
        pexcel.rm()

    def read_parquet(self):
        pparquet = TransparentPath("chien/chat.parquet")
        df2 = pparquet.read()
        pd.testing.assert_frame_equal(df_parquet, df2)
        pparquet.rm()

    def read_hdf5(self):
        phdf5 = TransparentPath("chien/chat.hdf5")
        with phdf5.read(use_pandas=True) as f:
            df2 = f["data"]
        pd.testing.assert_frame_equal(df_hdf5, df2)
        phdf5.rm()

        phdf5 = TransparentPath("chien/chat2.hdf5")
        with phdf5.read(use_pandas=True) as f:
            pd.testing.assert_frame_equal(df_hdf5, f["df1"])
            # noinspection PyTypeChecker
            pd.testing.assert_frame_equal(df_hdf5 * 2, f["df2"])
        phdf5.rm()

        phdf5 = TransparentPath("chien/chat3.hdf5")
        with phdf5.read() as f:
            np.testing.assert_equal(arr_hdf5, np.array(f["data"]))
        phdf5.rm()

    def read_txt(self):
        ptxt = TransparentPath("chien/chat.txt")
        s2 = ptxt.read()
        assert s2 == s
        ptxt.rm()

    def read_dask(self):
        def dask_csv():
            pcsv = TransparentPath("chien/chat_dask.csv")
            df2 = pcsv.read(use_dask=True, index_col=0)
            dd.utils.assert_eq(dd_csv, df2)
            pcsv.rm()

        def dask_excel():
            pexcel = TransparentPath("chien/chat_dask.xlsx")
            df2 = pexcel.read(use_dask=True, index_col=0)
            # Can't use dd.utils.assert_eq, for partitions will be different (None when read from .xlsx file)
            pd.testing.assert_frame_equal(dd_excel.head(), df2.head())
            pexcel.rm()

        def dask_parquet():
            pparquet = TransparentPath("chien/chat_dask.parquet")
            df2 = pparquet.read(use_dask=True)
            dd.utils.assert_eq(dd_parquet, df2)
            pparquet.with_suffix("").rm(recursive=True)

        def dask_hdf5():
            phdf5 = TransparentPath("chien/chat_dask.hdf5")
            df2 = phdf5.read(phdf5, set_names="data", use_dask=True)
            # with phdf5.read() as f:
            #     df2 = dd.from_pandas(pd.DataFrame(f["data"]), npartitions=2)
            # Can't use dd.utils.assert_eq, for partitions will be different (None when read from hdf5 file)
            pd.testing.assert_frame_equal(dd_hdf5.head(), df2.head())
            phdf5.rm()

        # def dask_hdf5_2():
        #     phdf5_2 = TransparentPath("chien/chat_2_dask.hdf5")
        #     with phdf5_2.read(use_pandas=True) as f:
        #         df2 = f["data"]
        #     pd.testing.assert_frame_equal(dd_hdf5, df2)
        #     phdf5_2.rm()

        dask_csv()
        print("      ...read_dask_csv tested")
        dask_parquet()
        print("      ...read_dask_parquet tested")
        dask_hdf5()
        print("      ...read_dask_hdf5 tested")
        # dask_hdf5_2()
        # print("    ...read_dask_hdf5_2 tested")
        dask_excel()
        print("      ...read_dask_excel tested")


class Methods:
    def __init__(self):
        self.p = TransparentPath("chien/chat/cheval")

    def cd(self):
        self.p.rm(absent="ignore", ignore_kind=True)
        if self.p.fs_kind == "gcs":
            assert TransparentPath("/") == TransparentPath()
            assert str(TransparentPath("/")) == f"gs://{self.p.bucket}"
            assert str(TransparentPath()) == f"gs://{self.p.bucket}"
            assert str(self.p.cd("/")) == f"gs://{self.p.bucket}"
            assert str(self.p.cd(self.p.bucket)) == f"gs://{self.p.bucket}"
            assert self.p.cd("/chien") == TransparentPath("chien")

        assert self.p.cd("../../../") == TransparentPath()
        assert self.p.cd("..") == TransparentPath("chien/chat/")
        assert self.p.cd(".") == self.p

    def ls(self):
        self.p.rm(absent="ignore", ignore_kind=True)
        (self.p / "f1").touch()
        (self.p / "f2").touch()
        (self.p / "d3").touch()
        ls_res = list(self.p.ls())
        ls_res.sort()
        expectation = [
            TransparentPath(self.p / "f1"),
            TransparentPath(self.p / "f2"),
            TransparentPath(self.p / "d3"),
        ]
        expectation.sort()
        assert ls_res == expectation

        rose = False
        try:
            self.p.ls("f")
        except FileNotFoundError:
            rose = True
        assert rose

        rose = False
        try:
            ls_res = self.p.ls("f2")
        except NotADirectoryError:
            rose = True
        assert rose
        assert ls_res == expectation

        self.p.rm(absent="ignore", ignore_kind=True)

    def glob(self):
        self.p.rm(absent="ignore", ignore_kind=True)
        (self.p / "f1.png").touch()
        (self.p / "f2.pdf").touch()
        (self.p / "d3.pdf").touch()

        glob_res = list(self.p.glob())
        glob_res.sort()
        expectation = [
            TransparentPath(self.p / "f1.png"),
            TransparentPath(self.p / "f2.pdf"),
            TransparentPath(self.p / "d3.pdf"),
        ]
        expectation.sort()
        assert glob_res == expectation

        glob_res = list(self.p.glob("/*"))
        glob_res.sort()
        assert glob_res == expectation

        glob_res = list(self.p.glob("*"))
        glob_res.sort()
        assert glob_res == expectation

        glob_res = list(self.p.glob("/*.pdf"))
        glob_res.sort()
        expectation = [TransparentPath(self.p / "f2.pdf"), TransparentPath(self.p / "d3.pdf")]
        expectation.sort()
        assert glob_res == expectation

        self.p.rm(absent="ignore", ignore_kind=True)

    def with_suffix(self):
        p2 = self.p.with_suffix(".txt")
        assert str(p2) == str(self.p) + ".txt"
        assert str(p2.with_suffix(".pdf")) == str(self.p) + ".pdf"

    def stat(self):
        if not self.p.is_file():
            self.p.touch()
        stat = self.p.stat()
        nokeys = [
            "size",
            "timeCreated",
            "updated",
            "created",
            "mode",
            "uid",
            "gid",
            "mtime",
        ]
        keys = [
            "st_size",
            "st_mtime",
            "st_ctime",
            "st_mode",
            "st_uid",
            "st_gid",
            "st_mtime",
        ]
        for key in keys:
            assert key in stat
        for nokey in nokeys:
            assert nokey not in stat
        self.p.rm()


def do_test(fs, bucket=None, project=None):
    TransparentPath.set_global_fs(fs, bucket, project)
    print(f"\n  Testing Checks {fs}...")
    checks = Checks()
    checks.raise_missing_attr()
    print("    ...raise_missing_attr tested")
    checks.is_file()
    print("    ...is_file tested")
    checks.parent()
    print("    ...parent tested")
    checks.is_dir()
    print("    ...is_dir tested")
    checks.duplicate_instanciation()
    print("    ...dup. instanc. tested")
    checks.duplicate_method()
    print("    ...dup. method tested")
    # checks.get_put_mv()
    print("    ...get_put_mv tested")

    TransparentPath.set_global_fs(fs, bucket, project)
    print(f"\n  Testing Write {fs}...")
    write = Write()
    write.write_csv()
    print("    ...write_csv tested")
    write.write_excel()
    print("    ...write_excel tested")
    write.write_parquet()
    print("    ...write_parquet tested")
    write.write_hdf5()
    print("    ...write_hdf5 tested")
    write.write_txt()
    print("    ...write_txt tested")
    write.cust_open()
    print("    ...write_cust_open tested")
    write.write_dask()
    print("    ...write_dask tested")

    TransparentPath.set_global_fs(fs, bucket, project)
    print(f"\n  Testing Read {fs}")
    read = Read()
    read.read_csv()
    print("    ...read_csv tested")
    read.read_excel()
    print("    ...read_excel tested")
    read.read_parquet()
    print("    ...read_parquet tested")
    read.read_hdf5()
    print("    ...read_hdf5 tested")
    read.read_txt()
    print("    ...read_txt tested")
    read.read_dask()
    print("    ...read_dask tested")

    TransparentPath.set_global_fs(fs, bucket, project)
    print(f"\n  Testing Methods {fs}")
    methods = Methods()
    methods.cd()
    print("    ...cd tested")
    methods.ls()
    print("    ...ls tested")
    methods.glob()
    print("    ...glob tested")
    methods.with_suffix()
    print("    ...with_suffix tested")
    methods.stat()
    print("    ...stat tested")


def local_init(init):
    init.init_local()
    init.init_local_path_after_class()
    init.init_local_path_before_class()


def try_local_init(init):
    print("Testing local...")
    try:
        local_init(init)
    except Exception as e:
        print(f"Exception {type(e)} was raised, skipped.")
        print(e)
        return False
    else:
        print("Success.")
        return True


def gcs_init(init):
    init.init_gcs_fail()
    init.init_gcs_success()
    init.init_gcs_path_after_class()
    init.init_gcs_path_before_class_fail()
    init.init_gcs_path_before_class_success()


def try_gcs_init(init):
    print("Testing GCS...")
    try:
        gcs_init(init)
    except gcsfs.utils.HttpError as e:
        print(f"Exception {type(e)} was raised, skipped.")
        print(f"\t{e}")
        return False
    else:
        print("Success.")
        return True


def test_all():
    print("\nTesting Initialisations...")
    init = Init()
    loc = try_local_init(init)
    gcs = try_gcs_init(init)
    if loc and gcs:
        init.init_local_class_then_gcs_path()
        init.init_gcs_class_then_local_path()

    if loc:
        do_test("local")
        TransparentPath("chien").rmdir()
    else:
        print("Local test skipped.")

    if gcs:
        do_test("gcs", project=project, bucket=bucket)
    else:
        print("GCS test skipped.")
