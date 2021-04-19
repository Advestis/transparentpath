import subprocess
from pathlib import Path
from typing import List

from setuptools import find_packages, setup

name = "transparentpath"


def run_cmd(cmd) -> List[str]:
    if isinstance(cmd, str):
        cmd = cmd.split(" ")
    return subprocess.check_output(cmd).decode(encoding="UTF-8").split("\n")


def get_greatest_version(versions: List[str]) -> str:
    versions = [list(map(int, v[1:].split("."))) for v in versions]
    greatest = None
    for v in versions:
        if greatest is None:
            greatest = v
        else:
            lower = False
            for i in range(len(v)):
                if len(greatest) < i + 1:
                    greatest = v
                    break
                if v[i] > greatest[i]:
                    greatest = v
                    break
                if v[i] < greatest[i]:
                    lower = True
                    break
            if not lower:
                greatest = v
    return f"v{'.'.join([str(s) for s in greatest])}"


def get_last_tag() -> str:
    result = [v for v in run_cmd("git tag -l v*") if not v == ""]
    if len(result) == 0:
        run_cmd("git tag v0.1")
    result = [v for v in run_cmd("git tag -l v*") if not v == "" and v.startswith("v")]
    return get_greatest_version(result)


def get_nb_commits_until(tag: str) -> int:
    return len(run_cmd(f'git log {tag}..HEAD --oneline'))


def get_version() -> str:
    last_tag = get_last_tag()
    return f"{'.'.join(last_tag.split('.'))}.{get_nb_commits_until(last_tag)}"


def get_branch_name() -> str:
    branches = run_cmd("git branch")
    for branch in branches:
        if "*" in branch:
            return branch.replace("*", "").replace(" ", "")
    return ""


git_installed = subprocess.call('command -v git >> /dev/null', shell=True)

branch_name = None
if git_installed == 0:
    try:
        branch_name = get_branch_name()
        with open("BRANCH.txt", "w") as bfile:
            bfile.write(branch_name)
    except FileNotFoundError as e:
        pass
if branch_name is None:
    # noinspection PyBroadException
    try:
        with open("BRANCH.txt", "r") as bfile:
            branch_name = bfile.readline()
    except:
        branch_name = "master"

if branch_name != "master":
    name = "_".join([name, branch_name])

try:
    long_description = Path("README.md").read_text()
except UnicodeDecodeError:
    with open("README.md", "rb") as ifile:
        lines = [line.decode("utf-8") for line in ifile.readlines()]
        long_description = "".join(lines)

optional_requirements = {}
requirements = []
all_reqs = []

for afile in Path("").glob("*requirements.txt"):
    if str(afile) == "requirements.txt":
        requirements = afile.read_text().splitlines()
        all_reqs = list(set(all_reqs) | set(afile.read_text().splitlines()))
    else:
        option = afile.stem.replace("-requirements", "")
        optional_requirements[option] = afile.read_text().splitlines()
        all_reqs = list(set(all_reqs) | set(optional_requirements[option]))


version = None
if git_installed == 0:
    try:
        version = get_version()
        with open("VERSION.txt", "w") as vfile:
            vfile.write(version)
    except FileNotFoundError as e:
        pass
if version is None:
    # noinspection PyBroadException
    try:
        with open("VERSION.txt", "r") as vfile:
            version = vfile.readline()
    except:
        pass

optional_requirements["all"] = all_reqs

if __name__ == "__main__":
    setup(
        name=name,
        version=version,
        author="Philippe COTTE",
        author_email="pcotte@advestis.com",
        include_package_data=True,
        description="A class that allows one to use a path in a local file system or a gcs file system (more or less) "
                    "in almost the same way one would use a pathlib.Path object.",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/Advestis/transparentpath",
        packages=find_packages(),
        install_requires=requirements,
        package_data={"": ["*", ".*"]},
        extras_require=optional_requirements,
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
            "Operating System :: OS Independent",
            "Development Status :: 5 - Production/Stable"
        ],
        python_requires='>=3.7',
    )

    if Path("apt-requirements.txt").is_file():
        apt_requirements = Path("apt-requirements.txt").read_text().splitlines()
        print("WARNING: Found apt-requirements.txt. You will have to install by hand its content :")
        for line in apt_requirements:
            print(" - ", line)
        print("If you are using Linux, you can use apt-get install or equivalent to install those packages. Else,"
              "download and install them according to your OS.")
        print("If you are using Linux and used install.sh to install this package, you can ignore this message,"
              "the requirements have been installed.")

    if Path("gspip-requirements.txt").is_file():
        gspip_requirements = Path("gspip-requirements.txt").read_text().splitlines()
        print("WARNING: Found gspip-requirements.txt. You will have to install from gcs its content :")
        for line in gspip_requirements:
            print(" - ", line)
        print("If you are using Linux, install and use gspip from https://github.com/Advestis/gspip. On windows,"
              "you will have to download by hand the latest version of the required packages on"
              " gs://pypi_server_sand/package_name")
        print("If you are using Linux and used install.sh to install this package, you can ignore this message,"
              "the requirements have been installed.")
