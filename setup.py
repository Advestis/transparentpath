from pathlib import Path
import versioneer

from setuptools import setup

cmdclass = versioneer.get_cmdclass()
sdist_class = cmdclass["sdist"]

workdir = Path(__file__).parent

if __name__ == "__main__":

    setup(
        version=versioneer.get_version(),
        cmdclass=cmdclass,
    )
