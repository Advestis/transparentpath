FROM python:3.8-slim

# Install: git setuptools
RUN apt-get update
RUN apt-get install -y git gcc python3-dev
RUN python -m pip install --upgrade pip
RUN pip3 install setuptools


# Mount: install directory
COPY . /install


# Install: transparentpath
RUN cd /install/ || exit 1 && pip3 install .[all];


# Remove: install directory
RUN rm -rfd /install/ || exit 1;
