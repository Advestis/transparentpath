FROM python:3.6-slim

#COPY --from=thomasweise/docker-texlive-full / /

# Install: git setuptools
RUN apt-get update
RUN apt-get install -y git gcc python3-dev
RUN pip3 install setuptools


# Mount: install directory
COPY . /install


# Install: transparentpath
RUN cd /install/ || exit 1 && python setup.py install;


# Remove: install directory
RUN rm -rfd /install/ || exit 1;
