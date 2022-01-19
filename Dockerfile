FROM python:3.9-slim
# Mount: install directory
COPY . /install
# Install: transparentpath
RUN cd /install/ || exit 1 && pip install -U pip || exit 1 && pip install .[all];
# Remove: install directory
RUN rm -rfd /install/ || exit 1;
