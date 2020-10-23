pip3 install sphinx
pip3 install sphinxcontrib-apidoc
pip3 install sphinx-rtd-theme
pip3 install --upgrade recommonmark
pip3 install sphinx-markdown-builder
pip3 install gitchangelog
gitchangelog > ./source/changelog.rst
if [ ! -d ./source/_static ]; then mkdir -p ./source/_static; fi
if [ ! -d ./source/_templates ]; then mkdir -p ./source/_templates; fi
sphinx-apidoc -f -o source/ transparentpath
make html
make markdown
cp -a _build/html/. docs/
