pip3 install sphinx
pip3 install sphinxcontrib-apidoc
pip3 install sphinx-rtd-theme
pip3 install --upgrade recommonmark
pip3 install sphinx-markdown-builder
pip3 install gitchangelog
gitchangelog > ./docs/source/changelog.rst
if [ ! -d ./docs/source/_static ]; then mkdir -p ./docs/source/_static; fi
if [ ! -d ./docs/source/_templates ]; then mkdir -p ./docs/source/_templates; fi
sphinx-apidoc -f -o ./docs/source/ transparentpath
cd docs && make html
if ! [ -f ./docs/build/html/.nojekyll ] ; then touch ./docs/build/html/.nojekyll ; fi
