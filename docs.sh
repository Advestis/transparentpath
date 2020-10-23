pip3 install sphinx
pip3 install sphinxcontrib-apidoc
pip3 install sphinx-rtd-theme
pip3 install --upgrade recommonmark
pip3 install sphinx-markdown-builder
pip3 install gitchangelog
gitchangelog > ./docsbuild/source/changelog.rst
if [ ! -d ./docsbuild/source/_static ]; then mkdir -p ./docsbuild/source/_static; fi
if [ ! -d ./docsbuild/source/_templates ]; then mkdir -p ./docsbuild/source/_templates; fi
sphinx-apidoc -f -o ./docsbuild/source/ transparentpath
cd docsbuild && make html
if ! [ -f ./build/html/.nojekyll ] ; then
  touch ./build/html/.nojekyll
fi

cp -a ./build/html/. ../docs/
