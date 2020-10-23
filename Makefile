PACKAGE = $(basename pwd)
current_dir := $(notdir $(patsubst %/,%,$(dir $(mkfile_path))))


test:
	@echo $(current_dir)

help:
	@echo "Run :"
	@echo "  - make install to install the program"
	@echo "  - make doc to compile the doc"
	@echo "  - make a_command to run python setup.py 'a_command'"

.PHONY: help Makefile

doc:
	@echo "Making documentation..."
	@pip3 install sphinx
	@pip3 install sphinxcontrib-apidoc
	@pip3 install sphinx-rtd-theme
	@pip3 install --upgrade recommonmark
	@pip3 install sphinx-markdown-builder
	@pip3 install gitchangelog
	@gitchangelog > ./docsbuild/source/changelog.rst
	@if [ ! -d ./docsbuild/source/_static ]; then mkdir -p ./docsbuild/source/_static; fi
	@if [ ! -d ./docsbuild/source/_templates ]; then mkdir -p ./docsbuild/source/_templates; fi
	@sphinx-apidoc -f -o ./docsbuild/source/ transparentpath
	@make -C docsbuild html
	@if ! [ -f ./docsbuild/build/html/.nojekyll ] ; then touch ./docsbuild/build/html/.nojekyll ; fi
	@if ! [ -d ./docs ] ; then mkdir ./docs ; fi
	@cp -a ./docsbuild/build/html/. ./docs/

%: Makefile
	@echo "Running python setup.py "$@"..."
	@if [ -f apt-requirements.txt ] ; then if command -v sudo > /dev/null ; then sudo apt-get install -y $(grep -vE "^\s*#" apt-requirements.txt  | tr "\n" " ") else apt-et install -y $(grep -vE "^\s*#" apt-requirements.txt  | tr "\n" " ") ; fi ; fi

	@if [ -f gspip-requirements.txt ] ; then if command -v gspip > /dev/null ; then gspip --upgrade install $(grep -vE "^\s*#" gspip-requirements.txt  | tr "\n" " ") else git clone https://github.com/Advestis/gspip && gspip/gspip.sh --upgrade install $(grep -vE "^\s*#" gspip-requirements.txt  | tr "\n" " ") && rm -rf gspip ; fi ; fi

	@pip3 uninstall "$(PACKAGE)" -y
	@pip3 install setuptools
	@python setup.py $@
	@if [ -d "dist" ] && [ $@ != "sdist" ] ; then rm -r dist ; fi
	@if [ -d "build" ] ; then rm -r build ; fi
	@if ls "$(PACKAGE)".egg-info* &> /dev/null ; then rm -r "$(PACKAGE)".egg-info* ; fi

