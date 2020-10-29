mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
PACKAGE := $(notdir $(patsubst %/,%,$(dir $(mkfile_path))))


help:
	@echo "Run :"
	@echo "  - make install to install the program"
	@echo "  - make doc to compile the doc"
	@echo "  - make a_command to run python setup.py 'a_command'"

.PHONY: help Makefile

doc:
	@echo "Making documentation..."
	@pip3 install pdoc3
	@pdoc --html $(PACKAGE) -o docs
	@mv docs/$(PACKAGE)/* docs/
	@rm -r docs/$(PACKAGE)

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
