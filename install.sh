#!/bin/bash

PACKAGE="transparentpath"
VERSION="3"
COMMAND="install"

while true; do
  case "$1" in
    -v) VERSION=$2 ; shift 2 ;;
    -c) COMMAND=$2 ; shift 2 ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -f apt-requirements.txt ] ; then
  if command -v sudo > /dev/null ; then
    sudo apt-get install -y $(grep -vE "^\s*#" apt-requirements.txt  | tr "\n" " ")
  else
    apt-get install -y $(grep -vE "^\s*#" apt-requirements.txt  | tr "\n" " ")
  fi
fi

if [ -f gspip-requirements.txt ] ; then
  if command -v gspip > /dev/null ; then
    gspip --upgrade install $(grep -vE "^\s*#" gspip-requirements.txt  | tr "\n" " ")
  else
    git clone https://github.com/Advestis/gspip && gspip/gspip.sh --upgrade install $(grep -vE "^\s*#" gspip-requirements.txt  | tr "\n" " ") && rm -rf gspip
  fi
fi

pip3 uninstall "$PACKAGE" -y
pip3 install setuptools
python$VERSION setup.py $COMMAND
if [ -d "dist" ] && [ "$COMMAND" != "sdist" ] ; then rm -r dist ; fi
if [ -d "build" ] ; then rm -r build ; fi
if ls "$PACKAGE".egg-info* &> /dev/null ; then rm -r "$PACKAGE".egg-info* ; fi
