#!/bin/sh


set -e

mkdir -p $HOME/build
cd $HOME/build && git clone https://github.com/internetofwater/gleaner.git
if [ "$(go env GOOS)" != "linux" ]
then
   # go can build multi o/s but this script doesn't support/need right now
   echo "Unhandled build operating system target: $(go env GOOS)" && exit 1
fi

cd gleaner 
case "$(go env GOARCH)" in 
amd64)
  go build -o $HOME/gleaner
  ;;
arm64)
  make gleaner.m2.linux 
  cp gleaner_m2_linux $HOME/bin/gleaner
  ;;
*) 
   # go handles multiarch (also) but script doesn't support/need now
   echo "Unhandled build architecture target: $(go env GOARCH)" && exit 1
   ;;
esac


