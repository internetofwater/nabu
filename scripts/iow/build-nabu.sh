#!/bin/sh


set -e

mkdir -p $HOME/build
cd $HOME/build && git clone https://github.com/internetofwater/nabu.git
if [ "$(go env GOOS)" != "linux" ]
then
   # go can build multi o/s but this script doesn't support/need right now
   echo "Unhandled build operating system target: $(go env GOOS)" && exit 1
fi

cd nabu 
case "$(go env GOARCH)" in 
amd64)
  make nabu    
  cp cmd/nabu $HOME/bin
  ;;
arm64)
  make nabu.m2.linux 
  cp nabu_m2_linux $HOME/bin/nabu
  ;;
*) 
   # go handles multiarch (also) but script doesn't support/need now
   echo "Unhandled build architecture target: $(go env GOARCH)" && exit 1
   ;;
esac


