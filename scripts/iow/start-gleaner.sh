#!/bin/sh



set -e # exit on error 

TS=`date +%Y-%m-%dT%H.%M.%S`
LOGDIR="$HOME/logs/$TS"




mkdir -p $LOGDIR 
cd $HOME

GLEANERCONFIG_PATH=""

if [  -z "$1 ]
then
  echo "Usage: $0 GLEANERCONFIG_PATH" && exit 1
fi


for src in `cat $GLEANERCONFIG_PATH | grep '\Wname:'|awk '{print $2}'`
do

OUTFILE="$LOGDIR/gleaner-$src.out"
ERRFILE="$LOGDIR/gleaner-$src.err"

echo "harvesting source '$src'..."
bin/gleaner -log debug  -cfg $GLEANERCONFIG_PATH -source $src -rude # > $OUTFILE 2>$ERRFILE
done
echo "complete!"

