#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

set -e # exit on error 

if [  -z "$1" ]
then
  echo "Usage: $0 GLEANERCONFIG_PATH" && exit 1
fi
GLEANERCONFIG_PATH="$1"

for src in `cat $GLEANERCONFIG_PATH | grep '\Wname:'|awk '{print $2}'`
do

	OUTFILE="$LOGDIR/gleaner-$src.out"
	ERRFILE="$LOGDIR/gleaner-$src.err"

	echo "harvesting source '$src'..."
	$HOME/gleaner --cfg $GLEANERCONFIG_PATH --source $src
done
echo "completed gleaner harvest!"

