#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0



if [ -z "$1" ]
then
  echo "Usage: $0 MINIO_BASE_PATH" && exit 1
fi


for path in $(mc ls -r $1| awk '{print $NF}')
do
   
   shasum=$(mc cat $1/$path|shasum|awk '{print $1}')
   fmtpath=$(echo $1/$path|sed 's/\/\//\//g')
   echo $fmtpath $shasum
done 

