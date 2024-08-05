#!/bin/sh


docker stop graphdb >/dev/null 2>&1
set -e
while [ "$(docker ps|grep -c graphdb)" -gt "0" ]
do
 sleep 1
done 
