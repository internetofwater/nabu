#!/bin/sh

set -e


base_s3_path="myminio/iow/prov/"


function assert_count {

	if [ "$1" -ne "$2" ]
	then
   		echo "$3: expected s3 file count to be $1: found $2" && exit 1
	fi

}

function test_s3_filecount {

	src_name="$1"
	expect_count="$2"

	s3_path="$base_path/$src_name"
	count=$($HOME/bin/mc ls $s3_path|grep '.jsonld'|wc -l)
	assert_count $expect_count $count $s3_path

}


# test expected counts of .jsonld files that should land in s3 per source site (name) 
test_s3_filecount refgages0 330 
test_s3_filecount refmainstems 66
test_s3_filecount dams0 45
test_s3_filecount cdss0 30
test_s3_filecount nmwdist0 266

