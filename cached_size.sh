#!/bin/bash

# This script calculates the real cached size on disk for object chunks of a given file id

if [[ $# -ne 1 ]]; then
    echo "Usage: cache_compare.sh <FILE ID>"
    exit
fi

fileId=$1

sqlite="sqlite3 $HOME/Library/Caches/StreamDrive/filesystem.db "
if [[ `$sqlite "select count(id) from filesystem where id = '$fileId'"` -eq 0 ]]; then
    echo "File ID seems to be invalid. File couldn't be found in cache."
    exit
fi

objectNames=`$sqlite "select object_name from object_chunk where file_id = '$fileId'"`
totalSize=0
for o in $objectNames; do
    totalSize=$(($totalSize+`stat -f "%z" $HOME/Library/Caches/StreamDrive/objects/$o`))
done

expectedSize=`$sqlite "select size from filesystem where id = '$fileId'"`

echo "Total cached size: $totalSize bytes"
echo "Expected cache size: $expectedSize bytes"

difference=$(($totalSize - $expectedSize))
if [[ $difference -ge 0 ]]; then
    difference="+$difference"
fi
echo "Difference: $difference bytes"

