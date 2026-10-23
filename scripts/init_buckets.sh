#!/usr/bin/env bash
set -e
for b in bronze silver gold meta; do
  echo "s3.bucket.create -name $b" | docker exec -i seaweedfs weed shell -master=localhost:9333
done
echo "Đã tạo bucket: bronze silver gold meta"