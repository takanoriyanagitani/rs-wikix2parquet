#!/bin/sh

export OUT_FILENAME=./sample.d/out.parquet
export COMPRESSION=snappy
export BATCH_SIZE=131072
export BATCH_SIZE=16384
export BATCH_SIZE=1024

input(){
	dd \
		if="$HOME/Downloads/enwiki-20250801-pages-articles-multistream-index.txt.gz" \
		of=/dev/stdout \
		bs=1048576 \
		status=progress |
		zcat
}

test -f "${OUT_FILENAME}" || input | ./wikix2parquet

time duckdb -c "SELECT COUNT(*) FROM read_parquet('./sample.d/out.parquet')"
