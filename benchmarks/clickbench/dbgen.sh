#!/usr/bin/env bash
set -euo pipefail

echo "Generating ClickBench database"

mkdir -p data
cd data

# Reuse existing datasets
if [ -z "$(ls -A "clickbench")" ]; then
  (
    mkdir -p clickbench
    cd clickbench

    curl -OL https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz
    echo 'de2f86030d1c86fd39d03468bd90a911 hits.tsv.gz' | md5sum --check --status
    gzip -d hits.tsv.gz
  )
fi
