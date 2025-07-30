#!/usr/bin/env bash
set -euo pipefail

echo "Generating MarkJoin database"

mkdir -p data
cd data

# Reuse existing datasets
if [ -z "$(ls -A "markjoin")" ]; then
(
  mkdir markjoin
  cd markjoin

  echo 'a78ae08d4e163470b39d41d25a5b1d11174b4d84f35a12c5c043a90b89acce5b  markjoinbench.zip' | sha256sum --check --status 2>/dev/null || curl -OL https://db.in.tum.de/~birler/data/markjoinbench.zip
  echo 'a78ae08d4e163470b39d41d25a5b1d11174b4d84f35a12c5c043a90b89acce5b  markjoinbench.zip' | sha256sum --check --status
  unzip -o markjoinbench.zip
  rm markjoinbench.zip
)
fi
