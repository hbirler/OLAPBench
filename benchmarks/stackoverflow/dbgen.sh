#!/usr/bin/env bash
set -euo pipefail

SF=${1:-1}
BASE="http://db.in.tum.de/~schmidt/data/stackoverflow"
echo "Downloading StackOverflow data set"

mkdir -p "data"
cd "data"

wget -q "${BASE}_schema.sql"
if [ "$SF" == "dba" ] || [ "$SF" == "1" ]; then
  mkdir "stackoverflow_dba"
  if [ -z "$(ls -A "stackoverflow_dba")" ]; then
    wget -q -O - "${BASE}_dba.tar.gz" | tar -xz
  fi
fi

if [ "$SF" == "math" ] || [ "$SF" == "12" ]; then
  mkdir "stackoverflow_math"
  if [ -z "$(ls -A "stackoverflow_math")" ]; then
    wget -q -O - "${BASE}_math.tar.gz" | tar -xz
  fi
fi

if [ "$SF" == "full" ] || [ "$SF" == "222" ]; then
  mkdir "stackoverflow"
  if [ -z "$(ls -A "stackoverflow")" ]; then
    wget -q -O - "${BASE}.tar.gz" | tar -xz
  fi
fi
