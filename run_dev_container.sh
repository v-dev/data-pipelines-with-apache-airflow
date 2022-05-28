#!/usr/bin/env bash

usage() {
  echo Usage:
  echo "$0 <chapter/directory>"
  echo; echo "Example:"
  echo "$0 chapter08"
}

if [ $# -eq 0 ]
  then
    usage
    exit 1
fi


chapter=$1

docker build -t devairflow .

docker run --rm -it -v $(pwd):/source -v $(pwd)/${chapter}/dags:/opt/airflow/dags devairflow
