#!/bin/sh

set -e

SOLUTIONID="SO9052"

result=$(grep -nr --exclude-dir='.*' "${sdf}")
if [[ $? -eq 0 ]] then
  echo $result
else
  echo 'Solution ID "${SOLUTIONID}" not found'
fi

export result
