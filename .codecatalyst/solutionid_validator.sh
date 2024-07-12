#!/bin/sh

set -e

SOLUTIONID="SO9052XX"

echo "grep -nr --exclude-dir='.codecatalyst' "${SOLUTIONID}" ./.."
result=$(grep -nr --exclude-dir='.codecatalyst' "${SOLUTIONID}" ./..)
if [ $? -eq 0 ] 
then
  echo "$result"
else
  echo 'Solution ID "${SOLUTIONID}" not found'
fi

export result
