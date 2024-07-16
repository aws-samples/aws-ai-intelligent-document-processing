#!/bin/sh

USER_NAME="Vijay Prince0"
EMAIL="vjprince@amazon.com"

git config --list
git remote add upstream https://github.com/vprince1/aws-ai-intelligent-document-processing.git
git fetch upstream
git -c "user.name=Vijay Prince" -c "user.email=vjprince@amazon.com" merge upstream/main --allow-unrelated-histories

echo "git -c "user.name=${USER_NAME}" -c "user.email=${EMAIL}" merge upstream/main --allow-unrelated-histories"
result=$(git -c "user.name=${USER_NAME}" -c "user.email=${EMAIL}" merge upstream/main --allow-unrelated-histories)
if [ $? -eq 0 ]
then
  echo "Merge Success"
  echo "$result"
  #git push
  exit 0
else
  echo "Merge Failed"
  git diff
  exit 1
fi

