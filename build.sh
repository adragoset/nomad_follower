#!/bin/sh
while getopts ":n:s:b:t:" opt; do
  case $opt in
    s) SHA1="$OPTARG"
    ;;
    b) BRANCH="$OPTARG"
    ;;
    t) TAG="$OPTARG"
    ;;
    n) NET="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done
echo docker build -t devopsintralox/nomad_follower:${SHA1} --network=${NET} .
docker build -t devopsintralox/nomad_follower:${SHA1} --network=${NET} .
docker push devopsintralox/nomad_follower:${SHA1}
if [ "${TAG}" != "" ]; then
    docker tag devopsintralox/nomad_follower:${SHA1} devopsintralox/nomad_follower:${TAG}
    docker push devopsintralox/nomad_follower:${TAG}
else
    docker tag devopsintralox/nomad_follower:${SHA1} devopsintralox/nomad_follower:${BRANCH}
    docker push devopsintralox/nomad_follower:${BRANCH}
fi