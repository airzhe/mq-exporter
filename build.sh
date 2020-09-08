#!/usr/bin/env bash
set -e
cd $(dirname $0)

echo "build start"

APP_NAME=mq-exporter

# use go vendor
export GO111MODULE=on
export GOPROXY=https://goproxy.io
export GOFLAGS="-mod=vendor"

get_commit() {
    branch=$(git rev-parse --abbrev-ref HEAD)
    commit_id=$(git rev-parse HEAD)
    echo ${branch}-${commit_id}
}

version=$(git describe --tags --always --dirty | tr '-' '.')
commit=$(get_commit)

#GOOS=linux GOARCH=amd64 go build \
go build \
    -ldflags "-X main.versionStr=$version -X main.commitStr=$commit" \
    -o bin/${APP_NAME}

mkdir -p ./pkg/bin/
mkdir -p ./pkg/config
mkdir -p ./pkg/logs
mkdir -p ./output
cp -r ./bin/* ./pkg/bin
cp -r ./config/*.yaml ./pkg/config/

echo "making tarball"
cd ./pkg

tar -czvf ../output/${APP_NAME}${version}.tar.gz .

echo "done"