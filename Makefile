BINARY_NAME=mq-exporter
ROOT:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
VERSION=$(shell git describe --tags --always --dirty | tr '-' '.')
COMMIT=$(shell echo `git rev-parse --abbrev-ref HEAD`-`git rev-parse HEAD`)

.PHONY: all build_binary build_docker_binary docker-build run test clean

all: build_binary packaged test

build_binary:
	@echo "build start"
	@go build \
    	-ldflags "-X main.versionStr=$(VERSION) -X main.commitStr=$(COMMIT)" \
    	-o $(ROOT)/bin/$(BINARY_NAME)

build_docker_binary:
	@echo "build start"
	@CGO_ENABLED=0 go build \
    	-ldflags "-X main.versionStr=$(VERSION) -X main.commitStr=$(COMMIT)" \
    	-o $(ROOT)/bin/$(BINARY_NAME)

packaged:
	@mkdir -p $(ROOT)/pkg/bin/
	@mkdir -p $(ROOT)/pkg/config
	@mkdir -p $(ROOT)/pkg/logs
	@mkdir -p $(ROOT)/output

	@cp -r $(ROOT)/bin/* $(ROOT)/pkg/bin
	@cp -r $(ROOT)/config/*.yaml $(ROOT)/pkg/config/

	@echo "making tarball"
	@cd  $(ROOT)/pkg && tar -czvf $(ROOT)/output/$(BINARY_NAME)$(VERSION).tar.gz . && cd ..

	@echo "done"

docker-build: build_docker_binary packaged
	@cd  $(ROOT)/output ; tar -zxvf $(ROOT)/output/$(BINARY_NAME)$(VERSION).tar.gz
	docker build -t $(BINARY_NAME):$(VERSION) .

run: build_binary
	@tar -zxvf $(ROOT)/output/$(BINARY_NAME)$(VERSION).tar.gz
	@sh $(ROOT)/output/bin/run.sh

test:
	@echo "test"

clean:
	@rm -rf output
	@rm -rf pkg
	@rm -r $(ROOT)/bin/$(BINARY_NAME)
