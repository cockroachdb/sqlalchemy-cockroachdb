.PHONY: all
all: proto test

.PHONY: proto
proto:
	protoc --proto_path=cockroach-proto --python_out=. cockroach-proto/cockroach/proto/*.proto

.PHONY: test
test:
	tox
