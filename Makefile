.PHONY: all
all: proto test

.PHONY: proto
proto:
	protoc --proto_path=cockroach-proto --python_out=. cockroach-proto/cockroach/proto/*.proto

.PHONY: test
test:
	tox

# In theory we should just be able to do docker-compose build &&
# docker-compose run cockroachpython, but docker-compose doesn't
# seem to create the network links except when the server is launched
# with start/stop.
# We also need to delete the cockroach container first to start from
# an empty database.
.PHONY: dockertest
dockertest:
	-docker-compose stop
	-docker-compose rm --force cockroach
	docker-compose build
	docker-compose start cockroach
	docker-compose run cockroachpython
	docker-compose stop
