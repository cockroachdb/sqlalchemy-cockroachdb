.PHONY: all
all: proto test

.PHONY: proto
proto:
	protoc --proto_path=cockroach-proto --python_out=. cockroach-proto/cockroach/proto/*.proto

.PHONY: test
test:
	tox --skip-missing-interpreters

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
	-rm -rf /tmp/test-disk1
	mkdir /tmp/test-disk1
	docker-compose build
	# TODO: ditch the --insecure; client needs to learn TLS for that.
	# docker-compose run cockroach cert create-ca --certs=/data
	# docker-compose run cockroach cert create-node --certs=/data localhost 127.0.0.1 $(hostname)
	docker-compose run cockroach init --stores=hdd=/data
	docker-compose start cockroach
	docker-compose run cockroachpython
	docker-compose stop
