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
	-docker-compose rm --force -v cockroach
	docker-compose build
	# TODO: ditch the --insecure; client needs to learn TLS for that.
	# docker-compose run cockroach cert create-ca --certs=/data
	# docker-compose run cockroach cert create-node --certs=/data localhost 127.0.0.1 $(hostname)
	# Note that we clean up the old data from inside the container;
	# the volume it uses is not visible from outside in the case of
	# boot2docker (and rocksdb refuses to run in a virtualbox shared
	# folder).
	docker-compose run cockroach shell rm -rf '/data/*'
	docker-compose run cockroach init --stores=hdd=/data
	docker-compose start cockroach
	docker-compose run cockroachpython
	docker-compose stop
