.PHONY: all
all: test lint

.PHONY: test
test:
	tox

.PHONY: lint
lint:
	tox -e lint

# Update the requirements files (but does not install the updates; use
# bootstrap.sh for that)
.PHONY: update-requirements
update-requirements:
	./update-requirements.sh dev-requirements.txt
	./update-requirements.sh test-requirements.txt

.PHONY: build
build: clean
	python setup.py sdist

.PHONY: clean
clean:
	rm -rf dist build

.PHONY: detox
detox: clean
	rm -rf .tox
