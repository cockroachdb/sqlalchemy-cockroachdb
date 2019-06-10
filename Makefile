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
	build/update-requirements.sh dev-requirements.txt
	build/update-requirements.sh test-requirements.txt

.PHONY: detox
detox:
	rm -rf .tox
