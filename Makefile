.PHONY: all
all: test

.PHONY: test
test:
	tox

.PHONY: check
check:
	flake8 --max-line-length=100 cockroachdb test
	python setup.py check

# Update the requirements files (but does not install the updates; use
# bootstrap.sh for that)
.PHONY: update-requirements
update-requirements:
	build/update-requirements.sh dev-requirements.txt
	build/update-requirements.sh test-requirements.txt
