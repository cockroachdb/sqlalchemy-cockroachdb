.PHONY: all
all: test

.PHONY: test
test:
	tox --skip-missing-interpreters
