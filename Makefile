.PHONY: all
all: test

.PHONY: test
test:
	tox

.PHONY: check
check:
	flake8 --max-line-length=100 cockroachdb test
	python setup.py check

.PHONY: update-dependencies
update-dependencies:
	pip install -U -r dev-requirements.txt.in
	pip freeze > dev-requirements.txt
