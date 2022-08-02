.PHONY: all
all: help

.PHONY: clean
clean:
	rm -f .coverage coverage.xml writer.pickle
	rm -rf .pytest_cache build dist htmlcov test-reports

.PHONY: help
help:
	@echo 'Makefile Targets'
	@echo '  clean      clean up local files'
	@echo '  help       print this help output'
	@echo '  install    install library as editable with all dependencies'
	@echo '  lint       execute flake8 against source code'
	@echo '  test       execute all tests'

.PHONY: install
install:
	pip install --editable ".[test,extra,ciso,async]"

.PHONY: lint
lint:
	flake8 setup.py influxdb_client/

.PHONY: test
test:
	pytest tests \
		--cov=./ \
		--cov-report html:htmlcov \
		--cov-report xml:coverage.xml
