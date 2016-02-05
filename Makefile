#!/usr/bin/make -f

COMPARE_BRANCH=origin/master


test: quality coverage

coverage:
	coverage run manage.py test notifier
	coverage report

quality: pep8 pylint

pep8:
	diff-quality --violations pep8 --compare-branch $(COMPARE_BRANCH)

pylint:
	diff-quality --violations pylint --compare-branch $(COMPARE_BRANCH)

clean:
	find . -type f -name '*.pyc' -delete
