#!/usr/bin/make -f

COMPARE_BRANCH=origin/master


test: quality coverage

coverage:
	coverage run manage.py test notifier
	coverage report

quality: pycodestyle pylint

pycodestyle:
	diff-quality --violations pycodestyle --compare-branch $(COMPARE_BRANCH)

pylint:
	diff-quality --violations pylint --compare-branch $(COMPARE_BRANCH)

clean:
	find . -type f -name '*.pyc' -delete
