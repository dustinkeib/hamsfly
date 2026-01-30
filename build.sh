#!/usr/bin/env bash
set -o errexit

pip install -r requirements.txt

# Run tests with coverage (fail build if tests fail)
coverage run --source='apps.hamsalert' manage.py test apps.hamsalert.tests --verbosity=1 --noinput
coverage report

python manage.py collectstatic --no-input
python manage.py migrate
python manage.py createcachetable --dry-run || python manage.py createcachetable
