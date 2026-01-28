#!/usr/bin/env bash
set -o errexit

pip install -r requirements.txt

# Run tests (fail build if tests fail)
python manage.py test apps.hamsalert.tests --verbosity=1

python manage.py collectstatic --no-input
python manage.py migrate
