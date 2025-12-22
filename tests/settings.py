import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

IN_TEST = "IN_TEST" in os.environ or (len(sys.argv) > 1 and sys.argv[1] == "test")

ALLOWED_HOSTS = ["*"]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": os.path.join(BASE_DIR, "db.sqlite3"),
    }
}

INSTALLED_APPS = [
    "django_amqp",
    "tests",
]

SECRET_KEY = "abcde12345"

USE_TZ = True

if not IN_TEST:
    DEBUG = True
    TASKS = {"default": {"BACKEND": "django_amqp.backend.AMQPBackend"}}
