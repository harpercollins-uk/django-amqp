# Django AMQP

AMQP support for Django, with implementations for RabbitMQ and Azure ServiceBus.

## Overview

Django AMQP is an extension for Django that provides Advanced Message Queuing Protocol (AMQP) support. It enables you to easily implement message queues and task processing in your Django applications using either RabbitMQ or Azure Service Bus as the backend.

The library integrates with Django's task framework, allowing you to define, queue, and process asynchronous tasks with support for scheduled/deferred execution.

## Installation

```bash
pip install django-amqp
```

## Configuration

Add `django_amqp` to your `INSTALLED_APPS` in `settings.py`:

```python
INSTALLED_APPS = [
    # ...
    'django_amqp',
    # ...
]
```

Configure your task backend in `settings.py`:

```python
SERVICEBUS_CONNECTION_STRING = os.environ.get("SERVICEBUS_CONNECTION_STRING")

TASK_BACKEND_MAPPING = {
    "immediate": "django_tasks.backends.immediate.ImmediateBackend",
    "service-bus": "audioai.django_amqp.backend.ServiceBusBackend",
    "dummy": "django_tasks.backends.dummy.DummyBackend",
    "database": "django_tasks.backends.database.DatabaseBackend",
}

TASKS = {
    "default": {
        "BACKEND": TASK_BACKEND_MAPPING.get(
            os.environ.get("TASKS_BACKEND", "immediate"),
        ),
        "QUEUES": [
            "default",
        ],
        "OPTIONS": {"connection_string": SERVICEBUS_CONNECTION_STRING},
    }
}

# For worker commands
SERVICEBUS_CONNECTION_STRING = "your-servicebus-connection-string-from-env-var"
```

## Usage

### Defining Tasks

Create a task using Django's task decorator:

```python
from django.tasks import task

@task(queue_name="my-queue")
def my_background_task(param1, param2, **kwargs):
    # Your task logic here
    pass
```

### Queueing Tasks

Queue a task for immediate execution:

```python
from myapp.tasks import my_background_task

# Queue for immediate execution
my_background_task.enqueue(param1="value1", param2="value2")
```

### Running Workers

To process messages from a specific queue:

```bash
python manage.py amqp_worker --queue-name="my-queue" --burst
```

The `--burst` flag tells the worker to process all available messages and then exit.

## Advanced Usage

### Cancelling Scheduled Messages

You can cancel scheduled messages using the sequence number returned when scheduling a message:

```python
from django_amqp.service_bus import cancel_deferred_message

# Schedule a task and get the sequence number
sequence_number = my_background_task.delay_until(future_time, param1="value1")

# Later, if you need to cancel it
cancel_deferred_message(sequence_number, queue_name="my-queue")
```

### Error Handling

Failed tasks are automatically moved to the dead-letter queue with the error reason and description. You can implement additional error handling in your task functions.

## License

This project is licensed under the BSD 3-Clause License - see the LICENSE file for details.
