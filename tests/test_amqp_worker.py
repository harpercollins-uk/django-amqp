import json
from unittest.mock import MagicMock, patch
from django.core.exceptions import ImproperlyConfigured

from django.test import TestCase, override_settings

from django_amqp.management.commands.amqp_worker import Worker


class AMQPWorkerTest(TestCase):
    @override_settings(SERVICEBUS_CONNECTION_STRING="DUMMY_CONN_STRING")
    def test_worker_initialization(self):
        queue_name = "test-queue"
        worker = Worker(queue_name=queue_name)
        self.assertEqual(worker.queue_name, queue_name)

    def test_worker_initialization_fails_no_conn_string(self):
        queue_name = "test-queue"
        with self.assertRaises(ImproperlyConfigured):
            Worker(queue_name=queue_name)

    @override_settings(SERVICEBUS_CONNECTION_STRING="DUMMY_CONN_STRING")
    @patch(
        "django_amqp.management.commands.amqp_worker.AzureServiceBusSubscriptionStreamer"
    )
    @patch("django_amqp.management.commands.amqp_worker.import_attribute")
    def test_process_message_success(self, mock_import_attribute, mock_streamer_class):
        mock_streamer = MagicMock()
        mock_streamer_class.return_value.__enter__.return_value = mock_streamer

        mock_task_func = MagicMock()
        # The real code finds @task functions by their module path
        # mock_task_func.func is the callable to be invoked
        mock_task_func.__module__ = "my_module"
        mock_task_func.__name__ = "my_function"
        mock_task_func.func = mock_task_func
        mock_import_attribute.return_value = mock_task_func

        worker = Worker(queue_name="test-queue")
        message_content = json.dumps(
            {
                "func": "my_module.my_function",
                "args": [1, 2],
                "kwargs": {"key": "value"},
            }
        )
        mock_message = MagicMock()
        mock_message.__str__.return_value = message_content

        worker.process_message(mock_message, mock_streamer)

        mock_import_attribute.assert_called_once_with("my_module.my_function")
        mock_task_func.assert_called_once_with(1, 2, key="value")
        mock_streamer.dead_letter_message.assert_not_called()
