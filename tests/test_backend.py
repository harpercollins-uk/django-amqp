import json
from unittest.mock import MagicMock, patch

from azure.servicebus import ServiceBusMessage
from django.test import TestCase, override_settings
from django.tasks.base import Task, task

from django_amqp.backend import AMQPBackend


@task(priority=1, queue_name="default")
def dummy_function():
    pass

    @property
    def module_path(self):
        return "my_module.dummy_function"


class AMQPBackendTest(TestCase):
    @override_settings(SERVICEBUS_CONNECTION_STRING="DUMMY_CONN_STRING")
    @patch("azure.servicebus.ServiceBusClient.from_connection_string")
    def test_enqueue_task(self, mock_servicebus):
        backend = AMQPBackend(alias="default", params={})
        mock_sender = MagicMock()
        mock_servicebus.return_value.get_queue_sender.return_value = mock_sender
        backend.enqueue(dummy_function, args=("arg1",), kwargs={"key": "value"})

        # Ensure message is correctly structured
        expected_message_content = json.dumps(
            {
                "func": dummy_function.module_path,
                "args": ["arg1"],
                "kwargs": {"key": "value"},
            }
        )

        # Ensure the message was sent
        mock_sender.send_messages.assert_called_once()
        sent_message = mock_sender.send_messages.call_args[0][0]
        self.assertIsInstance(sent_message, ServiceBusMessage)
        self.assertEqual(str(sent_message), expected_message_content)
