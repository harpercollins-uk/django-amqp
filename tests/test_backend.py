import json
from unittest.mock import MagicMock, patch

from azure.servicebus import ServiceBusMessage
from django.test import TestCase
from django.tasks.base import Task, task

from django_amqp.backend import ServiceBusBackend


@task(priority=1, queue_name="default")
def dummy_function():
    pass

    @property
    def module_path(self):
        return "my_module.dummy_function"


class ServiceBusBackendTest(TestCase):
    def setUp(self):
        self.backend = ServiceBusBackend(
            alias="default",
            params={"OPTIONS": {"connection_string": "dummy_connection_string"}},
        )
        self.mock_task = MagicMock(spec=Task)


    @patch("azure.servicebus.ServiceBusClient.from_connection_string")
    def test_enqueue_task(self, mock_servicebus):
        mock_sender = MagicMock()
        mock_servicebus.return_value.get_queue_sender.return_value = mock_sender
        self.backend.enqueue(dummy_function, args=("arg1",), kwargs={"key": "value"})

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

    @patch("azure.servicebus.ServiceBusClient.from_connection_string")
    def test_batch_enqueue_task(self, mock_servicebus):
        mock_sender = MagicMock()
        mock_batch = MagicMock()
        mock_sender.create_message_batch.return_value = mock_batch
        mock_servicebus.return_value.get_queue_sender.return_value = mock_sender

        # Fix: Each args should be a list, not a string
        self.backend.batch_enqueue(
            dummy_function,
            [(["arg1"], {"key1": "value1"}), (["arg2"], {"key2": "value2"})],
        )

        # Verify create_message_batch was called
        mock_sender.create_message_batch.assert_called_once()

        # Verify add_message was called twice (once for each message)
        self.assertEqual(mock_batch.add_message.call_count, 2)

        # Verify the batch was sent
        mock_sender.send_messages.assert_called_once_with(mock_batch)
