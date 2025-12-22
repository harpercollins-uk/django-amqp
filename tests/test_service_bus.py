from unittest.mock import MagicMock, patch

from django.test import TestCase, override_settings

from django_amqp.service_bus import (
    AzureServiceBusSubscriptionStreamer,
    cancel_deferred_message,
)


@override_settings(SERVICEBUS_CONNECTION_STRING="DUMMY_CONN_STRING")
@patch("django_amqp.service_bus.AutoLockRenewer")
@patch("django_amqp.service_bus.ServiceBusClient")
@patch("django_amqp.service_bus.ServiceBusReceivedMessage")
@patch("django_amqp.service_bus.time.sleep")
class AzureServiceBusSubscriptionStreamerTests(TestCase):
    def test_connect_success(
        self, mock_sleep, _, mock_servicebus_client, mock_auto_lock_renewer
    ):
        mock_servicebus_client.from_connection_string.return_value = MagicMock()

        streamer = AzureServiceBusSubscriptionStreamer(
            "DUMMY_CONN_STRING", "DUMMY_QUEUE"
        )
        streamer.connect()

        self.assertIsNotNone(streamer.client)
        self.assertIsNotNone(streamer.auto_lock_renewer)
        self.assertIsNotNone(streamer.receiver)

    def test_connect_retry(
        self, mock_sleep, _, mock_servicebus_client, mock_auto_lock_renewer
    ):
        mock_servicebus_client.from_connection_string.side_effect = [
            Exception("API failure"),
            MagicMock(),
        ]

        streamer = AzureServiceBusSubscriptionStreamer(
            "DUMMY_CONN_STRING", "DUMMY_QUEUE"
        )
        streamer.connect()

        self.assertEqual(mock_servicebus_client.from_connection_string.call_count, 2)

        self.assertIsNotNone(streamer.client)
        self.assertIsNotNone(streamer.auto_lock_renewer)
        self.assertIsNotNone(streamer.receiver)

    def test_connect_failure(
        self, mock_sleep, _, mock_servicebus_client, mock_auto_lock_renewer
    ):
        mock_servicebus_client.from_connection_string.side_effect = [
            Exception("API failure")
        ] * 3

        streamer = AzureServiceBusSubscriptionStreamer(
            "DUMMY_CONN_STRING", "DUMMY_QUEUE"
        )
        with self.assertRaises(ConnectionError):
            streamer.connect()

        self.assertEqual(mock_servicebus_client.from_connection_string.call_count, 3)

        self.assertIsNone(streamer.client)
        self.assertIsNone(streamer.auto_lock_renewer)
        self.assertIsNone(streamer.receiver)

    def test_stream_messages_success(
        self,
        mock_sleep,
        mock_recieved_message,
        mock_servicebus_client,
        mock_auto_lock_renewer,
    ):
        streamer = AzureServiceBusSubscriptionStreamer(
            "DUMMY_CONN_STRING", "DUMMY_QUEUE"
        )

        mock_receiver = MagicMock()
        mock_receiver.receive_messages.side_effect = [
            ["dummy message"],
            # give an empty message to stop the while loop
            [],
        ]

        streamer.receiver = mock_receiver
        streamer.auto_lock_renewer = mock_auto_lock_renewer

        result = list(streamer.stream_messages())

        self.assertEqual(result, ["dummy message"])

    def test_stream_messages_no_messages(
        self,
        mock_sleep,
        mock_recieved_message,
        mock_servicebus_client,
        mock_auto_lock_renewer,
    ):
        streamer = AzureServiceBusSubscriptionStreamer(
            "DUMMY_CONN_STRING", "DUMMY_QUEUE"
        )

        mock_receiver = MagicMock()
        mock_receiver.receive_messages.side_effect = [
            [],
        ]

        streamer.receiver = mock_receiver
        streamer.auto_lock_renewer = mock_auto_lock_renewer

        result = list(streamer.stream_messages())

        self.assertEqual(result, [])

    def test_get_queue_count_upto_10(
        self,
        mock_sleep,
        mock_recieved_message,
        mock_servicebus_client,
        mock_auto_lock_renewer,
    ):
        streamer = AzureServiceBusSubscriptionStreamer(
            "DUMMY_CONN_STRING", "DUMMY_QUEUE"
        )
        mock_receiver = MagicMock()
        mock_receiver.peek_messages.return_value = ["dummy message"] * 10

        streamer.receiver = mock_receiver

        result = streamer.get_queue_count_upto_10()

        self.assertEqual(result, 10)

    def test_complete_message(
        self,
        mock_sleep,
        mock_recieved_message,
        mock_servicebus_client,
        mock_auto_lock_renewer,
    ):
        streamer = AzureServiceBusSubscriptionStreamer(
            "DUMMY_CONN_STRING", "DUMMY_QUEUE"
        )
        mock_receiver = MagicMock()
        dummy_message = "dummy message"

        streamer.receiver = mock_receiver

        streamer.complete_message(dummy_message)

        mock_receiver.complete_message.assert_called_once_with(message=dummy_message)

    def test_dead_letter_message(
        self,
        mock_sleep,
        mock_recieved_message,
        mock_servicebus_client,
        mock_auto_lock_renewer,
    ):
        streamer = AzureServiceBusSubscriptionStreamer(
            "DUMMY_CONN_STRING", "DUMMY_QUEUE"
        )
        mock_receiver = MagicMock()
        dummy_message = "dummy message"
        dummy_reason = "dummy reason"
        dummy_error_description = "dummy description"

        streamer.receiver = mock_receiver

        streamer.dead_letter_message(
            dummy_message, dummy_reason, dummy_error_description
        )

        mock_receiver.dead_letter_message.assert_called_once_with(
            dummy_message,
            reason=dummy_reason,
            error_description=dummy_error_description,
        )

    def test_close(
        self,
        mock_sleep,
        mock_recieved_message,
        mock_servicebus_client,
        mock_auto_lock_renewer,
    ):
        streamer = AzureServiceBusSubscriptionStreamer(
            "DUMMY_CONN_STRING", "DUMMY_QUEUE"
        )
        mock_receiver = MagicMock()
        mock_client = MagicMock()

        streamer.receiver = mock_receiver
        streamer.client = mock_client

        streamer.close()

        streamer.receiver.close.assert_called_once()
        streamer.client.close.assert_called_once()


@override_settings(SERVICEBUS_CONNECTION_STRING="DUMMY_CONN_STRING")
@patch("django_amqp.service_bus.ServiceBusClient")
class CancelDeferredMessageTests(TestCase):
    def test_cancel_deferred_message(self, mock_servicebus_client):
        # Set up the mock servicebus client instance
        mock_client_instance = MagicMock()
        mock_servicebus_client.from_connection_string.return_value = (
            mock_client_instance
        )

        mock_sender = MagicMock()
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__.return_value = mock_sender
        mock_client_instance.get_queue_sender.return_value = mock_context_manager

        dummy_sequence_number = 123
        dummy_queue_name = "dummy_queue"

        result = cancel_deferred_message(dummy_sequence_number, dummy_queue_name)

        self.assertIsNone(result)
        mock_client_instance.get_queue_sender.assert_called_once_with(
            queue_name=dummy_queue_name
        )
        mock_sender.cancel_scheduled_messages.assert_called_once_with(
            dummy_sequence_number
        )
