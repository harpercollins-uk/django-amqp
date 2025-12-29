import logging
import time
from typing import Generator

from azure.servicebus import (
    AutoLockRenewer,
    ServiceBusClient,
    ServiceBusReceivedMessage,
)
from django.conf import settings

logger = logging.getLogger(__name__)


class AzureServiceBusSubscriptionStreamer:
    def __init__(
        self,
        connection_string: str,
        queue_name: str = None,
    ):
        self.connection_string = connection_string
        self.queue_name = queue_name
        self.max_wait_time = 20
        self.client = None
        self.receiver = None
        self.messages = None
        self.auto_lock_renewer = None

    def connect(self, retries: int = 3, delay: int = 5):
        for attempt in range(retries):
            try:
                self.client = ServiceBusClient.from_connection_string(
                    conn_str=self.connection_string
                )
                self.auto_lock_renewer = AutoLockRenewer(max_lock_renewal_duration=1200)
                self.receiver = self.client.get_queue_receiver(
                    queue_name=self.queue_name,
                    auto_lock_renewer=self.auto_lock_renewer,
                    max_wait_time=self.max_wait_time,
                )
                break
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(delay)
        else:
            raise ConnectionError(
                "Failed to connect to Azure ServiceBus after multiple attempts."
            )

    def stream_messages(self) -> Generator[ServiceBusReceivedMessage]:
        while True:
            self.messages = self.receiver.receive_messages(
                max_message_count=1,
                max_wait_time=20,
            )
            if not self.messages:
                break
            message = self.messages[0]
            self.auto_lock_renewer.register(
                self.receiver, message, max_lock_renewal_duration=600
            )
            yield message

    def get_queue_count_upto_10(self) -> int:
        return len(self.receiver.peek_messages(max_message_count=10))

    def complete_message(self, message):
        self.receiver.complete_message(message=message)

    def dead_letter_message(
        self, message, reason: str | None = None, error_description: str | None = None
    ):
        self.receiver.dead_letter_message(
            message, reason=reason, error_description=error_description
        )

    def close(self):
        self.receiver.close()
        self.client.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def cancel_deferred_message(sequence_number: int, queue_name: str):
    servicebus_client = ServiceBusClient.from_connection_string(
        conn_str=settings.SERVICEBUS_CONNECTION_STRING
    )

    with servicebus_client.get_queue_sender(queue_name=queue_name) as sender:
        sender.cancel_scheduled_messages(sequence_number)
