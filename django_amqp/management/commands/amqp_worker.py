import json
import logging
from argparse import ArgumentParser

from django.conf import settings
from django.core.management.base import BaseCommand
from pydantic import ValidationError

from django_amqp.backend import TaskStructure
from django_amqp.service_bus import AzureServiceBusSubscriptionStreamer
from django_amqp.utils import import_attribute
from django.core.exceptions import ImproperlyConfigured

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        *,
        queue_name: str,
    ):
        self.queue_name = queue_name
        try:
            conn_str = settings.SERVICEBUS_CONNECTION_STRING
            if not conn_str:
                raise AttributeError
        except AttributeError:
            raise ImproperlyConfigured(
                "SERVICEBUS_CONNECTION_STRING should be set for the AMQP worker."
            )

    def run(self) -> None:
        with AzureServiceBusSubscriptionStreamer(
            connection_string=settings.SERVICEBUS_CONNECTION_STRING,
            queue_name=self.queue_name,
        ) as message_streamer:
            for message in message_streamer.stream_messages():
                self.process_message(message, message_streamer)

    def process_message(self, message, message_streamer):
        """
        Process an individual message from the queue.
        """
        try:
            task_data = TaskStructure(**json.loads(str(message)))
        except ValidationError:
            logger.exception("Invalid task structure in ServiceBus message")
            message_streamer.dead_letter_message(
                message, reason="Invalid task structure"
            )
            return

        try:
            task_func = import_attribute(task_data.func)
        except ValueError as e:
            logger.error(e)
            message_streamer.dead_letter_message(message, reason=e)
            return

        try:
            logger.info(f"Running background task: {task_data.func}")
            task_func.func(*task_data.args, **task_data.kwargs)
        except Exception as e:
            logger.exception(f"Task {task_data.func} failed")
            message_streamer.dead_letter_message(
                message,
                reason=f"Task {task_data.func} failed",
                error_description=str(e),
            )
        else:
            message_streamer.complete_message(message)


class Command(BaseCommand):
    help = "Run a database background worker"

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--queue-name",
            nargs="?",
            default=None,
            type=str,
            help="The queue to process.",
        )

        parser.add_argument(
            "--burst",
            action="store_true",
            default=False,
            help=(
                "Run the worker in burst mode "
                "(process all available tasks and then exit)."
            ),
        )



    def handle(self, *args, **options) -> None:
        queue_name = options.get("queue_name")
        if queue_name is None:
            raise ImproperlyConfigured(
                "You must specify a --queue-name for the AMQP worker."
            )
        if not options.get("burst"):
            raise NotImplemented(
                "The AMQP worker only supports --burst mode currently."
            )
        worker = Worker(
            queue_name=queue_name,
        )
        worker.run()
