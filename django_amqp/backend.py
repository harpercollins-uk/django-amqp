import json
from typing import Any, TypeVar

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from django.conf import settings
from django.utils import timezone
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import Task
from pydantic import BaseModel
from typing_extensions import ParamSpec
from django.core.exceptions import ImproperlyConfigured

T = TypeVar("T")
P = ParamSpec("P")


class TaskStructure(BaseModel):
    """
    This model defines the text structure of the ServiceBusMessage

    It should be used to encode and decode messages sent to background worker queues
    """

    func: str
    args: list[Any]
    kwargs: dict[str, Any]


class AMQPBackend(BaseTaskBackend):
    supports_defer = True

    def __init__(self, alias: str, params: dict):
        super().__init__(alias, params)
        self.tasks = {}
        try:
            conn_str = settings.SERVICEBUS_CONNECTION_STRING
            if not conn_str:
                raise AttributeError
        except AttributeError:
            raise ImproperlyConfigured(
                "SERVICEBUS_CONNECTION_STRING should be set for the AMQP worker."
            )

    def enqueue(
        self,
        task: Task,
        args: P.args,
        kwargs: P.kwargs,
    ) -> None | int:
        """
        Container apps jobs are queued to Azure Service Bus

        Tasks can be scheduled for later by including a utc time to send
        This must be in the future at time of sending message
        """

        servicebus_client = ServiceBusClient.from_connection_string(
            conn_str=settings.SERVICEBUS_CONNECTION_STRING
        )

        sender = servicebus_client.get_queue_sender(queue_name=task.queue_name)

        message_content = json.dumps(
            TaskStructure(
                func=task.module_path,
                args=args,
                kwargs=kwargs,
            ).model_dump()
        )

        message = ServiceBusMessage(message_content)

        if task.run_after:
            if task.run_after <= timezone.now():
                raise ValueError("schedule_time must be in the future")
            # this returns the servicebus sequence number (i.e. unique id of the
            # scheduled message). This can be used to cancel the scheduled message
            return sender.schedule_messages(message, task.run_after)

        sender.send_messages(message)

    def batch_enqueue(self, task: Task, jobs_data: list[tuple[P.args, P.kwargs]]):
        with ServiceBusClient.from_connection_string(
            conn_str=settings.SERVICEBUS_CONNECTION_STRING
        ) as servicebus_client:
            sender = servicebus_client.get_queue_sender(queue_name=task.queue_name)
            batch_message = sender.create_message_batch()

            while jobs_data:
                task_args, task_kwargs = jobs_data.pop(0)

                message_content = json.dumps(
                    TaskStructure(
                        func=task.module_path,
                        args=task_args,
                        kwargs=task_kwargs,
                    ).model_dump()
                )

                try:
                    batch_message.add_message(ServiceBusMessage(message_content))
                except ValueError:
                    sender.send_messages(batch_message)
                    batch_message = sender.create_message_batch()
                    batch_message.add_message(ServiceBusMessage(message_content))

            sender.send_messages(batch_message)

    def validate_task(self, task: Task) -> None:
        """
        Add task to task register and validate task.

        Determine whether the provided task is one which can be executed by the backend.
        """

        super().validate_task(task)
        self.tasks[task.func.__qualname__] = task.func
