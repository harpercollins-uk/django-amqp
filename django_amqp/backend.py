import json
from typing import Any, TypeVar

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from django.utils import timezone
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import Task
from pydantic import BaseModel
from typing_extensions import ParamSpec
from django.core.exceptions import ImproperlyConfigured
from abc import abstractmethod

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

        message_content = self._prepare_message(task, args, kwargs)

        if task.run_after:
            if task.run_after <= timezone.now():
                raise ValueError("schedule_time must be in the future")
            return self._send_scheduled_message(
                task.queue_name, message_content, task.run_after
            )

        self._send_message(task.queue_name, message_content)

    def batch_enqueue(self, task: Task, jobs_data: list[tuple[P.args, P.kwargs]]):
        """
        Enqueue multiple tasks in a batch for efficiency.
        """
        messages = []
        for task_args, task_kwargs in jobs_data:
            message_content = self._prepare_message(task, task_args, task_kwargs)
            messages.append(message_content)

        self._send_batch_messages(task.queue_name, messages)

    def _prepare_message(self, task: Task, args: P.args, kwargs: P.kwargs) -> str:
        """Prepare the message content as JSON string"""
        return json.dumps(
            TaskStructure(
                func=task.module_path,
                args=args,
                kwargs=kwargs,
            ).model_dump()
        )

    @abstractmethod
    def _send_message(self, queue_name: str, message_content: str) -> None:
        """Send a single message to the queue"""
        pass

    @abstractmethod
    def _send_scheduled_message(
        self, queue_name: str, message_content: str, scheduled_time
    ) -> int:
        """Send a scheduled message to the queue. Returns message ID."""
        pass

    @abstractmethod
    def _send_batch_messages(self, queue_name: str, messages: list[str]) -> None:
        """Send multiple messages in a batch"""
        pass


class ServiceBusBackend(AMQPBackend):
    """
    Azure Service Bus implementation of the AMQP backend.
    """

    def __init__(self, alias: str, params: dict):
        super().__init__(alias, params)
        self._client = None
        if not params.get("OPTIONS") or not params.get("OPTIONS").get(
            "connection_string"
        ):
            raise ImproperlyConfigured(
                "ServiceBusBackend requires 'connection_string' in options"
            )
        self.connection_string = params["OPTIONS"].get("connection_string")

    @property
    def client(self) -> ServiceBusClient:
        """Lazy initialization of ServiceBusClient"""
        if self._client is None:
            self._client = ServiceBusClient.from_connection_string(
                conn_str=self.connection_string
            )
        return self._client

    def _send_message(self, queue_name: str, message_content: str) -> None:
        """Send a single message to Azure Service Bus"""
        sender = self.client.get_queue_sender(queue_name=queue_name)
        message = ServiceBusMessage(message_content)
        sender.send_messages(message)

    def _send_scheduled_message(
        self, queue_name: str, message_content: str, scheduled_time
    ) -> int:
        """
        Send a scheduled message to Azure Service Bus.
        Returns the servicebus sequence number (i.e. unique id of the
        scheduled message). This can be used to cancel the scheduled message.
        """
        sender = self.client.get_queue_sender(queue_name=queue_name)
        message = ServiceBusMessage(message_content)
        return sender.schedule_messages(message, scheduled_time)

    def _send_batch_messages(self, queue_name: str, messages: list[str]) -> None:
        """Send multiple messages in a batch to Azure Service Bus"""
        sender = self.client.get_queue_sender(queue_name=queue_name)
        batch_message = sender.create_message_batch()

        messages_to_send = messages.copy()
        while messages_to_send:
            message_content = messages_to_send.pop(0)
            try:
                batch_message.add_message(ServiceBusMessage(message_content))
            except ValueError:
                # Batch is full, send it and create a new one
                sender.send_messages(batch_message)
                batch_message = sender.create_message_batch()
                batch_message.add_message(ServiceBusMessage(message_content))

        # Send remaining messages
        sender.send_messages(batch_message)

    def close(self):
        """Close the ServiceBusClient connection"""
        if self._client is not None:
            self._client.close()
            self._client = None

    def __del__(self):
        """Cleanup on deletion"""
        self.close()
