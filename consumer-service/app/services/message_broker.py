import logging
import signal
import time
from typing import Optional, Callable, Any

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError, ConnectionClosedByBroker

logger = logging.getLogger(__name__)


class MessageBroker:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        queue_name: str,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.queue_name = queue_name
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[BlockingChannel] = None
        self._should_stop = False

    def connect(self) -> None:
        credentials = pika.PlainCredentials(
            username=self.username,
            password=self.password,
        )
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials,
            heartbeat=30,
            blocked_connection_timeout=300,
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(
            queue=self.queue_name,
            durable=True,
        )
        self.channel.basic_qos(prefetch_count=1)
        logger.info(
            "Connected to RabbitMQ",
            extra={
                "host": self.host,
                "port": self.port,
                "queue": self.queue_name,
            },
        )

    def start_consuming(self, callback: Callable[[bytes], bool]) -> None:
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

        def wrapped_callback(
            ch: BlockingChannel,
            method: Any,
            properties: Any,
            body: bytes,
        ) -> None:
            success = callback(body)
            if success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                ch.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=False,
                )

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=wrapped_callback,
            auto_ack=False,
        )

        logger.info("Starting to consume messages")
        while not self._should_stop:
            try:
                self.connection.process_data_events(time_limit=1)
            except (AMQPConnectionError, ConnectionClosedByBroker):
                logger.error("Connection lost, attempting reconnect")
                self._reconnect()
            except Exception:
                logger.exception("Unexpected error in event loop")
                time.sleep(1)

        logger.info("Stopped consuming messages")
        self._cleanup()

    def _handle_sigterm(self, signum: int, frame: Any) -> None:
        logger.info(
            "Received shutdown signal",
            extra={"signal": signum},
        )
        self._should_stop = True

    def _reconnect(self) -> None:
        retry_delay = 2
        max_delay = 60
        while not self._should_stop:
            try:
                if self.connection and not self.connection.is_closed:
                    self.connection.close()
            except Exception:
                pass
            try:
                self.connect()
                logger.info("Reconnected to RabbitMQ")
                return
            except Exception:
                logger.warning(
                    "Reconnect failed, retrying",
                    extra={"retry_delay": retry_delay},
                )
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_delay)

    def _cleanup(self) -> None:
        if self.channel and self.channel.is_open:
            self.channel.close()
        if self.connection and self.connection.is_open:
            self.connection.close()
        logger.info("RabbitMQ connections closed")

    def disconnect(self) -> None:
        self._should_stop = True
        self._cleanup()