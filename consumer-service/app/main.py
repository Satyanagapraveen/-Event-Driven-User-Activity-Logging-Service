import logging
import os
import sys

from dotenv import load_dotenv

from app.services.message_broker import MessageBroker
from app.services.nosql_storage import NoSqlStorage
from app.services.event_processor import EventProcessor

load_dotenv()


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    mq_host = os.getenv("MQ_HOST", "localhost")
    mq_port = int(os.getenv("MQ_PORT", "5672"))
    mq_user = os.getenv("MQ_USER", "guest")
    mq_pass = os.getenv("MQ_PASS", "guest")
    mq_queue_name = os.getenv("MQ_QUEUE_NAME", "user_activity")

    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    mongo_db_name = os.getenv("MONGO_DB_NAME", "user_activity_logs")

    broker = MessageBroker(
        host=mq_host,
        port=mq_port,
        username=mq_user,
        password=mq_pass,
        queue_name=mq_queue_name,
    )

    storage = NoSqlStorage(
        mongo_uri=mongo_uri,
        db_name=mongo_db_name,
    )

    processor = EventProcessor(storage=storage)

    try:
        logger.info("Starting consumer service")
        broker.connect()
        storage.connect_sync()

        broker.start_consuming(callback=processor.process)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")

    except Exception:
        logger.exception("Fatal error in consumer service")

    finally:
        logger.info("Shutting down consumer service")
        broker.disconnect()
        storage.disconnect_sync()
        logger.info("Consumer service stopped")


if __name__ == "__main__":
    main()