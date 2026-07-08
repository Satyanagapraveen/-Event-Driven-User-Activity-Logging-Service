import pika
import json
import datetime
import time

def publish_event(channel, event_data):
    channel.basic_publish(
        exchange='',
        routing_key='user_activity',
        body=json.dumps(event_data).encode('utf-8'),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    print(f" [x] Sent '{event_data['event_type']}' event for user '{event_data['user_id']}'")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='user_activity', durable=True)

    events = [
        {
            "user_id": "user-001",
            "event_type": "login",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "details": {"ip_address": "192.168.1.1"}
        },
        {
            "user_id": "user-002",
            "event_type": "view_product",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "details": {"product_id": "prod-abc", "category": "electronics"}
        },
        {
            "user_id": "user-001",
            "event_type": "add_to_cart",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "details": {"product_id": "prod-xyz", "quantity": 2}
        },
        {
            "user_id": "user-003",
            "event_type": "register",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "details": {"email": "user3@example.com"}
        },
        {
            "user_id": "user-002",
            "event_type": "purchase",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "details": {"order_id": "ord-789", "total": 99.99}
        }
    ]

    for event in events:
        publish_event(channel, event)
        time.sleep(0.5)

    connection.close()
    print(" [✓] All events published successfully")

if __name__ == '__main__':
    main()