import pika
import json
import os
import time
import logging

# Logging for easier debugging.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_rabbitmq_connection():
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    parameters = pika.ConnectionParameters(rabbitmq_host)

    for attempt in range(10):  # Retry, because the rabbitmq service may not be ready yet.
        try:
            connection = pika.BlockingConnection(parameters)
            if connection.is_open:
                return connection
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Connection to RabbitMQ failed on attempt {attempt + 1}: {e}")
            time.sleep(5 * (attempt + 1))  # Increase wait time after each failure
    
    raise Exception("Failed to connect to RabbitMQ after several attempts")

def publish_event(queue_name, event):
    try:
        with get_rabbitmq_connection() as connection:
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(event),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                ))
    except Exception as e:
        logger.error(f"Failed to publish event to {queue_name}: {e}")

def consume_event(queue_name, callback):
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)

        def on_message(ch, method, properties, body):
            try:
                event = json.loads(body)
                callback(event)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=on_message)

        logger.info(f"Starting to consume from {queue_name}")
        channel.start_consuming()

    except Exception as e:
        logger.error(f"Failed to consume from {queue_name}: {e}")

    finally:
        if connection and connection.is_open:
            connection.close()
            logger.info("Connection closed")
