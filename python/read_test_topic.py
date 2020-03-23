from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException

import sys 


conf = {'bootstrap.servers': "34.68.195.254 :9092",
        'group.id': "foo" #, 'auto.offset.reset': 'smallest'
        }

consumer = Consumer(conf)

print('Consumer created ...')

running = True

def msg_process(msg):
    print("asdasdasdasd", msg)


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        print("subscribed...")

        while running:
            print("Getting msg...")
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


basic_consume_loop(consumer, ['users'])