from gevent import monkey

monkey.patch_all()


from kafka import KafkaProducer
from gevent import Greenlet
import random
import gevent


def produce(client, topic):
    print "%s topic start produce message" % topic
    while True:
        client.send(topic, "hello, %s." % topic)
        gevent.sleep(random.random() * 5)


if __name__ == '__main__':
    client = KafkaProducer(bootstrap_servers="10.224.32.39:9092")
    Greenlet.spawn(produce, client, 'a')
    Greenlet.spawn(produce, client, 'b').join()
