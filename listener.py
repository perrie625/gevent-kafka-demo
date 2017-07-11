from gevent import monkey


monkey.patch_all()


from gevent import Greenlet
from kafka import KafkaConsumer



def listen(topic, server):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=server,
        group_id='gevent-listener'
    )
    print "%s start listen" % topic
    while True:
        record = consumer.next()
        msg = record.value.strip()
        print msg


if __name__ == '__main__':
    Greenlet.spawn(listen, 'a', '10.224.32.39:9092')
    Greenlet.spawn(listen, 'b', '10.224.32.39:9092').join()
