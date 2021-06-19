from kafka import KafkaProducer
from time import sleep

producer = KafkaProducer(bootstrap_servers=['mipt-node06.atp-fivt.org:9092'],
                         value_serializer=lambda x:
                         x.encode('utf-8'))

inp = input()
while inp != 'exit':
    producer.send('had2020011-topic', value=str(inp))
    inp = input()