from kafka import KafkaConsumer


consumer = KafkaConsumer(
    'had2020011-out',
     bootstrap_servers=['mipt-node06.atp-fivt.org:9092'],
     enable_auto_commit=True)


for message in consumer:
    message = message.value
    print(message)