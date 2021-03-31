from kafka import KafkaConsumer


consumer = KafkaConsumer(
    'example_topic',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True)
     # group_id='my-group')
     # value_deserializer=lambda x: x.decode('utf-8'))

print(consumer.topics())

for message in consumer:
    message = message.value
    print(message)