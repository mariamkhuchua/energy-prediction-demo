import pandas as pd
import numpy as np
import avro.schema

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import DoubleSerializer, StringSerializer
from confluent_kafka.admin import AdminClient, NewTopic

from time import sleep
from datetime import datetime

from simulator_utils import Consumption_Timestep, timestep_to_dict, white_noise

sleep(20)

# Load data
df = pd.read_csv("historical.csv")

# Create topic
admin_client = AdminClient({'bootstrap.servers': 'kafka:29092'})

topic_name = "simulated-consumption"
config = {'retention.ms': '86400000'}
topic = NewTopic(topic_name, num_partitions=1, replication_factor=1, config=config)
fs = admin_client.create_topics([topic])

# Connect to Schema Registry
schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Load Value Avro schema

simulated_consumption_value = avro.schema.parse(open("simulated-consumption-value.avsc", "rb").read())
simulated_consumption_value = str(simulated_consumption_value)

# Create serializers
value_serializer = AvroSerializer(schema_registry_client, simulated_consumption_value, timestep_to_dict)
#key_serializer = DoubleSerializer()
key_serializer = StringSerializer()

producer_conf = {'bootstrap.servers': 'kafka:29092',
                    'key.serializer': key_serializer,
                    'value.serializer': value_serializer}

producer = SerializingProducer(producer_conf)

# Produce to topic
now = datetime.now()
current_ts = datetime.timestamp(now) // 1
current_ts = float(current_ts)
quarter_ts = 15 * 60

while True:
    mw_noise = white_noise(1, 5, len(df), 0)
    temperature_noise = white_noise(1, 5, len(df), 0)

    for index, row in df.iterrows():
        mw = (row["mW"] + mw_noise[index]).astype(float)
        mw = round(mw, 3)
        temperature = (row["Temperature"] + temperature_noise[index]).astype(float)
        temperature = round(temperature,3)
        daytime = row["Daytime"]
        timestep = Consumption_Timestep(mw, temperature, daytime)
        str_ts = str(current_ts)
        producer.produce(topic=topic_name, key=str_ts, value=timestep)
        producer.poll(0)
        current_ts = current_ts + quarter_ts
        sleep(2)

producer.flush()
