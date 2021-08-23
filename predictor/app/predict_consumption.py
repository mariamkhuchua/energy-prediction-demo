import tensorflow as tf
import numpy as np

import avro.schema
from time import sleep

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import SerializingProducer

from simulator_utils import Consumption_Timestep, timestep_to_dict, dict_to_timestep

# Wait for the simulator to start producing
sleep(20)

# Load model
model = tf.keras.models.load_model("model")

n_prediction_ts = model.layers[-1].output_shape[1]

# Create prediction topic
admin_client = AdminClient({'bootstrap.servers': 'kafka:29092'})

predicted_consumption_topic = "predicted-consumption"
config = {'cleanup.policy': 'compact',
          'delete.retention.ms': '30000'}

topic = NewTopic(predicted_consumption_topic, num_partitions=1, replication_factor=1, config=config)
fs = admin_client.create_topics([topic])

predicted_consumption_value = avro.schema.parse(open("predicted-consumption-value.avsc", "rb").read())
predicted_consumption_value = str(predicted_consumption_value)

# Connect to Schema Registry
schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

simulated_consumption_value = avro.schema.parse(open("simulated-consumption-value.avsc", "rb").read())
simulated_consumption_value = str(simulated_consumption_value)

# Create deserializers
value_deserializer = AvroDeserializer(schema_registry_client, schema_str=simulated_consumption_value, from_dict=dict_to_timestep)
key_deserializer = StringDeserializer()

consumer_conf = {'bootstrap.servers': 'kafka:29092',
                    'key.deserializer': key_deserializer,
                    'value.deserializer': value_deserializer,
                    'group.id': 'predictor-consumer',
                    'auto.offset.reset': "earliest"}

simulated_consumption_topic = "simulated-consumption"

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe([simulated_consumption_topic])

# Create predictions producer
value_serializer = AvroSerializer(schema_registry_client, predicted_consumption_value)
key_serializer = StringSerializer()

producer_conf = {'bootstrap.servers': 'kafka:29092',
                    'key.serializer': key_serializer,
                    'value.serializer': value_serializer}

producer = SerializingProducer(producer_conf)

# Make predictions on a sliding window of simulated data and produce them as a batch
quarter_ts = 15 * 60

batch = 0
ts = []
data = []

while True:
    message = consumer.poll(1)
    if message is None:
            continue
    else:
        batch += 1
        # add new data point to the list
        msg_dict = timestep_to_dict(message.value())
        value_list = list(msg_dict.values())
        data.append(value_list)
        # add new timestamp to the list
        msg_ts = message.key()
        msg_ts = float(msg_ts)
        ts.append(msg_ts)

        # if there is enough input data
        if batch > 47:
            # transform data to be input into prediction model
            input_data = np.array(data)
            input_data = np.expand_dims(input_data, axis=0)
            # get predictions
            predicted_values = model(input_data)
            predicted_values = predicted_values[:,:,:1]
            predicted_values = tf.reshape(predicted_values, [n_prediction_ts]).numpy()
            # compute timestamps for predicted values
            predicted_ts = [msg_ts+(quarter_ts*x) for x in range(1,n_prediction_ts+1)]
            # send each predicted timestep as an individual message into compacted topic
            for timestep, predicted_value in zip(predicted_ts, predicted_values):
                predicted_ts_dict = {"predicted_mw": predicted_value}
                key_ts = str(timestep)
                producer.produce(topic=predicted_consumption_topic, key=key_ts, value=predicted_ts_dict)
                producer.poll(0)
            # remove oldest data point
            batch -= 1
            ts.pop(0)
            data.pop(0)
            continue
