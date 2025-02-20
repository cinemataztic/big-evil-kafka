import { KafkaConsumer } from 'node-rdkafka';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { backOff } from 'exponential-backoff';

import { avroSchemaRegistry, clientId, brokers } from './utils/config';
import retryOptions from './utils/retry';

/* this manages the consumer state
    as 'ready' event is not re-fired on already connected consumers. */
let isConsumerConnected = false;
let kafkaConsumerIntervalId = null;

const registry = new SchemaRegistry({
  host: avroSchemaRegistry,
});

const consumerGlobalConfig = {
  'group.id': groupId,
  'client.id': clientId,
  'metadata.broker.list': brokers.join(','),
  'enable.auto.commit': true,
  'auto.commit.interval.ms': 1000,
};

const consumerTopicConfig = {};

const consumer = new KafkaConsumer(consumerGlobalConfig, consumerTopicConfig);

const connect = async () => {
  try {
    await backOff(() => {
      return new Promise((resolve, reject) => {
        consumer.connect();

        consumer.once('ready', () => {
          isConsumerConnected = true;
          console.log('Kafka consumer successfully connected');
          resolve();
        });

        consumer.once('event.error', (err) => {
          isConsumerConnected = false;
          console.error(`Kafka consumer connection error: ${err.message}`);
          reject(err);
        });
      }, retryOptions);
    });
  } catch (error) {
    console.error(`Error connecting to Kafka consumer: ${error}`);
  }
};

const connectConsumer = async () => {
  if (!isConsumerConnected) {
    console.warn('Kafka consumer is not connected. Retrying...');
    await connect();
  }
};

export const consumeMessage = async (topic, onMessage) => {
  try {
    await connectConsumer();

    if (isConsumerConnected) {
      consumer.subscribe([topic]);
      console.log(`Subscribed to topic ${topic}`);

      if (!kafkaConsumerIntervalId) {
        kafkaConsumerIntervalId = setInterval(function () {
          consumer.consume(10); // Read 10 messages every 1000 milliseconds.
        }, 1000);
      }

      consumer.on('data', async (data) => {
        try {
          const decodedValue = await registry.decode(data.value);

          console.log(`Message received by consumer on topic: ${topic}`);

          onMessage({ value: decodedValue });
        } catch (error) {
          console.error(
            `Error occurred consuming messages from ${topic}: ${error}`,
          );
        }
      });
    }
  } catch (error) {
    console.error(`Error occurred in consume: ${error}`);
    // Clear Kafka Consumer Polling Interval and set the value to null in case any error occurs in consume method
    clearInterval(kafkaConsumerIntervalId);
    kafkaConsumerIntervalId = null;
  }
};

export const disconnectConsumer = async () => {
  try {
    consumer.disconnect();
    consumer.removeAllListeners();
    isConsumerConnected = false;

    // Clear Kafka Consumer Polling Interval and set the value to null
    clearInterval(kafkaConsumerIntervalId);
    kafkaConsumerIntervalId = null;
    console.log('Successfully disconnected Kafka consumer');
  } catch (error) {
    console.error(`Error disconnecting Kafka consumer: ${error}`);
  }
};
