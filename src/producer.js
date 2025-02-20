import { Producer } from 'node-rdkafka';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

import { avroSchemaRegistry, clientId, brokers } from './utils/config';
import logger from './utils/logger';
import retryOptions from './utils/retry';

/* this manages the producer state
    as 'ready' event is not re-fired on already connected consumers. */
let isProducerConnected = false;

const registry = new SchemaRegistry({
  host: avroSchemaRegistry,
});

const producerGlobalConfig = {
  'client.id': clientId,
  'metadata.broker.list': brokers.join(','),
  dr_cb: false,
};

const producer = new Producer(producerGlobalConfig);

const connect = async () => {
  try {
    await backOff(() => {
      return new Promise((resolve, reject) => {
        producer.connect();

        isProducerConnected = true;
        logger.info('Kafka producer successfully connected');
        resolve();

        producer.once('event.error', (err) => {
          isProducerConnected = false;
          logger.error(`Kafka producer connection error: ${err.message}`);
          reject(err);
        });
      }, retryOptions);
    });
  } catch (error) {
    logger.error(`Error connecting to Kafka producer: ${error}`);
  }
};

const connectProducer = async () => {
  if (!isProducerConnected) {
    logger.warn('Kafka producer is not connected. Retrying...');
    await this.connect();
  }
};

export const sendMessage = async (topic, message) => {
  await connectProducer();

  if (isProducerConnected) {
    const subject = `${topic}-value`;
    const id = await registry.getRegistryId(subject, 'latest');

    logger.info(`Using schema ${topic}-value@latest (id: ${id})`);

    const encodedMessage = await registry.encode(id, message);

    producer.produce(
      topic,
      null, // Partition, null for automatic partitioning
      Buffer.from(encodedMessage),
      `${topic}-schema`, // Key
    );

    logger.info(`Successfully published data to topic: ${topic}`);
  } else {
    logger.error('Major issue with the kafka producer init process.');
  }
};

export const disconnectProducer = async () => {
  try {
    producer.disconnect();
    producer.removeAllListeners();
    isProducerConnected = false;

    logger.info('Successfully disconnected Kafka producer');
  } catch (error) {
    logger.error(`Error disconnecting Kafka producer: ${error}`);
  }
};
