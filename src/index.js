const { Producer, KafkaConsumer } = require('node-rdkafka');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');
const { backOff } = require('exponential-backoff');

const retryOptions = require('./utils/retryOptions');

/**
 * Kafka client which is a wrapper library around node-rdkafka
 *
 */
class KafkaClient {
  /**
   * The client identifier .
   * @type {String}
   * @private
   */
  #clientId;
  /**
   * The client group id string.
   * @type {String}
   * @private
   */
  #groupId;
  /**
   * The list of brokers.
   * @type {Array}
   * @private
   */
  #brokers;
  /**
   * The schema registry host.
   * @type {String}
   * @private
   */
  #avroSchemaRegistry;
  /**
   * The producer instance.
   * @type {Object}
   * @private
   */
  #producer;
  /**
   * The consumer instance.
   * @type {Object}
   * @private
   */
  #consumer;
  /**
   * The schema registry instance.
   * @type {Object}
   * @private
   */
  #registry;
  /**
   * The producer connection flag.
   * @type {Boolean}
   * @private
   */
  #isProducerConnected;
  /**
   * The consumer connection flag.
   * @type {Boolean}
   * @private
   */
  #isConsumerConnected;
  /**
   * The interval ID.
   * @type {number | NodeJS.Timeout | null}
   * @private
   */
  #intervalId;

  /**
   * Initialize Kafka Client
   * @constructor
   * @public
   * @param {Object} config The configuration object for kafka client initialization
   * @param {String} config.clientId The client identifier (default: 'default-client')
   * @param {String} config.groupId The client group id string. All clients sharing the same groupId belong to the same group (default: 'default-group-id')
   * @param {Array} config.brokers The initial list of brokers as a CSV list of broker host or host:port (default: ['localhost:9092'])
   * @param {String} config.avroSchemaRegistry The schema registry host for encoding and decoding the messages as per the avro schemas wrt a subject (default: 'http://localhost:8081')
   */
  constructor(config = {}) {
    this.#clientId = config.clientId || 'default-client';
    this.#groupId = config.groupId || 'default-group-id';
    this.#brokers = config.brokers || ['localhost:9092'];
    this.#avroSchemaRegistry =
      config.avroSchemaRegistry || 'http://localhost:8081';

    this.#producer = new Producer({
      'client.id': this.#clientId,
      'metadata.broker.list': this.#brokers.join(','),
      dr_cb: false,
    });

    this.#consumer = new KafkaConsumer(
      {
        'group.id': this.#groupId,
        'client.id': this.#clientId,
        'metadata.broker.list': this.#brokers.join(','),
        'enable.auto.commit': true,
        'auto.commit.interval.ms': 1000,
      },
      {
        'auto.offset.reset': 'earliest',
      },
    );

    this.#registry = new SchemaRegistry({ host: this.#avroSchemaRegistry });
  }

  /**
   * Connects to node-rdkakfa client's producer using an exponential backoff retry mechanism
   * @private
   */
  async #connectProducer() {
    try {
      await backOff(() => {
        return new Promise((resolve, reject) => {
          // Remove any previously attached listeners for these events
          this.#producer.removeAllListeners('ready');
          this.#producer.removeAllListeners('event.error');
          this.#producer.removeAllListeners('connection.failure');

          this.#producer.connect();

          this.#producer.once('ready', () => {
            this.#isProducerConnected = true;
            console.log('Kafka producer successfully connected');
            resolve();
          });

          this.#producer.once('event.error', (err) => {
            this.#isProducerConnected = false;
            console.error(`Kafka producer connection error: ${err}`);
            reject(err);
          });

          this.#producer.once('connection.failure', (err) => {
            this.#isProducerConnected = false;
            console.error(
              `Kafka producer connection resulted in failure: ${err}`,
            );
            reject(err);
          });
        }, retryOptions);
      });
    } catch (error) {
      throw new Error(`Error connecting to Kafka producer: ${error}`);
    }
  }

  /**
   * Connects to node-rdkakfa client's consumer using an exponential backoff retry mechanism
   * @private
   */
  async #connectConsumer() {
    try {
      await backOff(() => {
        return new Promise((resolve, reject) => {
          // Remove any previously attached listeners for these events
          this.#consumer.removeAllListeners('ready');
          this.#consumer.removeAllListeners('event.error');
          this.#consumer.removeAllListeners('connection.failure');

          this.#consumer.connect();

          this.#consumer.once('ready', () => {
            this.#isConsumerConnected = true;
            console.log('Kafka consumer successfully connected');
            resolve();
          });

          this.#consumer.once('event.error', (err) => {
            this.#isConsumerConnected = false;
            console.error(`Kafka consumer connection error: ${err}`);
            reject(err);
          });

          this.#consumer.once('connection.failure', (err) => {
            this.#isConsumerConnected = false;
            console.error(
              `Kafka consumer connection resulted in failure: ${err}`,
            );
            reject(err);
          });
        }, retryOptions);
      });
    } catch (error) {
      throw new Error(`Error connecting to Kafka consumer: ${error}`);
    }
  }

  /**
   * Wrapper function around #connectProducer where it first checks whether the producer has been connected previously
   * @private
   */
  async #initProducer() {
    try {
      if (!this.#isProducerConnected) {
        console.log('Kafka producer is not connected. Connecting producer');
        await this.#connectProducer();
      }
    } catch (error) {
      throw new Error(`Error initializing producer: ${error}`);
    }
  }

  /**
   * Wrapper function around #connectConsumer where it first checks whether the consumer has been connected previously
   * @private
   */
  async #initConsumer() {
    try {
      if (!this.#isConsumerConnected) {
        console.log('Kafka consumer is not connected. Connecting consumer');
        await this.#connectConsumer();
      }
    } catch (error) {
      throw new Error(`Error initializing consumer: ${error}`);
    }
  }

  /**
   * Sends an encoded message to a topic. Encodes the message data using this.#registry.encode
   * @param {String} topic The topic to send the message to
   * @param {Object} message The message to send to a topic
   * @public
   */
  async sendMessage(topic, message) {
    try {
      await this.#initProducer();

      if (this.#isProducerConnected) {
        const subject = `${topic}-value`;
        const id = await this.#registry.getRegistryId(subject, 'latest');

        console.log(`Using schema ${topic}-value@latest (id: ${id})`);

        const encodedMessage = await this.#registry.encode(id, message);

        this.#producer.produce(
          topic,
          null, // Partition, null for automatic partitioning
          Buffer.from(encodedMessage),
          `${topic}-schema`, // Key
        );

        console.log(`Successfully published data to topic: ${topic}`);
      } else {
        console.error('Major issue with the kafka producer init process.');
        throw new Error('Unable to initialize kafka producer');
      }
    } catch (error) {
      console.error(
        `Error occurred in sending message to topic ${topic}: ${error}`,
      );
      throw new Error(
        `Error occurred in sending message to topic ${topic}: ${error}`,
      );
    }
  }

  /**
   * Consumes a message from a topic. Decodes the message using this.#registry.decode
   * @param {String} topic The topic to consume the message from
   * @callback onMessage
   * @param {onMessage} onMessage A function that processes the decoded message data received by consumer
   * @public
   */
  async consumeMessage(topic, onMessage) {
    try {
      await this.#initConsumer();

      if (this.#isConsumerConnected) {
        this.#consumer.subscribe([topic]);
        console.log(`Subscribed to topic ${topic}`);

        if (!this.#intervalId) {
          this.#intervalId = setInterval(() => {
            this.#consumer.consume(10);
          }, 1000);
        }

        this.#consumer.on('data', async (data) => {
          try {
            const decodedValue = await this.#registry.decode(data.value);

            console.log(`Message received by consumer on topic: ${topic}`);

            onMessage({ value: decodedValue });
          } catch (error) {
            console.error(
              `Error occurred consuming messages from ${topic}: ${error}`,
            );
          }
        });
      } else {
        console.error('Major issue with the kafka consumer init process.');
        throw new Error('Unable to initialize kafka consumer');
      }
    } catch (error) {
      console.error(
        `Error occurred in consuming message from topic ${topic}: ${error}`,
      );
      clearInterval(this.#intervalId);
      this.#intervalId = null;
      throw new Error(
        `Error occurred in consuming message from topic ${topic}: ${error}`,
      );
    }
  }

  /**
   * Disconnects node-rdkafka client's producer and removes all associated listeners if any.
   * @public
   */
  async disconnectProducer() {
    try {
      if (this.#isProducerConnected) {
        return new Promise((resolve) => {
          this.#producer.disconnect();

          this.#producer.once('disconnected', () => {
            this.#isProducerConnected = false;
            this.#producer.removeAllListeners();

            console.log('Successfully disconnected Kafka producer');
            resolve();
          });
        });
      }
    } catch (error) {
      console.error(`Error disconnecting Kafka producer: ${error}`);
      throw new Error(`Error disconnecting Kafka producer: ${error}`);
    }
  }

  /**
   * Disconnects node-rdkafka client's consumer and removes all associated listeners if any.
   * @public
   */
  async disconnectConsumer() {
    try {
      if (this.#isConsumerConnected) {
        return new Promise((resolve) => {
          this.#consumer.disconnect();

          this.#consumer.once('disconnected', () => {
            this.#isConsumerConnected = false;
            this.#consumer.removeAllListeners();

            clearInterval(this.#intervalId);
            this.#intervalId = null;

            console.log('Successfully disconnected Kafka consumer');
            resolve();
          });
        });
      }
    } catch (error) {
      console.error(`Error disconnecting Kafka consumer: ${error}`);
      clearInterval(this.#intervalId);
      this.#intervalId = null;
      throw new Error(`Error disconnecting Kafka consumer: ${error}`);
    }
  }
}

module.exports = { KafkaClient };
