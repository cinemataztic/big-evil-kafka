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
   * @param {String} config.clientId The client identifier (default: 'default-client-id')
   * @param {String} config.groupId The client group id string. All clients sharing the same groupId belong to the same group (default: 'default-group-id')
   * @param {Array} config.brokers The initial list of brokers as a CSV list of broker host or host:port (default: ['localhost:9092'])
   * @param {String} config.avroSchemaRegistry The schema registry host for encoding and decoding the messages as per the avro schemas wrt a subject (default: 'http://localhost:8081')
   */
  constructor(config = {}) {
    this.#clientId = config.clientId || 'default-client-id';
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
    this.#registerProducerEventListeners();
    this.#registerConsumerEventListeners();
  }

  #registerProducerEventListeners() {
    this.#producer.once('disconnected', async () => {
      if (this.#isProducerConnected) {
        console.error('Kafka producer disconnected unexpectedly.');
        this.#isProducerConnected = false;
      }
    });

    this.#producer.once('event.error', async (error) => {
      console.error(`Kafka producer encountered event error: ${error}`);
      this.#isProducerConnected = false;
    });
  }

  #registerConsumerEventListeners() {
    this.#consumer.once('disconnected', async () => {
      if (this.#isConsumerConnected) {
        console.error(
          'Kafka consumer disconnected unexpectedly. Retrying kafka consumer connection...',
        );

        this.#isConsumerConnected = false;
        clearInterval(this.#intervalId);
        this.#intervalId = null;

        try {
          await this.#connectConsumer();
        } catch (error) {
          console.error(
            `Kafka consumer re-connection failed with error ${error.message}. Max retries reached. Exiting...`,
          );
          process.exit(1);
        }
      }
    });

    this.#consumer.once('event.error', async (error) => {
      console.error(
        `Kafka consumer encountered event error: ${error}. Retrying kafka consumer connection...`,
      );

      this.#isConsumerConnected = false;
      clearInterval(this.#intervalId);
      this.#intervalId = null;

      try {
        await this.disconnectConsumer();
        await this.#connectConsumer();
      } catch (error) {
        console.error(
          `Kafka consumer re-connection failed with error ${error.message}. Max retries reached. Exiting...`,
        );
        process.exit(1);
      }
    });
  }

  /**
   * Connects to node-rdkakfa client's producer using an exponential backoff retry mechanism
   * @private
   */
  async #connectProducer() {
    try {
      await backOff(() => {
        return new Promise((resolve, reject) => {
          this.#producer.removeAllListeners('ready');
          this.#producer.removeAllListeners('connection.failure');

          this.#producer.connect();

          this.#producer.once('ready', () => {
            this.#isProducerConnected = true;
            console.log('Kafka producer successfully connected');
            this.#producer.removeAllListeners('connection.failure');
            resolve();
          });

          this.#producer.once('connection.failure', (error) => {
            this.#isProducerConnected = false;
            console.error(
              `Kafka producer connection resulted in failure: ${error}`,
            );
            reject(error);
          });
        });
      }, retryOptions);
    } catch (error) {
      throw new Error(error);
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
          this.#consumer.removeAllListeners('ready');
          this.#consumer.removeAllListeners('connection.failure');

          this.#consumer.connect();

          this.#consumer.once('ready', () => {
            this.#isConsumerConnected = true;
            console.log('Kafka consumer successfully connected');
            this.#consumer.removeAllListeners('connection.failure');
            resolve();
          });

          this.#consumer.once('connection.failure', (error) => {
            this.#isConsumerConnected = false;
            console.error(
              `Kafka consumer connection resulted in failure: ${error}`,
            );
            reject(error);
          });
        });
      }, retryOptions);
    } catch (error) {
      throw new Error(error);
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
      console.error(
        `Kafka producer connection failed with error ${error.message}. Max retries reached. Exiting...`,
      );
      process.exit(1);
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
      console.error(
        `Kafka consumer connection failed with error ${error.message}. Max retries reached. Exiting...`,
      );
      process.exit(1);
    }
  }

  /**
   * Sends an encoded message to a topic. Encodes the message data using this.#registry.encode
   * @param {String} topic The topic to send the message to
   * @param {Object} message The message to send to a topic
   * @public
   */
  async publishToTopic(topic, message) {
    try {
      await this.#initProducer();

      if (this.#isProducerConnected) {
        const subject = `${topic}-value`;
        const id = await this.#registry.getRegistryId(subject, 'latest');

        const encodedMessage = await this.#registry.encode(id, message);

        this.#producer.produce(
          topic,
          null, // Partition, null for automatic partitioning
          Buffer.from(encodedMessage),
          `${topic}-schema`, // Key
        );
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
  async subscribeToTopic(topic, onMessage) {
    try {
      await this.#initConsumer();

      if (this.#isConsumerConnected) {
        this.#consumer.subscribe([topic]);

        if (!this.#intervalId) {
          this.#intervalId = setInterval(() => {
            this.#consumer.consume(10);
          }, 1000);
        }

        this.#consumer.on('data', async (data) => {
          try {
            const decodedValue = await this.#registry.decode(data.value);

            onMessage({ value: decodedValue });
          } catch (error) {
            console.error(
              `Error occurred consuming messages from ${topic}: ${error}`,
            );
          }
        });
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
        await backOff(() => {
          return new Promise((resolve, reject) => {
            this.#producer.once('disconnected', () => {
              this.#isProducerConnected = false;
              this.#producer.removeAllListeners();

              console.log('Successfully disconnected Kafka producer');
            });

            this.#producer.disconnect((error) => {
              if (error) {
                console.error(
                  `Error occurred disconnecting producer: ${error}`,
                );
                reject(error);
              } else {
                resolve();
              }
            });
          });
        }, retryOptions);
      }
    } catch (error) {
      console.error(
        `Kafka producer disconnection failed with error ${error.message}. Max retries reached. Exiting...`,
      );
      process.exit(1);
    }
  }

  /**
   * Disconnects node-rdkafka client's consumer and removes all associated listeners if any.
   * @public
   */
  async disconnectConsumer() {
    try {
      if (this.#isConsumerConnected) {
        await backOff(() => {
          return new Promise((resolve, reject) => {
            this.#consumer.once('disconnected', () => {
              this.#isConsumerConnected = false;
              this.#consumer.removeAllListeners();

              clearInterval(this.#intervalId);
              this.#intervalId = null;

              console.log('Successfully disconnected Kafka consumer');
            });

            this.#consumer.disconnect((error) => {
              if (error) {
                console.error(
                  `Error occurred disconnecting consumer: ${error}`,
                );
                reject(error);
              } else {
                resolve();
              }
            });
          });
        }, retryOptions);
      }
    } catch (error) {
      console.error(
        `Kafka consumer disconnection failed with error ${error.message}. Max retries reached. Exiting...`,
      );
      process.exit(1);
    }
  }
}

module.exports = { KafkaClient };
