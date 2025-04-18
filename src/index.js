const { Producer, KafkaConsumer } = require('node-rdkafka');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');

const retryConnection = require('./utils/retry');

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
   * The consumer connection flag in case of 'disconnection' or 'event.error' events to avoid duplicate reconnection attempts.
   * @type {Boolean}
   * @private
   */
  #isConsumerReconnecting = false;
  /**
   * The producer reconnection flag in case of 'disconnection' or 'event.error' events to avoid duplicate reconnection attempts.
   * @type {Boolean}
   * @private
   */
  #isProducerReconnecting = false;
  /**
   * The initial retry connection attempts for producer. Initial retry attempts are set to 1.
   * @type {number}
   * @private
   */
  #producerMinConnectAttempts = 1;
  /**
   * The retry connection attempts for producer in case of an 'event.error' or unexpected 'disconnected' events. Retry attempts are set to 5.
   * @type {number}
   * @private
   */
  #producerMaxConnectAttempts = 5;
  /**
   * The retry connection attempts for consumer. Retry attempts are set to 5.
   * @type {number}
   * @private
   */
  #consumerMaxConnectAttempts = 5;
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
      event_cb: true,
    });

    this.#consumer = new KafkaConsumer(
      {
        'group.id': this.#groupId,
        'client.id': this.#clientId,
        'metadata.broker.list': this.#brokers.join(','),
        'enable.auto.commit': true,
        'auto.commit.interval.ms': 1000,
        event_cb: true,
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
    this.#producer.on('event.error', async (error) => {
      if (this.#producer.isConnected() && !this.#isProducerReconnecting) {
        console.error(`Producer runtime error: ${error}`);
        this.#isProducerReconnecting = true;
        this.#producer.setPollInterval(0);
        await this.#retryProducerConnection();
      }
    });
  }

  #registerConsumerEventListeners() {
    this.#consumer.on('event.error', async (error) => {
      if (this.#consumer.isConnected() && !this.#isConsumerReconnecting) {
        console.error(`Consumer runtime error: ${error}`);
        this.#isConsumerReconnecting = true;
        clearInterval(this.#intervalId);
        await this.#retryConsumerConnection();
      }
    });
  }

  /**
   * Connects to node-rdkakfa client's producer using an exponential backoff retry mechanism
   * @private
   */
  async #connectProducer() {
    try {
      await retryConnection(
        () => {
          return new Promise((resolve, reject) => {
            this.#producer.removeAllListeners('ready');
            this.#producer.removeAllListeners('connection.failure');

            this.#producer.connect();

            this.#producer.once('ready', () => {
              this.#isProducerReconnecting = false;
              this.#producer.setPollInterval(100);
              console.log('Producer connected');
              resolve();
            });

            this.#producer.once('connection.failure', () => {
              this.#isProducerReconnecting = true;
              reject();
            });
          });
        },
        'producer-connection',
        this.#producerMinConnectAttempts,
      );
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
      await retryConnection(
        () => {
          return new Promise((resolve, reject) => {
            this.#consumer.removeAllListeners('ready');
            this.#consumer.removeAllListeners('connection.failure');

            this.#consumer.connect();

            this.#consumer.once('ready', () => {
              this.#isConsumerReconnecting = false;
              console.log('Consumer connected');
              resolve();
            });

            this.#consumer.once('connection.failure', () => {
              this.#isConsumerReconnecting = true;
              reject();
            });
          });
        },
        'consumer-connection',
        this.#consumerMaxConnectAttempts,
      );
    } catch (error) {
      throw new Error(error);
    }
  }

  /**
   * Wrapper function around #connectProducer where it first checks whether the producer has been connected previously
   * @private
   */
  async #initProducer() {
    if (!this.#producer.isConnected()) {
      console.log('Initializing producer connection…');
      try {
        await this.#connectProducer();
      } catch (error) {
        console.error('Producer failed to initialize:', error);
      }
    }
  }

  /**
   * Wrapper function around #connectConsumer where it first checks whether the consumer has been connected previously
   * @private
   */
  async #initConsumer() {
    if (!this.#consumer.isConnected()) {
      console.log('Initializing consumer connection…');
      try {
        await this.#connectConsumer();
      } catch (error) {
        console.error('Consumer failed to initialize:', error);
        process.exit(1);
      }
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

      if (this.#producer.isConnected()) {
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

      if (this.#consumer.isConnected()) {
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
      if (this.#producer.isConnected()) {
        return new Promise((resolve, reject) => {
          this.#producer.once('disconnected', () => {
            // Set setPollInterval to 0 to turn it off
            this.#producer.setPollInterval(0);
            this.#producer.removeAllListeners();
            console.log('Producer disconnected');
            resolve();
          });

          this.#producer.disconnect((error) => {
            if (error) {
              console.error(`Error disconnecting producer: ${error}`);
              reject(error);
            }
          });
        });
      }
    } catch (error) {
      console.error(`Producer disconnection failed: ${error}`);
    }
  }

  /**
   * Disconnects node-rdkafka client's consumer and removes all associated listeners if any.
   * @public
   */
  async disconnectConsumer() {
    try {
      if (this.#consumer.isConnected()) {
        return new Promise((resolve, reject) => {
          this.#consumer.once('disconnected', () => {
            clearInterval(this.#intervalId);
            this.#intervalId = null;
            this.#consumer.removeAllListeners();
            console.log('Consumer disconnected');
            resolve();
          });

          this.#consumer.disconnect((error) => {
            if (error) {
              console.error(`Error disconnecting consumer: ${error}`);
              reject(error);
            }
          });
        });
      }
    } catch (error) {
      console.error(`Consumer disconnection failed: ${error}`);
    }
  }

  /**
   * Helper function to reconnect producer in case of 'event.error' event. In case retries are exhausted then application will exit
   * @private
   */
  async #retryProducerConnection() {
    try {
      await retryConnection(
        () => {
          return new Promise((resolve, reject) => {
            this.#producer.removeAllListeners('ready');

            this.#producer.disconnect((error) => {
              if (error) {
                console.error(`Error disconnecting producer: ${error}`);
                reject(error);
              }
            });

            this.#producer.connect({}, (err) => {
              if (err) {
                this.#isProducerReconnecting = true;
                reject(err);
              }
            });

            this.#producer.once('ready', () => {
              this.#isProducerReconnecting = false;
              this.#producer.setPollInterval(100);
              console.log('Producer reconnected');
              resolve();
            });
          });
        },
        'producer-reconnection',
        this.#producerMaxConnectAttempts,
      );
    } catch (error) {
      console.error(
        `Producer reconnection failed error: ${error.message}. Max retries reached. Exiting...`,
      );
      process.exit(1);
    }
  }

  /**
   * Helper function to reconnect consumer in case of 'event.error' event. In case retries are exhausted then application will exit
   * @private
   */
  async #retryConsumerConnection() {
    try {
      await retryConnection(
        () => {
          return new Promise((resolve, reject) => {
            this.#consumer.removeAllListeners('ready');

            this.#consumer.disconnect((error) => {
              if (error) {
                console.error(`Error disconnecting consumer: ${error}`);
                reject(error);
              }
            });

            this.#consumer.connect({}, (err) => {
              if (err) {
                this.#isConsumerReconnecting = true;
                reject(err);
              }
            });

            this.#consumer.once('ready', () => {
              this.#isConsumerReconnecting = false;
              console.log('Consumer reconnected');
              resolve();
            });
          });
        },
        'consumer-reconnection',
        this.#consumerMaxConnectAttempts,
      );
    } catch (error) {
      console.error(
        `Consumer reconnection failed error: ${error.message}. Max retries reached. Exiting...`,
      );
      process.exit(1);
    }
  }
}

module.exports = { KafkaClient };
