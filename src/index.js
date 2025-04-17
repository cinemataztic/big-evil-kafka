const { Producer, KafkaConsumer } = require('node-rdkafka');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');

const retryConnection = require('./utils/retryConnection');

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
  #isProducerConnected = false;
  /**
   * The consumer connection flag.
   * @type {Boolean}
   * @private
   */
  #isConsumerConnected = false;
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
    this.#producer.on('disconnected', async () => {
      if (this.#isProducerConnected && !this.#isProducerReconnecting) {
        console.error('Producer disconnected. Reconnecting…');
        this.#isProducerConnected = false;
        this.#isProducerReconnecting = true;
        await this.#retryProducerConnection();
      }
    });

    this.#producer.on('event.error', async (error) => {
      if (!this.#isProducerReconnecting) {
        console.error(`Producer runtime error: ${error}`);
        this.#isProducerConnected = false;
        this.#isProducerReconnecting = true;
        await this.#retryProducerConnection();
      }
    });
  }

  #registerConsumerEventListeners() {
    this.#consumer.on('disconnected', async () => {
      if (this.#isConsumerConnected && !this.#isConsumerReconnecting) {
        console.error('Consumer disconnected. Reconnecting…');
        this.#isConsumerConnected = false;
        this.#isConsumerReconnecting = true;
        clearInterval(this.#intervalId);
        await this.#retryConsumerConnection();
      }
    });

    this.#consumer.on('event.error', async (error) => {
      if (!this.#isConsumerReconnecting) {
        console.error(`Consumer runtime error: ${error}`);
        this.#isConsumerConnected = false;
        this.#isConsumerReconnecting = true;
        clearInterval(this.#intervalId);
        await this.#retryConsumerConnection();
      }
    });
  }

  /**
   * Connects to node-rdkakfa client's producer using an exponential backoff retry mechanism
   * @param {number} [numOfAttempts=1] - The maximum number of retry attempts before the connection is considered failed.
   * @private
   */
  async #connectProducer(numOfAttempts = this.#producerMinConnectAttempts) {
    try {
      await retryConnection(
        () => {
          return new Promise((resolve, reject) => {
            this.#producer.connect();

            // Ensures that multiple listeners are not registered in case of failure
            this.#producer.removeAllListeners('ready');
            this.#producer.removeAllListeners('connection.failure');

            // Temporary event listener for connection phase, needed since retry requires a reject before attempting to connect again.
            this.#producer.once('event.error', (error) => {
              this.#isProducerReconnecting = true;
              console.error(
                `Producer connection error: ${error?.message || error}`,
              );
              reject(error);
            });

            this.#producer.once('ready', () => {
              this.#isProducerConnected = true;
              this.#isProducerReconnecting = false;
              this.#producer.setPollInterval(100);
              console.log('Producer connected');
              resolve();
            });

            this.#producer.once('connection.failure', (error) => {
              this.#isProducerConnected = false;
              this.#isProducerReconnecting = true;
              console.error(`Producer connection failure: ${error}`);
              reject(error);
            });
          });
        },
        'producer-connection',
        numOfAttempts,
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
            this.#consumer.connect();

            // Ensures that multiple listeners are not registered in case of failure
            this.#consumer.removeAllListeners('ready');
            this.#consumer.removeAllListeners('connection.failure');

            // Temporary event listener for connection phase, needed since retry requires a reject before attempting to connect again.
            this.#consumer.once('event.error', (error) => {
              this.#isConsumerReconnecting = true;
              console.error(
                `Consumer connection event error: ${error?.message || error}`,
              );
              reject(error);
            });

            this.#consumer.once('ready', () => {
              this.#isConsumerConnected = true;
              this.#isConsumerReconnecting = false;
              console.log('Consumer connected');
              resolve();
            });

            this.#consumer.once('connection.failure', (error) => {
              this.#isConsumerConnected = false;
              this.#isConsumerReconnecting = true;
              console.error(`Consumer connection failure: ${error}`);
              reject(error);
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
    if (!this.#isProducerConnected) {
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
    if (!this.#isConsumerConnected) {
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
        await retryConnection(
          () => {
            return new Promise((resolve, reject) => {
              this.#producer.once('disconnected', () => {
                this.#isProducerConnected = false;
                // Set setPollInterval to 0 to turn it off
                this.#producer.setPollInterval(0);
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
          },
          'producer-disconnection',
          this.#producerMaxConnectAttempts,
        );
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
        await retryConnection(
          () => {
            return new Promise((resolve, reject) => {
              this.#consumer.once('disconnected', () => {
                this.#isConsumerConnected = false;
                clearInterval(this.#intervalId);
                this.#intervalId = null;
                this.#consumer.removeAllListeners();
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
          },
          'consumer-disconnection',
          this.#consumerMaxConnectAttempts,
        );
      }
    } catch (error) {
      console.error(
        `Kafka consumer disconnection failed with error ${error.message}. Max retries reached. Exiting...`,
      );
      process.exit(1);
    }
  }

  /**
   * Helper function to reconnect producer in case of 'disconnected' or 'event.error' event. In case retries are exhausted then application will exit
   * @private
   */
  async #retryProducerConnection() {
    try {
      await this.#connectProducer(this.#producerMaxConnectAttempts);
    } catch (error) {
      console.error(
        `Kafka producer re-connection failed with error ${error.message}. Max retries reached. Exiting...`,
      );
      process.exit(1);
    }
  }

  /**
   * Helper function to reconnect consumer in case of 'disconnected' or 'event.error' event. In case retries are exhausted then application will exit
   * @private
   */
  async #retryConsumerConnection() {
    try {
      await this.#connectConsumer();
    } catch (error) {
      console.error(
        `Kafka consumer re-connection failed with error ${error.message}. Max retries reached. Exiting...`,
      );
      process.exit(1);
    }
  }
}

module.exports = { KafkaClient };
