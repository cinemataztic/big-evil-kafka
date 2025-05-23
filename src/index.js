const { Producer, KafkaConsumer } = require('node-rdkafka');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');
const { EventEmitter } = require('events');

const retryConnection = require('./utils/retryConnection');

/**
 * Kafka client which is a wrapper library around node-rdkafka
 *
 */
class KafkaClient extends EventEmitter{
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
   * The producer connection retry number.
   * @type {number}
   * @private
   */
  #producerMaxRetries = 1;
  /**
   * The consumer connection retry number.
   * @type {number}
   * @private
   */
  #consumerMaxRetries = 5;
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
    super();
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
      await retryConnection(
        () => {
          return new Promise((resolve, reject) => {
            const onReady = () => {
              cleanup();
              this.#isProducerConnected = true;
              this.#producer.setPollInterval(100);
              console.log('Producer connected');
              this.#registerProducerEventHandler();
              resolve();
            };

            const onConnectError = (error) => {
              cleanup();
              reject(error);
            };

            const cleanup = () => {
              this.#producer.removeListener('ready', onReady);
            };

            this.#producer.once('ready', onReady);
            this.#producer.connect({}, onConnectError);
          });
        },
        'producer-connection',
        this.#producerMaxRetries,
      );
    } catch (error) {
      throw new Error(error.message);
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
            const onReady = () => {
              cleanup();
              this.#isConsumerConnected = true;
              console.log('Consumer connected');
              this.#registerConsumerEventHandler();
              resolve();
            };

            const onConnectError = (error) => {
              cleanup();
              reject(error);
            };

            const cleanup = () => {
              this.#consumer.removeListener('ready', onReady);
            };

            this.#consumer.once('ready', onReady);
            this.#consumer.connect({}, onConnectError);
          });
        },
        'consumer-connection',
        this.#consumerMaxRetries,
      );
    } catch (error) {
      throw new Error(error.message);
    }
  }

  /**
   * Wrapper function around #connectProducer where it first checks whether the producer has been connected previously
   * @private
   */
  async #initProducer() {
    try {
      if (!this.#isProducerConnected) {
        console.log('Initializing Producer..');
        await this.#connectProducer();
      }
    } catch (error) {
      console.error(`Error initializing producer: ${error.message}`);
      throw new Error(`Error initializing producer: ${error.message}`);
    }
  }

  /**
   * Wrapper function around #connectConsumer where it first checks whether the consumer has been connected previously
   * @private
   */
  async #initConsumer() {
    try {
      if (!this.#isConsumerConnected) {
        console.log('Initializing Consumer..');
        await this.#connectConsumer();
      }
    } catch (error) {
      console.error(`Error initializing consumer: ${error.message}`);
      setTimeout(() => {
        console.error(
          'Application will be terminated in 10 seconds because the consumer failed to initialize.',
        );
        process.exit(1);
      }, 10000);
      throw new Error(`Error initializing consumer: ${error.message}`);
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
    } catch (error) {
      throw new Error(error.message);
    }

    try {
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
      }
    } catch (error) {
      console.error(`publishToTopic ('${topic}') failed: ${error}`);
      throw new Error(`publishToTopic ('${topic}') failed: ${error}`);
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
    } catch (error) {
      throw new Error(error.message);
    }

    try {
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
            console.error(`Consume from topic '${topic}' failed: ${error}`);
          }
        });
      }
    } catch (error) {
      console.error(`subscribeToTopic ('${topic}') failed: ${error}`);
      clearInterval(this.#intervalId);
      this.#intervalId = null;
      throw new Error(`subscribeToTopic ('${topic}') failed: ${error}`);
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
          this.#producer.once('disconnected', () => {
            this.#isProducerConnected = false;
            this.#producer.setPollInterval(0);
            this.#producer.removeAllListeners();
            console.log('Disconnected Producer');
            resolve();
          });

          this.#producer.disconnect();
        });
      }
    } catch (error) {
      console.error(`Producer disconnect failed: ${error}`);
      throw new Error(`Producer disconnect failed: ${error}`);
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
          this.#consumer.once('disconnected', () => {
            this.#isConsumerConnected = false;
            this.#consumer.removeAllListeners();
            clearInterval(this.#intervalId);
            this.#intervalId = null;
            console.log('Disconnected Consumer');
            resolve();
          });

          this.#consumer.disconnect();
        });
      }
    } catch (error) {
      clearInterval(this.#intervalId);
      this.#intervalId = null;
      console.error(`Consumer disconnect failed: ${error}`);
      throw new Error(`Consumer disconnect failed: ${error}`);
    }
  }

  #registerProducerEventHandler() {
    let lastErrorEmit = 0;
    this.#producer.on('event.error', (error) => {
      const now = Date.now();
      const errorMessage = `Producer runtime error: ${error}`;
      console.error(errorMessage);

      if (now - lastErrorEmit >= 60000) {
        lastErrorEmit = now;
        this.emit('producer.event.error', new Error(errorMessage), { source: 'producer' });
      }
    });
  }

  #registerConsumerEventHandler() {
    let lastErrorEmit = 0;
    this.#consumer.on('event.error', (error) => {
      const now = Date.now();
      const errorMessage = `Consumer runtime error: ${error}`;
      console.error(errorMessage);

      if (now - lastErrorEmit >= 60000) {
        lastErrorEmit = now;
        this.emit('consumer.event.error', new Error(errorMessage), { source: 'consumer' });
      }
    });
  }
}

module.exports = { KafkaClient };
