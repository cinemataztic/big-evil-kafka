const { Producer, KafkaConsumer } = require("node-rdkafka");
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry");
const { EventEmitter } = require("events");

const retryConnection = require("./utils/retryConnection");

/**
 * Kafka client which is a wrapper library around node-rdkafka
 *
 */
class KafkaClient extends EventEmitter {
  /** The producer connection promise to ensure only one connection attempt is made at a time
   * @type {Promise}
   * @private
   */
  #producerConnectionPromise = null;
  /** The consumer connection promise to ensure only one connection attempt is made at a time
   * @type {Promise}
   * @private
   */
  #consumerConnectionPromise = null;
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
   * Stores callbacks mapped by topic.
   * @type {Map<string, Function>}
   * @private
   */
  #topicCallbacks = new Map();

  /**
   * A cache to store schema IDs for topics to avoid redundant registry lookups. The key is the subject name (e.g., 'topic-value') and the value is the corresponding schema ID.
   * @type {Map<string, number>}
   * @private
   */
  #schemaIdCache = new Map();

  /**
   * Flag to ensure the data listener is only attached once.
   * @type {Boolean}
   * @private
   */
  #isDataListenerAttached = false;

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
    this.#clientId = config.clientId || "default-client";
    this.#groupId = config.groupId || "default-group-id";
    this.#brokers = config.brokers || ["localhost:9092"];
    this.#avroSchemaRegistry =
      config.avroSchemaRegistry || "http://localhost:8081";
    this.#producer = new Producer({
      "client.id": this.#clientId,
      "metadata.broker.list": this.#brokers.join(","),
      dr_cb: false,
    });
    this.#consumer = new KafkaConsumer(
      {
        "group.id": this.#groupId,
        "client.id": this.#clientId,
        "metadata.broker.list": this.#brokers.join(","),
        "enable.auto.commit": false,
        "auto.commit.interval.ms": 1000,
        "topic.metadata.refresh.interval.ms": 5000,
      },
      {
        "auto.offset.reset": "earliest",
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
            // node-rdkafka's producer.connect does not emit an error event when connection fails, instead it calls the callback with the error. Hence we handle success and failure scenarios within the callback itself and resolve or reject the promise accordingly.
            this.#producer.connect({}, (err, metadata) => {
              if (err) {
                return reject(err);
              }

              this.#isProducerConnected = true;
              this.#producer.setPollInterval(100);
              console.log("Producer connected");
              this.#registerProducerEventHandler();
              resolve();
            });
          });
        },
        "producer-connection",
        this.#producerMaxRetries,
      );
    } catch (error) {
      throw new Error(error.message);
    }
  }

  /**
   * Connects to node-rdkafka client's consumer using an exponential backoff retry mechanism
   * @private
   */
  async #connectConsumer() {
    try {
      await retryConnection(
        () => {
          return new Promise((resolve, reject) => {
            // node-rdkafka's consumer.connect does not emit an error event when connection fails, instead it calls the callback with the error. Hence we handle success and failure scenarios within the callback itself and resolve or reject the promise accordingly.
            this.#consumer.connect({}, (err, metadata) => {
              if (err) {
                return reject(err);
              }

              this.#isConsumerConnected = true;
              console.log("Consumer connected");
              this.#registerConsumerEventHandler();
              resolve();
            });
          });
        },
        "consumer-connection",
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
    if (this.#isProducerConnected) return;

    // If a connection attempt is already in progress, wait for it to complete instead of starting a new one
    if (this.#producerConnectionPromise) {
      await this.#producerConnectionPromise;
      return;
    }

    try {
      console.log("Initializing Producer..");
      this.#producerConnectionPromise = this.#connectProducer();
      await this.#producerConnectionPromise;
    } catch (error) {
      console.error(`Error initializing producer: ${error.message}`);
      throw new Error(`Error initializing producer: ${error.message}`);
    } finally {
      this.#producerConnectionPromise = null; // Clear lock when done
    }
  }

  /**
   * Wrapper function around #connectConsumer where it first checks whether the consumer has been connected previously
   * @private
   */
  async #initConsumer() {
    if (this.#isConsumerConnected) return;

    // If a connection attempt is already in progress, wait for it to complete instead of starting a new one
    if (this.#consumerConnectionPromise) {
      await this.#consumerConnectionPromise;
      return;
    }

    try {
      if (!this.#isConsumerConnected) {
        console.log("Initializing Consumer..");
        this.#consumerConnectionPromise = this.#connectConsumer();
        await this.#consumerConnectionPromise;
      }
    } catch (error) {
      console.error(`Error initializing consumer: ${error.message}`);
      throw new Error(`Error initializing consumer: ${error.message}`);
    } finally {
      this.#consumerConnectionPromise = null; // Clear lock when done
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
        let id = this.#schemaIdCache.get(subject);

        // If the schema ID for the subject is not cached, fetch it from the registry and cache it for future use
        if (!id) {
          id = await this.#registry.getRegistryId(subject, "latest");
          this.#schemaIdCache.set(subject, id);
        }

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
        // 1. Maintain a local list of all topics we want to subscribe to, including the new one. This ensures we don't lose existing subscriptions when we call subscribe again, since node-rdkafka's subscribe replaces the entire subscription list instead of adding to it.
        const allTopics = Array.from(this.#topicCallbacks.keys());
        if (!allTopics.includes(topic)) {
          allTopics.push(topic);
        }

        // 2. Call subscribe with the full list of topics we want to be subscribed to. This way we ensure that all our desired topics are registered with Kafka, and we don't accidentally drop any existing subscriptions.
        this.#consumer.subscribe(allTopics);

        // 3. Store the callback for this topic in our local map so we can reference it when messages arrive. This allows us to have a single data listener that can route messages to the correct callback based on the topic, and also ensures that if we receive a message for a topic that we haven't fully registered yet (e.g., due to async timing), we can safely ignore it without crashing.
        this.#topicCallbacks.set(topic, onMessage);

        console.log(`Subscribed to topics: ${allTopics.join(", ")}`);

        // 4. Start the consumer loop if it's not already running. We only want one loop running regardless of how many topics we subscribe to, so we check if the interval is already set before starting it. This ensures that we don't end up with multiple loops consuming messages concurrently, which could lead to duplicate processing
        if (!this.#intervalId) {
          this.#intervalId = setInterval(() => {
            this.#consumer.consume(10);
          }, 1000);
        }

        // 5. Attach a single data listener to the consumer if we haven't already. This listener will be responsible for routing incoming messages to the correct callback based on the topic. By checking the #isDataListenerAttached flag, we ensure that we only attach this listener once, even if subscribeToTopic is called multiple times. This prevents us from having multiple listeners attached to the 'data' event, which could cause messages to be processed multiple times or lead to memory leaks.
        if (!this.#isDataListenerAttached) {
          this.#consumer.on("data", async (data) => {
            // When a message arrives, we look up the callback for its topic in our local map. This allows us to handle messages for multiple topics with a single listener, and also provides a safeguard in case we receive a message for a topic that we haven't fully registered yet (e.g., due to async timing). If we don't find a callback for the message's topic, we log a warning and ignore the message, allowing it to be processed by whoever owns it without crashing our consumer loop.
            const targetCallback = this.#topicCallbacks.get(data.topic);

            // Strategy A: If we receive a message for a topic that doesn't have a registered callback (e.g., due to async timing issues where the consumer receives a message before we've had a chance to store its callback in the map), we log a warning and skip processing that message. This allows the message to be processed by whoever owns it without crashing our consumer loop, and also provides visibility into potential timing issues in our subscription logic.
            if (!targetCallback) {
              console.warn(`No callback registered for topic: ${data.topic}`);
              return;
            }

            try {
              // Decode the message value using the schema registry. If decoding fails (e.g., due to a poison pill message that doesn't conform to the expected schema), we catch the error and log it, but we don't let it crash our consumer loop. This ensures that even if we encounter bad messages, our consumer can continue processing subsequent messages without getting stuck in a reboot loop.
              const decodedValue = await this.#registry.decode(data.value);
              console.log(
                `Message received by consumer on topic: ${data.topic}`,
              );

              // Call the registered callback for this topic with the decoded message. We wrap this in a try-catch block to handle any potential errors that might occur within the callback itself, ensuring that even if the callback crashes, it doesn't bring down our consumer loop. By logging the error stack trace, we can gain visibility into issues within our message processing logic without sacrificing the stability of our consumer.
              await Promise.resolve(
                targetCallback({ value: decodedValue, topic: data.topic }),
              );
            } catch (error) {
              // Strategy B: If we encounter an error while decoding a message (e.g., due to a poison pill message that doesn't conform to the expected schema), we catch the error and log it, but we don't let it crash our consumer loop. This ensures that even if we encounter bad messages, our consumer can continue processing subsequent messages without getting stuck in a reboot loop. By logging the error stack trace, we can gain visibility into issues with specific messages without sacrificing the stability of our consumer.
              console.error(
                `Consume from topic '${data.topic}' failed: ${error}`,
              );
            } finally {
              // Regardless of whether processing succeeded or failed, we commit the message offset to ensure that we don't get stuck on a bad message. By committing the offset in the finally block, we guarantee that we won't repeatedly attempt to process the same poison pill message and get stuck in a reboot loop. This allows our consumer to continue making progress even in the face of bad messages, while still providing visibility into any issues through our error logging.
              this.#consumer.commitMessage(data);
            }
          });

          this.#isDataListenerAttached = true;
        }
      }
    } catch (error) {
      console.error(`subscribeToTopic ('${topic}') failed: ${error}`);
      if (this.#topicCallbacks.size === 0 && this.#intervalId) {
        clearInterval(this.#intervalId);
        this.#intervalId = null;
      }

      throw new Error(`subscribeToTopic ('${topic}') failed: ${error}`);
    }
  }

  /**
   * Disconnects node-rdkafka client's producer and removes all associated listeners if any.
   * @public
   */
  async disconnectProducer() {
    if (!this.#isProducerConnected) return;

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        console.warn(
          "Producer disconnect timed out after 5000ms. Forcing shutdown.",
        );
        cleanup();
      }, 5000);

      const cleanup = () => {
        clearTimeout(timeoutId);
        this.#isProducerConnected = false;
        this.#producer.setPollInterval(0);
        this.#producer.removeAllListeners();
        console.log("Disconnected Producer");
        resolve();
      };

      this.#producer.once("disconnected", cleanup);
      this.#producer.disconnect();
    });
  }

  /**
   * Disconnects node-rdkafka client's consumer and removes all associated listeners if any.
   * @public
   */
  async disconnectConsumer() {
    if (!this.#isConsumerConnected) return;

    return new Promise((resolve) => {
      // Create a 5-second timeout fail-safe
      const timeoutId = setTimeout(() => {
        console.warn(
          "Consumer disconnect timed out after 5000ms. Forcing shutdown.",
        );
        cleanup();
      }, 5000);

      const cleanup = () => {
        clearTimeout(timeoutId);
        this.#isConsumerConnected = false;
        this.#isDataListenerAttached = false; // Reset data listener flag
        this.#topicCallbacks.clear(); // Clear registered topic callbacks
        this.#consumer.removeAllListeners();

        if (this.#intervalId) {
          clearInterval(this.#intervalId);
          this.#intervalId = null;
        }

        console.log("Disconnected Consumer");
        resolve();
      };

      this.#consumer.once("disconnected", cleanup);

      try {
        this.#consumer.disconnect();
      } catch (error) {
        // In case of an error during disconnect, log it and proceed with cleanup to avoid hanging
        console.error(`Consumer disconnect threw an error: ${error.message}`);
        cleanup();
      }
    });
  }

  #registerProducerEventHandler() {
    let lastErrorEmit = 0;
    this.#producer.on("event.error", (error) => {
      const now = Date.now();
      const errorMessage = `Producer runtime error: ${error}`;
      console.error(errorMessage);

      if (now - lastErrorEmit >= 60000) {
        lastErrorEmit = now;
        this.emit("producer.event.error", new Error(errorMessage), {
          source: "producer",
        });
      }
    });
  }

  #registerConsumerEventHandler() {
    let lastErrorEmit = 0;
    this.#consumer.on("event.error", (error) => {
      const now = Date.now();
      const errorMessage = `Consumer runtime error: ${error}`;
      console.error(errorMessage);

      if (now - lastErrorEmit >= 60000) {
        lastErrorEmit = now;
        this.emit("consumer.event.error", new Error(errorMessage), {
          source: "consumer",
        });
      }
    });
  }
}

module.exports = { KafkaClient };
