const { Producer, Consumer } = require("@platformatic/kafka");
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry");

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
    this.#clientId = config.clientId || "default-client";
    this.#groupId = config.groupId || "default-group-id";
    this.#brokers = config.brokers || ["localhost:9092"];
    this.#avroSchemaRegistry =
      config.avroSchemaRegistry || "http://localhost:8081";
    this.#producer = new Producer({
      clientId: this.#clientId,
      bootstrapBrokers: this.#brokers,
    });
    this.#consumer = new Consumer({
      groupId: this.#groupId,
      clientId: this.#clientId,
      bootstrapBrokers: this.#brokers,
    });
    this.#registry = new SchemaRegistry({ host: this.#avroSchemaRegistry });
  }

  /**
   * Sends an encoded message to a topic. Encodes the message data using this.#registry.encode
   * @param {String} topic The topic to send the message to
   * @param {Object} message The message to send to a topic
   * @public
   */
  async publishToTopic(topic, message) {
    try {
      const subject = `${topic}-value`;
      const id = await this.#registry.getRegistryId(subject, "latest");

      console.log(`Using schema ${topic}-value@latest (id: ${id})`);

      const encodedMessage = await this.#registry.encode(id, message);

      await this.#producer.send({
        messages: [
          {
            topic,
            key: `${topic}-schema`,
            value: Buffer.from(encodedMessage),
            headers: {
              "content-type": "application/vnd.confluent.avro",
            },
          },
        ],
      });

      console.log(`Successfully published data to topic: ${topic}`);
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
      const stream = await this.#consumer.consume({
        autocommit: true,
        topics: [topic],
      });

      console.log(`Subscribed to topic ${topic}`);

      stream.on("data", async (message) => {
        try {
          const decodedValue = await this.#registry.decode(message.value);

          console.log(`Message received by consumer on topic: ${topic}`);

          onMessage({ value: decodedValue });
        } catch (error) {
          console.error(`Consume from topic '${topic}' failed: ${error}`);
        }
      });
    } catch (error) {
      console.error(`subscribeToTopic ('${topic}') failed: ${error}`);
      throw new Error(`subscribeToTopic ('${topic}') failed: ${error}`);
    }
  }

  /**
   * Disconnects node-rdkafka client's producer and removes all associated listeners if any.
   * @public
   */
  async disconnectProducer() {
    try {
      await this.#producer.close();
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
      await this.#consumer.close();
    } catch (error) {
      console.error(`Consumer disconnect failed: ${error}`);
      throw new Error(`Consumer disconnect failed: ${error}`);
    }
  }
}

module.exports = { KafkaClient };
