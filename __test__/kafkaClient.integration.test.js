const avro = require('avsc');

const { KafkaClient } = require('../src');

const { kafkaContainer, schemaRegistryContainer } =
  globalThis.__TEST_CONTAINERS__;

const topic = 'cinemataztic';
const brokers = [
  `${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(9092)}`,
];
const SCHEMA_REGISTRY_URL = `http://localhost:${schemaRegistryContainer.getMappedPort(8081)}`;

const config = {
  clientId: 'ctz-client',
  groupId: 'ctz-group',
  brokers: brokers,
  avroSchemaRegistry: SCHEMA_REGISTRY_URL,
};

const avroSchema = {
  type: 'record',
  name: 'HelloCinemataztic',
  fields: [{ name: 'greeting', type: 'string' }],
};

let kafkaClient;
let logSpy;

beforeAll(async () => {
  kafkaClient = new KafkaClient(config);
});

describe('Kafka Client Integration test', () => {
  beforeEach(async () => {
    logSpy = jest.spyOn(console, 'log').mockImplementation();
  });

  test('should log message when producer is connected', async () => {
    await kafkaClient.sendMessage(topic, 'Hello Cinemataztic');

    expect(logSpy).toHaveBeenCalledWith(
      'Kafka producer successfully connected',
    );
  });

  // test('should log message when producer sends a message', async () => {});

  // test('should log message when consumer is connected', async () => {});

  // test('should log message when consumer receives a message', async () => {});

  afterEach(() => {
    logSpy.mockRestore();
  });
});

afterAll(() => {
  // kafkaClient.disconnectProducer();
  // kafkaClient.disconnectConsumer();
});
