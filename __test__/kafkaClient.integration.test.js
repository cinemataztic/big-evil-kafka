const { KafkaClient } = require('../src');
const { values, config, constats } = require('./common');

const { schemaRegistryContainer } = globalThis.__TEST_CONTAINERS__;

const topic = constats.TOPIC;

config['avroSchemaRegistry'] =
  `http://localhost:${schemaRegistryContainer.getMappedPort(values.schemaRegistry.port)}`;

let kafkaClient;
let logSpy;

beforeAll(async () => {
  kafkaClient = new KafkaClient(config);
  logSpy = jest.spyOn(console, 'log').mockImplementation();
});

describe('Kafka Client Integration test', () => {
  beforeEach(() => {});
  test('should log message when producer is connected', async () => {
    await kafkaClient.sendMessage(topic, { message: 'Hello Cinemataztic' });
    expect(logSpy).toHaveBeenCalledWith(
      'Kafka producer successfully connected',
    );
  });
  // test('should log message when producer sends a message', async () => {
  //   await kafkaClient.sendMessage(topic, { message: 'Hello Cinemataztic' });
  //   expect(logSpy).toHaveBeenCalledWith(
  //     `Successfully published data to topic: ${topic}`,
  //   );
  // });
  // test('should log message when consumer is connected', async () => {
  //   await kafkaClient.sendMessage(topic, { message: 'Hello Cinemataztic' });
  //   await kafkaClient.consumeMessage(topic, (data) =>
  //     console.log('data', data),
  //   );
  //   expect(logSpy).toHaveBeenCalledWith(
  //     'Kafka consumer successfully connected',
  //   );
  //   expect(logSpy).toHaveBeenCalledWith(`Subscribed to topic ${topic}`);
  // });
  test('should log message when consumer receives a message', async () => {});
});

// afterAll(() => {
//   // kafkaClient.disconnectProducer();
//   // kafkaClient.disconnectConsumer();
//   logSpy.mockRestore();
// });
