const { KafkaClient } = require('../src');
const { values, config } = require('./common');

const { topic } = values;

let kafkaClient;
let logSpy;

beforeAll(async () => {
  kafkaClient = new KafkaClient(config);
  logSpy = jest.spyOn(console, 'log').mockImplementation();
});

describe('Kafka Client Integration test', () => {
  beforeEach(() => {
    jest.clearAllMocks(); // Ensures clean logs between tests
  });

  test('should log message when producer is connected', async () => {
    await kafkaClient.sendMessage(topic, { message: 'Hello Cinemataztic' });
    expect(logSpy).toHaveBeenCalledWith(
      'Kafka producer successfully connected',
    );
  });
  test('should log message when producer sends a message', async () => {
    await kafkaClient.sendMessage(topic, { message: 'Hello Cinemataztic' });
    expect(logSpy).toHaveBeenCalledWith(
      `Successfully published data to topic: ${topic}`,
    );
  });
  test('should log message when consumer is connected', async () => {
    await kafkaClient.sendMessage(topic, { message: 'Hello Cinemataztic' });
    await kafkaClient.consumeMessage(topic, (data) =>
      console.log('data', data),
    );
    expect(logSpy).toHaveBeenCalledWith(
      'Kafka consumer successfully connected',
    );
    expect(logSpy).toHaveBeenCalledWith(`Subscribed to topic ${topic}`);
  });
  test('should log message when consumer receives a message', async () => {});
});

afterAll(async () => {
  await kafkaClient.disconnectProducer();
  await kafkaClient.disconnectConsumer();
  logSpy.mockRestore();
});
