const { KafkaClient } = require('../src');

const topic = 'cinemataztic';

let kafkaClient;
let logSpy;

jest.spyOn(global, 'setInterval').mockImplementation(jest.fn());
jest.spyOn(global, 'clearInterval').mockImplementation(jest.fn());

beforeAll(async () => {
  kafkaClient = new KafkaClient({
    clientId: 'ctz-client',
    groupId: 'ctz-group',
    brokers: process.env.KAFKA_BROKERS
      ? process.env.KAFKA_BROKERS.split(',')
      : ['localhost:9092'],
  });
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
    await kafkaClient.consumeMessage(topic, () => {});
    expect(logSpy).toHaveBeenCalledWith(
      'Kafka consumer successfully connected',
    );
  });
  test.only('should log message when consumer receives a message', async () => {
    await kafkaClient.consumeMessage(topic, (data) => {});

    // Wait for the consumer to be fully connected/subscribed
    await new Promise((resolve) => setTimeout(resolve, 3000));

    await kafkaClient.sendMessage(topic, { message: 'Hello Cinemataztic' });

    // Wait a bit to ensure message processing happens
    await new Promise((resolve) => setTimeout(resolve, 3000));

    // Optionally print out the calls for debugging:
    console.log('Log calls:', logSpy.mock.calls);

    expect(logSpy).toHaveBeenCalledWith(
      `Message received by consumer on topic: ${topic}`,
    );
  });
});

afterAll(async () => {
  logSpy.mockRestore();
  await kafkaClient.disconnectProducer();
  await kafkaClient.disconnectConsumer();
});
