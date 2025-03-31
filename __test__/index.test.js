const { KafkaClient } = require('../src');

const topic = 'cinemataztic';

let kafkaClient;
let logSpy;

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

describe('Kafka client integration tests', () => {
  beforeEach(async () => {
    jest.clearAllMocks();
  });

  test('should log message when producer is connected', async () => {
    await kafkaClient.publishToTopic(topic, { message: 'Hello Cinemataztic' });
    expect(logSpy).toHaveBeenCalledWith(
      'Kafka producer successfully connected',
    );
  });

  test('should log message when consumer is connected', async () => {
    await kafkaClient.subscribeToTopic(topic, () => {});
    expect(logSpy).toHaveBeenCalledWith(
      'Kafka consumer successfully connected',
    );
  });

  test('should log message when consumer receives a message', async () => {
    await kafkaClient.subscribeToTopic(topic, (data) => {
      expect(data).toHaveProperty('value');
      expect(data.value).toHaveProperty('message', 'Hello Cinemataztic');
    });

    // Wait for consumer to connect and subscribe.
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Send a message after consumer is ready.
    await kafkaClient.publishToTopic(topic, { message: 'Hello Cinemataztic' });

    // Wait for the polling (via setInterval) to pick up the message.
    await new Promise((resolve) => setTimeout(resolve, 5000));
  });

  afterEach(() => {
    jest.useRealTimers();
  });
});

afterAll(async () => {
  logSpy.mockRestore();
  await kafkaClient.disconnectProducer();
  await kafkaClient.disconnectConsumer();
});
