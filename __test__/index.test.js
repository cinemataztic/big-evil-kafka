const { KafkaClient } = require("../src");

const topic = "cinemataztic";

let kafkaClient;
let logSpy;

jest.setTimeout(30000);

beforeAll(async () => {
  kafkaClient = new KafkaClient({
    clientId: "ctz-client",
    groupId: "ctz-group",
    brokers: process.env.KAFKA_BROKERS
      ? process.env.KAFKA_BROKERS.split(",")
      : ["localhost:9092"],
  });
  logSpy = jest.spyOn(console, "log").mockImplementation();
});

describe("Kafka client integration tests", () => {
  beforeEach(async () => {
    jest.clearAllMocks();
  });

  test("should log message when producer is connected", async () => {
    await kafkaClient.publishToTopic(topic, { message: "Hello Producer" });
    expect(logSpy).toHaveBeenCalledWith("Producer connected");
  });

  test("should log message when consumer is connected", async () => {
    await kafkaClient.subscribeToTopic(topic, () => {});
    expect(logSpy).toHaveBeenCalledWith("Consumer connected");
  });

  test("should log message when consumer receives a message", async () => {
    const uniqueMessage = `Hello Cinemataztic - ${Date.now()}`;

    const messageReceivedPromise = new Promise((resolve, reject) => {
      kafkaClient
        .subscribeToTopic(topic, (data) => {
          try {
            if (data?.value?.message === uniqueMessage) {
              expect(data).toHaveProperty("value");
              expect(data.value).toHaveProperty("message", uniqueMessage);
              resolve();
            }
          } catch (error) {
            reject(error);
          }
        })
        .catch(reject);
    });

    await new Promise((resolve) => setTimeout(resolve, 2000));

    await kafkaClient.publishToTopic(topic, { message: uniqueMessage });

    await messageReceivedPromise;
  });

  test("should route messages from multiple topics to their respective callbacks", async () => {
    const topicA = "cinemataztic";
    const topicB = "cinemataztic-a";

    await kafkaClient.publishToTopic(topicB, { message: "second-topic" });
    
    await new Promise((resolve) => setTimeout(resolve, 3000));

    const uniqueMessageA = `Message A - ${Date.now()}`;
    const uniqueMessageB = `Message B - ${Date.now()}`;

    let resolveA, rejectA;
    const messageReceivedPromiseA = new Promise((res, rej) => {
      resolveA = res;
      rejectA = rej;
    });

    let resolveB, rejectB;
    const messageReceivedPromiseB = new Promise((res, rej) => {
      resolveB = res;
      rejectB = rej;
    });

    await kafkaClient.subscribeToTopic(topicA, (data) => {
      try {
        if (data?.value?.message === uniqueMessageA) {
          expect(data).toHaveProperty("topic", topicA);
          expect(data.value).toHaveProperty("message", uniqueMessageA);
          resolveA(); 
        }
      } catch (error) {
        rejectA(error);
      }
    });

    await kafkaClient.subscribeToTopic(topicB, (data) => {
      try {
        if (data?.value?.message === uniqueMessageB) {
          expect(data).toHaveProperty("topic", topicB);
          expect(data.value).toHaveProperty("message", uniqueMessageB);
          resolveB(); 
        }
      } catch (error) {
        rejectB(error);
      }
    });

    await new Promise((resolve) => setTimeout(resolve, 5000));

    await kafkaClient.publishToTopic(topicA, { message: uniqueMessageA });
    await kafkaClient.publishToTopic(topicB, { message: uniqueMessageB });

    await Promise.all([messageReceivedPromiseA, messageReceivedPromiseB]);
  });
});

afterAll(async () => {
  logSpy.mockRestore();
  await kafkaClient.disconnectProducer();
  await kafkaClient.disconnectConsumer();
});
