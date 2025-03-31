# Big Evil Kafka

Wrapper package around [node-rdkafka](https://www.npmjs.com/package/node-rdkafka) where only the configuration is required, and the package can be used instantly with just the essentials. Don't be scared from the name, Kafka is cool and the name is a nod to the [Undertaker's](https://en.wikipedia.org/wiki/The_Undertaker) biker persona in the early 2000s.

The purpose is to provide a batteries-included package where one does not have to worry about configuring [node-rdkafka](https://www.npmjs.com/package/node-rdkafka) for sending a message to a topic and consuming a message from a topic. The package handles node-rdkafka producer/consumer instantiation wrt producer/consumer configuration internally and only allows connecting and disconnecting producer/consumer externally.

## Getting started

The package is available on [npm](https://www.npmjs.com/package/@cinemataztic/big-evil-kafka), and can be installed with:

```sh
npm i @cinemataztic/big-evil-kafka
```

## Prerequisites

Node.js version should be >=16

This package uses [confluent-schema-registry](https://www.npmjs.com/package/@kafkajs/confluent-schema-registry) and assumes that the schema registry is in place along with the Kafka server running in the background.

## Usage

To use the module, you must require it.

```js
const { KafkaClient } = require('@cinemataztic/big-evil-kafka');
```

## Configuration

Configurations must be passed to the KafkaClient to initialize node-rdkafka producer and consumer internally.

### `config`

- `clientId?: string`

  The unique identifier of both producer and consumer instance. It is meant as a label and is not to be confused with the group ID.

  Default value is `default-client-id`.

- `groupId?: string`

  The unique identifier for a group of consumers that work together to consume messages from a set of Kafka topics.

  Default value is `default-group-id`.

- `brokers?: Array`

  The list of brokers that specifies the Kafka broker(s), the producer and consumer should connect to. Brokers need to be passed as an array, i.e, `['localhost:9092', 'kafka:29092']` because the package internally converts them to string as a requirement for `metadata.broker.list`.  

  Default value is `['localhost:9092']`.

- `avroSchemaRegistry?: string`

  The schema registry URL, which helps in encoding and decoding the messages according to a specific Avro schema in a subject.

  Default value is `http://localhost:8081`.

```js
const client = new KafkaClient({
  clientId: process.env.KAFKA_CLIENT_ID,
  groupId: process.env.KAFKA_GROUP_ID,
  brokers: process.env.KAFKA_BROKERS.split(','), // Assumes value as localhost:9092
  avroSchemaRegistry: process.env.SCHEMA_REGISTRY_URL,
});
```

## Connection

To connect producer and consumer, call the following methods for producer and consumer respectively. The connect methods are wrapped internally within an exponential backoff algorithm, allowing the kafka client's producer and consumer to reconnect in case of failure.
```js
client.connectProducer();

client.connectConsumer();

## Publishing to a Topic

To publish a message to a topic, provide the topic name and the message. 

```js
client.publishToTopic(topic, message);
```

## Subscribing to a Topic

The package uses non-flowing consumer mode with `enable.auto.commit` enabled along with `auto.offset.reset` set to earliest.

The messages are consumed at an interval of 1 second with 10 messages consumed at each interval. 

To subscribe to a topic for consuming messages, provide the topic name and a callback function that would return the message.

```js
client.subscribeToTopic(topic, onMessage);
```

## Disconnection

To disconnect either the producer or consumer, call the following methods for producer and consumer respectively.
```js
client.disconnectProducer();

client.disconnectConsumer();

```

## Motivation

Many of our services are relying upon the Kafka message queue system. The problem with using node-rdkafka in multiple services was that in case of any change to kafka configuration, it had to be replicated across multiple services for consistency. The manual setup and configuration of node-rdkafka is not simple and requires a lot of effort to set it up in a way that ensures maintainability. 

Having a wrapper package around node-rdkafka allows to not only utilize [exponential backoff](https://www.npmjs.com/package/exponential-backoff) for consumer/producer retry mechanism but also provide a batteries-included package that would simply allow users to send and consume messages.
