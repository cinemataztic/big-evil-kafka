# Big Evil Kafka

Wrapper package around [node-rdkafka](https://www.npmjs.com/package/node-rdkafka) where only the configuration is required, and the package can be used instantly with just the essentials. Dont be scared from the name, Kafka is cool and the name is a nod to the [Undertaker's](https://en.wikipedia.org/wiki/The_Undertaker) biker persona in the early 2000's.

The purpose of this package is to provide a batteries included package where one does not have to worry about configuring the [node-rdkafka](https://www.npmjs.com/package/node-rdkafka) package for using kafka client's functions like sending a message to a topic and consuming a message to a topic. The package handles producer/consumer connection internally and only allows disconnecting both producer and consumer connection.

## Getting started

The package is available on [npm](https://www.npmjs.com/package/@cinemataztic/big-evil-kafka), and can be installed with:

```sh
npm i @cinemataztic/big-evil-kafka
```

## Prerequisites

This package uses [confluent-schema-registry](https://www.npmjs.com/package/@kafkajs/confluent-schema-registry) and assumes that schema registry is in place along with kafka server running in the background.

## Usage

To use the module, you must require it.

```js
const { KafkaClient } = require('@cinemataztic/big-evil-kafka');
```

## Configuration

Configurations must be passed to the KafkaClient in order to initialize node-rdkafka producer and consumer internally.

brokers needs to be passed as a string i.e `localhost:9092, kafka:29092`

```js
const client = new KafkaClient({
  clientId: process.env.KAFKA_CLIENT_ID,
  groupId: process.env.KAFKA_GROUP_ID,
  brokers: process.env.KAFKA_BROKERS.split(','), // Assumes value as localhost:9092
  avroSchemaRegistry: process.env.SCHEMA_REGISTRY_URL,
});
```

## Sending Messages

To send a message a topic, provide the topic name and the message. 

```js
client.sendMessage(topic, message);
```

## Consuming Messages

The package uses non-flowing consumer mode with `enable.auto.commit` enabled along with 'auto.offset.reset' set to earliest.

To consume a message from a topic, provide the topic name and a callback function that would return the message.

```js
client.consumeMessage(topic, onMessage);
```

## Disconnecting

To disconnect either the producer or consumer, call the following methods for both producer and consumer respectively.
```js
client.disconnectProducer();

client.disconnectConsumer();

```

## Motivation

Many of our services are relying upon the kafka message queue system. The problem with using node-rdkafka in each of the different services is that in case of any change to kafka configuration, it had to be replicated across different services for consistency and also the manual setup and configuration of node-rdkafka is not simple and requires a lot of effort to set it up in a way that ensures maintainability. 

Having a wrapper package around node-rdkafka enables us to not only utilize [exponential backoff](https://www.npmjs.com/package/exponential-backoff) for consumer/producer retry mechanism but also to provide a batteries included package that would simply allow users to send and consume messages and with additional ability to disconnect them in case of an error in the services. The user only needs to provide the configuration to use the package and can use the package without worrying about to set it up with respect to node-rdkafka connection and its methods that can be somewhat hard to implement if not well-versed in how node-rdkafka works.
