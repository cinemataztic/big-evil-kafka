const { GenericContainer, Network, Wait } = require('testcontainers');
const { KafkaContainer } = require('@testcontainers/kafka');

const { values } = require('../common');
const { registerAvroSchema } = require('./registerAvroSchema');

module.exports = async () => {
  const network = await new Network().start();

  // Start ZooKeeper
  const zooKeeperHost = values.zookeeper.host;
  const zooKeeperPort = values.zookeeper.port;

  const zookeeperContainer = await new GenericContainer(
    'confluentinc/cp-zookeeper:7.5.2',
  )
    .withNetwork(network)
    .withNetworkAliases(zooKeeperHost)
    .withEnvironment({ ZOOKEEPER_CLIENT_PORT: zooKeeperPort.toString() })
    .withExposedPorts(zooKeeperPort)
    .start();

  // Start Kafka
  const kafkaHost = values.kafka.host;
  const kafkaPort = values.kafka.port;

  const kafkaContainer = await new KafkaContainer('confluentinc/cp-kafka:7.5.2')
    .withNetwork(network)
    .withNetworkAliases(kafkaHost)
    .withZooKeeper(zooKeeperHost, zooKeeperPort)
    .withEnvironment({
      KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://${kafkaHost}:${kafkaPort}`,
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true',
    })
    .withExposedPorts(kafkaPort)
    .withWaitStrategy(
      Wait.forSuccessfulCommand(
        `/bin/sh -c "nc -zv ${kafkaHost} ${kafkaPort}"`, // Test network connection to kafka via kafkaHost and kafkaPort
      ),
    )
    .start();

  const kafkaBootstrapServers = `PLAINTEXT://${kafkaHost}:${kafkaPort}`;

  // Start Schema Registry
  const schemaRegistryHost = values.schemaRegistry.host;
  const schemaRegistryPort = values.schemaRegistry.port;

  const schemaRegistryContainer = await new GenericContainer(
    'confluentinc/cp-schema-registry:7.5.2',
  )
    .withNetwork(network)
    .withNetworkAliases(schemaRegistryHost)
    .withEnvironment({
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafkaBootstrapServers,
      SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL: 'PLAINTEXT',
      SCHEMA_REGISTRY_LISTENERS: `http://0.0.0.0:${schemaRegistryPort}`,
      SCHEMA_REGISTRY_HOST_NAME: schemaRegistryHost,
    })
    .withExposedPorts(schemaRegistryPort)
    .withWaitStrategy(
      Wait.forHttp('/subjects', schemaRegistryPort).forStatusCode(200),
    )
    .start();

  await registerAvroSchema(schemaRegistryContainer, schemaRegistryPort);

  // Store references to containers in globalThis under __TEST_CONTAINERS__ umbrella
  globalThis.__TEST_CONTAINERS__ = {
    network,
    zookeeperContainer,
    kafkaContainer,
    schemaRegistryContainer,
  };
};
