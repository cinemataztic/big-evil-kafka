const { GenericContainer, Network, Wait } = require('testcontainers');
const { KafkaContainer } = require('@testcontainers/kafka');

module.exports = async () => {
  const network = await new Network().start();

  // Start ZooKeeper
  const zooKeeperHost = 'zookeeper';
  const zooKeeperPort = 2181;
  const zookeeperContainer = await new GenericContainer(
    'confluentinc/cp-zookeeper:7.5.2',
  )
    .withNetwork(network)
    .withNetworkAliases(zooKeeperHost)
    .withEnvironment({ ZOOKEEPER_CLIENT_PORT: zooKeeperPort.toString() })
    .withExposedPorts(zooKeeperPort)
    .withWaitStrategy(Wait.forLogMessage('binding to port'))
    .start();
  console.log('✅ ZooKeeper started');

  // Start Kafka
  const kafkaHost = 'kafka';
  const kafkaPort = 9092;
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
    .withWaitStrategy(Wait.forLogMessage('started (kafka.server.KafkaServer)'))
    .start();

  const kafkaBootstrapServers = `PLAINTEXT://${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(kafkaPort)}`;

  console.log('✅ Kafka started');

  await new Promise((resolve) => setTimeout(resolve, 5000));

  // Start Schema Registry
  const schemaRegistryHost = 'schema-registry';
  const schemaRegistryPort = 8081;
  const schemaRegistryContainer = await new GenericContainer(
    'confluentinc/cp-schema-registry:7.5.2',
  )
    .withNetwork(network)
    .withNetworkAliases(schemaRegistryHost)
    .withExposedPorts(schemaRegistryPort)
    .withEnvironment('SCHEMA_REGISTRY_HOST_NAME', schemaRegistryHost)
    .withEnvironment(
      'SCHEMA_REGISTRY_LISTENERS',
      `http://0.0.0.0:${schemaRegistryPort}`,
    )
    .withEnvironment(
      'SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS',
      kafkaBootstrapServers,
    )
    .withWaitStrategy(
      Wait.forHttp('/subjects', schemaRegistryPort).forStatusCode(200),
    )
    .start();

  console.log('✅ Schema Registry started');

  // Store containers in global memory
  globalThis.__TEST_CONTAINERS__ = {
    network,
    zookeeperContainer,
    kafkaContainer,
    schemaRegistryContainer,
  };

  console.log('Kafka and ZooKeeper are up and running!');
};
