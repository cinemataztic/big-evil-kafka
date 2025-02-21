module.exports = async () => {
  if (globalThis.__TEST_CONTAINERS__) {
    const {
      network,
      zookeeperContainer,
      kafkaContainer,
      schemaRegistryContainer,
    } = globalThis.__TEST_CONTAINERS__;

    await zookeeperContainer.stop();

    console.log('Zookeeper container has stopped');

    await kafkaContainer.stop();

    console.log('Kafka container has stopped');

    await schemaRegistryContainer.stop();

    console.log('Schema Registry container has stopped');

    await network.stop();
  }
};
