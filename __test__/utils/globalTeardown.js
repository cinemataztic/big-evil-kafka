module.exports = async () => {
  if (globalThis.__TEST_CONTAINERS__) {
    const {
      network,
      zookeeperContainer,
      kafkaContainer,
      schemaRegistryContainer,
    } = globalThis.__TEST_CONTAINERS__;

    console.log('Stopping ZooKeeper...');
    await zookeeperContainer.stop();

    console.log('Stopping Kafka...');
    await kafkaContainer.stop();

    console.log('Stopping Schema Registry...');
    await schemaRegistryContainer.stop();

    console.log('Stopping Network...');
    await network.stop();

    console.log('All test containers have been stopped.');
  }
};
