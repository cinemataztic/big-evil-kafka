module.exports = async () => {
  if (globalThis.__TEST_CONTAINERS__) {
    const {
      network,
      zookeeperContainer,
      kafkaContainer,
      schemaRegistryContainer,
    } = globalThis.__TEST_CONTAINERS__;

    await zookeeperContainer.stop();

    await kafkaContainer.stop();

    await schemaRegistryContainer.stop();

    await network.stop();
  }
};
