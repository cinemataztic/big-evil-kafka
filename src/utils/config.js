const config = {
  clientId: process.env.KAFKA_CLIENT_ID || 'default-client',
  groupId: process.env.KAFKA_GROUP_ID || 'default-group-id',
  brokers: process.env.KAFKA_BROKERS
    ? process.env.KAFKA_BROKERS.split(',')
    : ['localhost:9092'],
  avroSchemaRegistry:
    process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081',
};

export default config;
