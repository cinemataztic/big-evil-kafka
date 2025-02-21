const constats = {
  TOPIC: 'cinemataztic',
};

const values = {
  topic: 'cinemataztic',
  zookeeper: { host: 'zookeeper', port: 2181 },
  kafka: { host: 'kafka', port: 9092 },
  schemaRegistry: { host: 'schema-registry', port: 8081 },
};

const config = {
  clientId: 'ctz-client',
  groupId: 'ctz-group',
  brokers: ['kafka:9092'],
};

const avroSchema = {
  schema: JSON.stringify({
    type: 'record',
    name: 'HelloWorld',
    fields: [{ name: 'message', type: 'string' }],
  }),
};

module.exports = { constats, values, config, avroSchema };
