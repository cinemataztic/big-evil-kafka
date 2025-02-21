const { default: axios } = require('axios');
const { avroSchema } = require('../common');

const registerAvroSchema = async (schemaRegistryContainer, schemaRegistryPort) => {
  const schemaRegistryUrl = `http://localhost:${schemaRegistryContainer.getMappedPort(schemaRegistryPort)}/subjects/cinemataztic-value/versions`;
  try {
    const response = await axios.post(schemaRegistryUrl, avroSchema, {
      headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json' },
    });
    console.log(`Successfully registered Avro Schema: ${response.data}`);
  } catch (error) {
    console.error(`Error registering Avro Schema: ${error}`);
  }
};

module.exports = { registerAvroSchema };
