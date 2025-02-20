import { sendMessage, disconnectProducer } from './producer';
import { consumeMessage, disconnectConsumer } from './consumer';

export const producer = {
  send: sendMessage,
  disconnect: disconnectProducer,
};

export const consumer = {
  consume: consumeMessage,
  disconnect: disconnectConsumer,
};
