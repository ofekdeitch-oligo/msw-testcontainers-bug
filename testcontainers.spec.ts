import { Kafka, KafkaConfig, logLevel } from "kafkajs";
import { KafkaContainer, StartedTestContainer } from 'testcontainers';
import { rest } from 'msw';
import { setupServer } from 'msw/node';

jest.setTimeout(60000);

describe('Testcontainers with msw', () => {
  
  it("should work when not using msw", async () => {
    const kafkaContainer = await new KafkaContainer().withExposedPorts(9093).start();

    await testPubSub(kafkaContainer);

    await kafkaContainer.stop();
  });

  it("should work when using msw", async () => {
    const mockServer = setupServer(
      // ...bypassRoutes(),
    );
    mockServer.listen({ onUnhandledRequest: 'bypass' });

    const kafkaContainer = await new KafkaContainer().withExposedPorts(9093).start();

    await testPubSub(kafkaContainer);

    await kafkaContainer.stop();
    mockServer.close();
  });

});

const testPubSub = async (kafkaContainer: StartedTestContainer, additionalConfig: Partial<KafkaConfig> = {}) => {
  const kafka = new Kafka({
    logLevel: logLevel.NOTHING,
    brokers: [`${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(9093)}`],
    ...additionalConfig,
  });

  const producer = kafka.producer();
  await producer.connect();

  const consumer = kafka.consumer({ groupId: "test-group" });
  await consumer.connect();

  await producer.send({
    topic: "test-topic",
    messages: [{ value: "test message" }],
  });

  await consumer.subscribe({ topic: "test-topic", fromBeginning: true });

  const consumedMessage = await new Promise((resolve) => {
    consumer.run({
      eachMessage: async ({ message }) => resolve(message.value?.toString()),
    });
  });

  expect(consumedMessage).toBe("test message");

  await consumer.disconnect();
  await producer.disconnect();
};

function bypassRoutes() {
  return [rest.get, rest.post, rest.put, rest.delete, rest.options].map(
    (handler) =>
      handler('*', (req) => {
        return req.passthrough();
      }),
  );
}