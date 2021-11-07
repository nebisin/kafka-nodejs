const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_example_2",
      brokers: ["192.168.1.2:9092"],
    });

    const consumer = kafka.consumer({
      groupId: "log_store_consumer_group",
    });
    console.log("Connecting the Consumer...");
    await consumer.connect();
    console.log("Connection is successfull.");

    await consumer.subscribe({
      topic: "LogStoreTopic",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Message received: ${message.value} on ${topic}-${partition}`
        );
      },
    });
  } catch (error) {
    console.error("something went wrong:", error);
  }
}
