const { Kafka } = require("kafkajs");

const topic_name = process.argv[2] || "Logs2";

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_example_1",
      brokers: ["192.168.1.2:9092"],
    });

    const consumer = kafka.consumer({
      groupId: "example_1_cg_1",
    });
    console.log("Connecting the Consumer...");
    await consumer.connect();
    console.log("Connection is successfull.");

    await consumer.subscribe({
      topic: topic_name,
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
