const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_pubsub_client",
      brokers: ["192.168.1.2:9092"],
    });

    const consumer = kafka.consumer({
      groupId: "mobile_encoder_consumer_group",
    });
    console.log("Connecting the Consumer...");
    await consumer.connect();
    console.log("Connection is successfull.");

    await consumer.subscribe({
      topic: "raw_video_topic",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Message received from mobile: ${message.value} on ${topic}-${partition}`
        );
      },
    });
  } catch (error) {
    console.error("something went wrong:", error);
  }
}
