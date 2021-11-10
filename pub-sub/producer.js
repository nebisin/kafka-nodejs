const { Kafka } = require("kafkajs");

createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_pubsub_client",
      brokers: ["192.168.1.2:9092"],
    });

    const producer = kafka.producer();
    console.log("Connecting the Producer...");
    await producer.connect();
    console.log("Connection is successfull.");

    const message_result = await producer.send({
      topic: "raw_video_topic",
      messages: [
        {
          value: "New video content",
          partition: 0,
        },
      ],
    });

    console.log("Sending is successfull:", JSON.stringify(message_result));

    await producer.disconnect();
  } catch (error) {
    console.error("something went wrong:", error);
  } finally {
    process.exit(0);
  }
}
