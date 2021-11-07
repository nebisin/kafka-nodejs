const { Kafka } = require("kafkajs");

const topic_name = process.argv[2] || "Logs2";
const partition = process.argv[3] || 0;

createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_example_1",
      brokers: ["192.168.1.2:9092"],
    });

    const producer = kafka.producer();
    console.log("Connecting the Producer...");
    await producer.connect();
    console.log("Connection is successfull.");

    const message_result = await producer.send({
      topic: topic_name,
      messages: [
        {
          value: "Hello World",
          partition: partition,
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
