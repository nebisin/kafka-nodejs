const { Kafka } = require("kafkajs");
const logData = require("./system_logs.json");

createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_example_2",
      brokers: ["192.168.1.2:9092"],
    });

    const producer = kafka.producer();
    console.log("Connecting the Producer...");
    await producer.connect();
    console.log("Connection is successfull.");
    const messages = logData.map((item) => {
      return {
        value: JSON.stringify(item),
        partition: item.type === "system" ? 0 : 1,
      };
    });

    const message_result = await producer.send({
      topic: "LogStoreTopic",
      messages: messages,
    });

    console.log("Sending is successfull:", JSON.stringify(message_result));

    await producer.disconnect();
  } catch (error) {
    console.error("something went wrong:", error);
  } finally {
    process.exit(0);
  }
}
