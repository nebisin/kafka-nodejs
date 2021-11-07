const { Kafka } = require("kafkajs");

createTopic();

async function createTopic() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_example_2",
      brokers: ["192.168.1.2:9092"],
    });

    const admin = kafka.admin();
    console.log("Connecting the Kafka Broker...");
    await admin.connect();
    console.log("Connection is successfull.\nCreating the topics...");
    await admin.createTopics({
      topics: [
        {
          topic: "LogStoreTopic",
          numPartitions: 2,
        },
      ],
    });
    console.log("Topics created successfully.");
    await admin.disconnect();
  } catch (error) {
    console.error("something went wrong:", error);
  } finally {
    process.exit(0);
  }
}
