import { createRequire } from "module";
import t from "tap";

import {
  connectProducer,
  deleteTopic,
  recreateTopic,
} from "../testHelpers/index.mjs";

const require = createRequire(import.meta.url);
const Kafka = require("../..");

await t.test("delete Records", async (t) => {
  t.setTimeout(1000 * 60 * 7);
  const brokerList = "localhost:9092,localhost:8092";
  const topicName = "test-delete-records";

  const amountTotal = 50;

  const adminClient = Kafka.AdminClient.create({
    "client.id": "kafka-test",
    "metadata.broker.list": brokerList,
  });
  const producer = new Kafka.Producer({
    "client.id": "e2eTestProducer",
    "metadata.broker.list": brokerList,
    dr_cb: true,
    debug: "all",
  });

  producer.setPollInterval(10);

  try {
    await connectProducer(producer);
    await recreateTopic(adminClient, producer, topicName);
  } catch (err) {
    t.error(err);
  }

  const produceAll = () => {
    let verified_received = 0;
    return new Promise((res) => {
      producer.on("delivery-report", function (err, report) {
        verified_received++;
        if (verified_received === amountTotal) res();
      });

      for (let it = 0; it <= amountTotal; it++) {
        producer.produce(topicName, null, Buffer.from("value"), "key");
      }
      producer.flush(5000, () => {
        return res();
      });
    });
  };

  const deleteRecords = () => {
    return new Promise((res, rej) => {
      // Delete Offset 0-10
      adminClient.deleteRecords(topicName, 0, 10, function miau(err, offsetsDeleted) {
        if (err) rej(err);
        if (offsetsDeleted) return res();
        rej("no offsets deleted");
      });
    });
  };

  try {
    await produceAll();
    // await getMetaDataAdmin();
    await deleteRecords();
    // await deleteTopic(adminClient, producer, topicName);
    adminClient.disconnect();
    producer.disconnect();
    t.end();
  } catch (ex) {
    t.error(ex);
  }
});
