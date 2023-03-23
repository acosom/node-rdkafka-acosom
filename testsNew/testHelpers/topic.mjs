import { lookup } from "dns";
import { waitMs } from "./index.mjs";

function getTopic(client, topicName) {
  return new Promise((resolve, reject) => {
    client.getMetadata(
      {
        topic: topicName,
      },
      (err, metadata) => {
        if (err) return reject(err);
        const topic = metadata.topics.find((topic) => topic.name === topicName);
        if (topic) return resolve(topic);
        return resolve(false);
      }
    );
  });
}

export async function lookupTopic(
  producer,
  topicName,
  maxTries,
  delayMs,
  shouldFind
) {
  for (let i = 0; i < maxTries; i++) {
    await waitMs(delayMs);
    const topic = await getTopic(producer, topicName);
    if (shouldFind && topic) return true;
    if (!shouldFind && !topic) return true;
  }
  if (shouldFind) return false;
  return true;
}

export async function dontFindTopic(producer, topicName, maxTries, delayMs) {
  const notFound = await lookupTopic(
    producer,
    topicName,
    maxTries,
    delayMs,
    false
  );
  if (!notFound) throw new Error("Found topic when it should not");
}
export async function findTopic(producer, topicName, maxTries, delayMs) {
  const found = await lookupTopic(producer, topicName, maxTries, delayMs, true);
  if (!found) throw new Error("Topic not found");
}

export function deleteTopic(adminClient, producer, topicName) {
  return new Promise((resolve, reject) => {
    adminClient.deleteTopic(topicName, async function (err) {
      // Unknown topic = code 3
      if (err && err.code !== 3) reject(err);
      await dontFindTopic(producer, topicName, 10, 100);
      resolve();
    });
  });
}

export function createTopic(adminClient, producer, topicName) {
  return new Promise((resolve, reject) => {
    adminClient.createTopic(
      { topic: topicName, num_partitions: 1, replication_factor: 1 },
      async function (err) {
        // Already exists code 36
        if (err && err.code !== 36) reject(err);
        await findTopic(producer, topicName, 10, 100);
        resolve();
      }
    );
  });
}

export async function recreateTopic(adminClient, producer, topicName) {
  return new Promise((resolve, reject) => {
    process.nextTick(async () => {
      try {
        await deleteTopic(adminClient, producer, topicName);
        await createTopic(adminClient, producer, topicName);
        resolve();
      } catch (ex) {
        reject(
          new Error(`Error during recreation of topic ${topicName}`, {
            cause: ex,
          })
        );
      }
    });
  });
}
