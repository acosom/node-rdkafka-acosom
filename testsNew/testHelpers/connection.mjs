export function connectProducer(producer) {
  return new Promise((resolve, reject) => {
    producer.connect({}, function (err) {
      if (err) reject();
      resolve();
    });
  });
}
