export function logger(client) {
  if (!process.env.DEBUG) {
    return;
  }

  client
    .on("event.error", function (err) {
      console.log(err);
    })
    .on("event.log", function (event) {
      var info = {
        severity: event.severity,
        fac: event.fac,
      };
      if (event.severity >= 7) {
        console.log(info, event.message);
      } else if (event.severity === 6 || event.severity === 5) {
        console.log(info, event.message);
      } else if (event.severity === 4) {
        console.log(info, event.message);
      } else if (event.severity > 0) {
        console.log(info, event.message);
      } else {
        console.log(info, event.message);
      }
    })
    .on("event.stats", function (event) {
      console.log(event, event.message);
    })
    .on("event.throttle", function (event) {
      console.log(event, "%s#%d throttled.", event.brokerName, event.brokerId);
    })
    .on("event.event", function (event) {
      console.log(event, event.message);
    })
    .on("ready", function (info) {
      console.log("%s connected to kafka server", info.name);
    });
}
