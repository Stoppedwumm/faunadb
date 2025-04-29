const { getNewClient } = require("./client");
const { CONSTANTS } = require("./constants");
const { logger, logError } = require("./logger");
const { maybePause } = require("./pause");

let streamEvents = async (
  query,
  streamName = "UnnamedStream",
) => {
  return new Promise(async (resolve, reject) => {
    let client = getNewClient();
    let attempt = 0;
    let errorsObserved = false;

    while (attempt < CONSTANTS.maxStreamAttempts) {
      await maybePause(() => {
        // Reset attempts and client after resuming from paused
        attempt = 0;
        client = getNewClient();
      });

      attempt++;

      const stream = client.stream(query);
      let state = { streamName: streamName, lastDocId: -1 };

      try {
        for await (const event of stream) {
          countEvents(event, state);

          if (event.data.message === undefined) {
            throw new Error(
              `[${state.streamName}] NoMessage: 'message' field missing; aborting`,
            );
          }

          state.lastDocId = event.data.id || -1;
        }
      } catch (error) {
        logError(error, `[${state.streamName}] `);
        errorsObserved = true;
      } finally {
        stream.close();
      }
    }

    if (errorsObserved) {
      reject();
    } else {
      resolve();
    }

    return;
  });
};

let countEvents = (event, state) => {
  let eventCount = state.eventCount || {
    add: 0,
    update: 0,
    remove: 0,
    total: 0,
  };
  eventCount[event.type] += 1;
  eventCount.total += 1;

  if (eventCount.total % 1000 == 0) {
    logger.info(
      `[${state.streamName}] Event count: ${JSON.stringify(eventCount)}`,
    );
  }

  state.eventCount = eventCount;
};

module.exports = {
  streamEvents,
};
