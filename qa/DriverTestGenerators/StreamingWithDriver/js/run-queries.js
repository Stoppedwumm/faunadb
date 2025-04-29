const {
  fql,
  ContendedTransactionError,
  NetworkError,
  QueryTimeoutError,
  ServiceInternalError
} = require("fauna");
const { logger, logError } = require("./logger");
const { maybePause } = require("./pause");
const { CONSTANTS } = require("./constants");
const { getNewClient } = require("./client");

let handleError = async (err, actionName) => {
  // We can be pretty forgiving with errors; the point is to keep
  // generating events, so swallow these errors and continue
  const ignorable = [
    ContendedTransactionError,
    NetworkError,
    QueryTimeoutError,
    ServiceInternalError
  ];

  if (ignorable.some(x => err instanceof x)) {
    logger.warn(
      `'${actionName}' query failed with ignorable error, '${err.code}'; waiting 1s and continuing`);
    await new Promise(r => setTimeout(r, CONSTANTS.ignorableErrorDelay));
  } else {
    logError(err, `'${actionName}' query error: `);
    throw err;
  }
};

// Entry point that will do some basic CRUD queries that
// can be streamed.
let incrementCrud = async (collName) => {
  logger.info(`Starting queries on '${collName}'`);

  let client = getNewClient();
  let i = 0;
  let lastId = "";

  while (true) {
    await maybePause(() => {
      // Reset client after resuming from paused
      client = getNewClient();
    });

    if (i % 100 == 0) {
      // On every 100th iteration, create a new doc; should retry a few times
      // to make sure it succeeds
      const maxRetries = 10;
      let shouldRetry = true;

      for (let j = 0; j < maxRetries && shouldRetry; j++) {
        await client.query(fql`
          Collection(${collName}).create({ message: ${i} })`)
          .then((response) => {
            lastId = response.data.id;
            shouldRetry = false;
          })
          .catch((err) => {
            handleError(err, "Create");

            // Throw on last attempt if it failed
            if (j == maxRetries - 1) {
              throw err;
            }
          });
      }
    } else {
      // Otherwise, update the last created doc with a new message
      await client.query(fql`
        Collection(${collName}).byId(${lastId})!.update({ message: ${i} })`)
        .catch((err) => {
          handleError(err, "Update");
        });
    }

    i++;

    // 50ms delay to slow things down a little
    await new Promise(r => setTimeout(r, CONSTANTS.mainLoopDelay));
  }
};

module.exports = {
  incrementCrud,
};
