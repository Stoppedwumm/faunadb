const { fql } = require("fauna");
const { logger } = require("./logger");
const { getNewClient, closeClients } = require("./client");
const { getDoc } = require("./doc");
const { maybePause } = require("./pause");
const { CONSTANTS } = require("./constants");

let anyErrors = false;

(async () => {
  const devKey = process.argv[2];
  const prodKey = process.argv[3];

  let clientMap = {
    dev: getNewClient(secret = devKey),
    prod: getNewClient(secret = prodKey),
  };

  let consecutiveFailures = 0;

  while (consecutiveFailures < CONSTANTS.maxFailures) {
    await maybePause(() => {
      // Reset clients after resuming from paused
      clientMap.dev = getNewClient(secret = devKey);
      clientMap.prod = getNewClient(secret = prodKey);
    });

    try {
      for (const clientKey in clientMap) {
        const client = clientMap[clientKey];

        for (let i = 0; i < CONSTANTS.indexRetries.count; i++) {
          const idxQueryable = await client.query(fql`test_schema.definition.indexes["IndexOneField"]`);

          // If the index doesn't exist, "data" will be null, so we can safely continue if null or
          // if "queryable" is "true"
          if (!idxQueryable.data || idxQueryable.data.queryable === true) {
            break;
          // Otherwise, check if we're not on the last iteration and if the index exists but
          // isn't queryable yet
          } else if (i < CONSTANTS.indexRetries.count - 1 && idxQueryable.data && idxQueryable.data.queryable !== true) {
            logger.warn(`Index not ready; waiting ${CONSTANTS.indexRetries.wait} ms.`);
            await new Promise(resolve => setTimeout(resolve, CONSTANTS.indexRetries.wait));
            continue;
          // Fall-through here means the index is still not queryable;
          // the next query will likely fail, but it can catch and retry if the
          // while loop hasn't run out of retries
          } else {
            logger.error("Index did not return, may fail queries.");
          }
        }

        const firstDoc = await client.query(fql`
          Object.keys(test_schema.all().first()!).where(x => x.matches("^v[0-9]").nonEmpty()).first()!.slice(1,2).parseInt()
        `);
        const version = firstDoc.data;
        const docs = Array.from({ length: CONSTANTS.createDocCount }, () => getDoc(version));

        // Query to read the index and then create docs
        await client.query(fql`
          test_schema.IndexOneField()
          ${docs}.forEach(x => test_schema.create(x))`);
      }

      consecutiveFailures = 0;
    } catch (error) {
      logger.warn("Query error:", error);
      consecutiveFailures++;
    }

    // Adding a 1s delay before continuing the loop; schema pushes on a small cluster like QA can get blocked by
    // constant traffic, so I'm trying to relieve the pressure
    await new Promise(resolve => setTimeout(resolve, CONSTANTS.mainLoopDelay));
  }
})()
  .catch((error) => {
    if (error !== undefined) {
      logger.error(
        `Unhandled error: ${error.name}, ${error.message}, ${error.summary}`
      );
    }

    anyErrors = true;
  })
  .finally(() => {
    closeClients();
    process.exit(anyErrors ? 1 : 0);
  });
