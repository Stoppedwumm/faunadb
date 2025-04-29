const { fql } = require("fauna");
const { logger } = require("./logger");
const { closeClients } = require("./client");
const { streamEvents } = require("./run-streams");
const { incrementCrud } = require("./run-queries");
const { createCollection } = require("./setup");

const coll = process.env.FAUNA_COLL_NAME || "StreamCollection";
let anyErrors = false;

(async () => {
  await createCollection(coll);

  logger.info(`Starting stream on '${coll}.all().toStream()'`);
  const p1 = streamEvents(
    fql`Collection(${coll}).all().toStream()`,
    (streamName = "BasicCollectionStream"),
  );

  logger.info(`Starting stream on '${coll}.all().changesOn(.messageMod10)'`);
  const p2 = streamEvents(
    fql`Collection(${coll}).all().changesOn(.message)`,
    (streamName = "ChangesOnFieldStream"),
  );

  logger.info(
    `Starting stream on '${coll}.idx_messageMod10().where(x => x.messageMod10 > 4).toStream()'`,
  );
  const p3 = streamEvents(
    fql`Collection(${coll}).idx_messageMod10().where(x => x.messageMod10 > 4).toStream()`,
    (streamName = "IndexFilteredFieldStream"),
  );

  const p4 = incrementCrud(coll);

  // If queries fail (p4) or all streams fail (p1-3), the whole test should fast-fail
  await Promise.all([
    p4,
    Promise.allSettled([p1, p2, p3]).then(results => {
      return results.some(result => result.status === "rejected") ?
        Promise.reject() :
        Promise.resolve();
    })
  ]);
})()
  .catch((error) => {
    if (error !== undefined) {
      logger.error(
        `Unhandled error: ${error.stack}`,
      );
    }

    anyErrors = true;
  })
  .finally(() => {
    closeClients();
    process.exit(anyErrors ? 1 : 0);
  });
