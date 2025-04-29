const {
  fql,
  ConstraintFailureError,
  ContendedTransactionError,
  NetworkError,
  QueryTimeoutError,
  ServiceInternalError,
  FaunaError,
  ServiceError
} = require("fauna");
const { logger, logError } = require("./logger");
const { getNewClient } = require("./client");

let createCollection = async (collName) => {
  const client = getNewClient();
  const maxRetries = 5;
  let shouldRetry = true;

  for (let i = 0; i < maxRetries && shouldRetry; i++) {
    await client.query(fql`
      if (!Collection.byName(${collName}).exists()) {
        Collection.create({
          name: ${collName},
          computed_fields: {
            messageMod10: { body: "x => {
              if (x.message != null)
                x.message % 10
              else
                null
              }"
            }
          },
          indexes: {
            idx_messageMod10: {
              terms: [],
              values: [
                { field: "messageMod10", order: "asc" }
              ]
            }
          }
        })
      }`)
      .then((x) => {
        logger.info(`Collection '${collName}' is ready`);
        shouldRetry = false;
      })
      .catch((err) => {
        const retryable = [ ContendedTransactionError, NetworkError, QueryTimeoutError, ServiceInternalError ];

        if (err instanceof ServiceError) {
          logger.warn(`ServiceError details:
HTTP status: ${err.httpStatus}
Error code: ${err.code}
Summary: ${err.queryInfo?.summary ?? "none"}
Constraint failures: ${err.constraint_failures?.map(cf => JSON.stringify(cf)).join(",") ?? "none"},
Stack: ${err.stack}`);
        }

        if (retryable.some(x => err instanceof x)) {
          logger.warn("Unable to create Collection due to a retryable error; retrying if there are attempts left.");
        } else {
          logError(err, "'Collection.create' query error: ");
          throw err;
        }
      });
  }
};

module.exports = {
  createCollection,
};
