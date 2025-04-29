const winston = require("winston");

const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [
    new winston.transports.Console({
      level: "debug",
      stderrLevels: ['error'],
      handleExceptions: true,
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.colorize(),
        winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`),
      ),
    }),
  ],
});

let logError = (error, logPrefix = "") => {
  if (error instanceof Error) {
    logger.error(`${logPrefix}${error.message}`);
  } else {
    logger.error(`${logPrefix}${JSON.stringify(error)}`);
  }
};

module.exports = {
  logger,
  logError,
};
