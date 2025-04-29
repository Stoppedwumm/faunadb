const fs = require("fs");
const path = require("path");
const { logger } = require("./logger");
const { CONSTANTS } = require("./constants");

const QA_HOST = process.env.FAUNA_QAHOST || "localhost";
let paused = false;

let isPaused = () => {
  return fs.existsSync(path.join(__dirname, `${QA_HOST}.pause`));
};

let maybePause = async () => {
  // Pause queries if {host_ip}.pause file exists
  while (isPaused()) {
    if (!paused) {
      logger.warn(`Pausing queries to host ${QA_HOST}`);
      paused = true;
    }

    // Wait 30 seconds and then retry
    await new Promise(resolve => setTimeout(resolve, CONSTANTS.pauseDelay));
  }

  if (paused) {
    logger.info(`Resuming queries to host ${QA_HOST}`);
    paused = false;
  }
}

module.exports = {
  maybePause
};
