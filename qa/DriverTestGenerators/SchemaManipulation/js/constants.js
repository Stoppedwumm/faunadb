const CONSTANTS = {
  maxFailures: 10,
  createDocCount: 3,
  mainLoopDelay: 1000,
  pauseDelay: 30000,
  // Index retries can wait up to 1 minute in 5s increments
  indexRetries: {
    count: 12,
    wait: 5000
  }
}

module.exports = {
  CONSTANTS
};
