const { Client } = require("fauna");

const clients = [];

function getNewClient(
  secret = process.env.FAUNA_SECRET,
  endpoint = process.env.FAUNA_ENDPOINT,
) {
  const client = new Client({
    secret: secret,
    endpoint: endpoint,
  });

  clients.push(client);

  return client;
}

function closeClients() {
  clients.forEach((c) => c.close());
}

module.exports = {
  getNewClient,
  closeClients,
};
