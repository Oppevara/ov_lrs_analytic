const MongoClient = require('mongodb').MongoClient;

const url = 'mongodb://localhost:27017';
const dbName = 'lrs';

const client = new MongoClient(url, { useNewUrlParser: true });

module.exports = {
  connect: () => {
    return client.connect();
  },
  db: (client) => {
    return client.db(dbName);
  }
};
