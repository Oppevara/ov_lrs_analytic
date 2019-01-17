const connect = require('./lib/db').connect;
const clientDb = require('./lib/db').db;
const materialInteractions = require('./lib/queries').materialInteractions;
const materialInteractionsByVerb = require('./lib/queries').materialInteractionsByVerb;
const materialScores = require('./lib/queries').materialScores;
const contentTypeVerbs = require('./lib/queries').contentTypeVerbs;
const writeDocumentsToCsv = require('./lib/csv').writeDocumentsToCsv;
const chalk = require('chalk');
const os = require('os');

function buildSingleCase(db, singleCase) {
  const promise = singleCase.function(db)
  .then(documents => {
    writeDocumentsToCsv(documents, singleCase.file, singleCase.csvCallback, singleCase.csvHeader);
  });

  return promise;
}

let cases = {};

cases.materialInteractions = {
  title: 'Material interactions',
  file: 'material-interactions.csv',
  function: materialInteractions,
  csvCallback: (document, stringifier) => {
    stringifier.write([document._id, document.name, document.count, document.verbs.join(',')]);
  },
  csvHeader: ['URL', 'name', 'count', 'verbs']
};
cases.materialInteractionsByVerb = {
  title: 'Material interactions by verb',
  file: 'material-interactions-by-verb.csv',
  function: materialInteractionsByVerb,
  csvCallback: (document, stringifier) => {
    stringifier.write([document._id.url, document.name, document._id.verb, document.count]);
  },
  csvHeader: ['URL', 'name', 'verb', 'count']
};
cases.materialScores = {
  title: 'Material scores',
  file: 'material-scores.csv',
  function: materialScores,
  csvCallback: (document, stringifier) => {
    stringifier.write([document._id.url, document._id.verb, document.name, document.count, document.successCount, document.min, document.max, document.rawMin, document.rawMax, document.rawAvg]);
  },
  csvHeader: ['URL', 'verb', 'name', 'statement count', 'success count', 'minimum possible score', 'maximum possible score', 'student minimum score (raw)', 'student maximum score (raw)', 'student average score (raw)']
};
cases.contentTypeVerbs = {
  title: 'Verbs by content type',
  file: 'content-type-verbs.csv',
  function: contentTypeVerbs,
  csvCallback: (document, stringifier) => {
    stringifier.write([document._id, document.verbs.join(',')]);
  },
  csvHeader: ['content type', 'verbs']
};
cases.all = {
  title: 'All',
  function: (client, db) => {
    const promises = [];

    Object.keys(cases).forEach(single => {
      if (single !== 'all') {
        const promise = buildSingleCase(db, cases[single]);

        promises.push(promise);
      }
    });

    Promise.all(promises)
    .then(() => {
      client.close();
    });
  }
};

if (process.argv.length < 3) {
  const textLine = ' Please specify which case you would like to run! ';
  console.log(chalk.yellowBright('-'.repeat(textLine.length)));
  console.log(chalk.yellowBright(textLine));
  console.log(chalk.yellowBright('-'.repeat(textLine.length)));
  console.log(os.EOL, 'Possible cases:', os.EOL);
  Object.keys(cases).forEach(key => {
    console.log(chalk.green(key), ' : ', cases[key].title);
  });
  process.exit(0);
} else if (!(process.argv[2] in cases)) {
  console.log('Unknown case!');
  process.exit(0);
}

const queryCase = process.argv[2];

connect()
.then((client) => {
  const db = clientDb(client);

  if (queryCase === 'all') {
    cases[queryCase].function(client, db);
  } else {
    buildSingleCase(db, cases[queryCase])
    .then(() => {
      client.close();
    });
  }
})
.catch(err => {
  console.error(err);
});
