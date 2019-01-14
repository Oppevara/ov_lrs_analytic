const stringify = require('csv-stringify');
const fs = require('fs');
const path = require('path');
const chalk = require('chalk');

module.exports = {
  writeDocumentsToCsv: (documents, fileName, writeCallback, header) => {
    console.log(chalk.green('Estimated rows:'), documents.length);
    console.log(chalk.green('Writing to file:'), fileName);
    const stream = fs.createWriteStream(path.join(__dirname, '../results', fileName));
    const stringifier = stringify({
      delimiter: ','
    });
    stringifier.on('error', err => {
      console.error(err.message);
    });

    stringifier.pipe(stream);

    if (header) {
      stringifier.write(header);
    }

    documents.forEach(document => {
      writeCallback(document, stringifier);
    });

    stringifier.end();

    // TODO Might need a more meaningful solution
    process.on('exit', () => {
      stream.end();
    });
  }
};
