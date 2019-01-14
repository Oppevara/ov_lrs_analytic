const ObjectID = require('mongodb').ObjectID;

const queryMatch = {
  "lrs._id": ObjectID("5abb7996ed015bbcae98a112"),
  "statement.timestamp": {
    $gte: "2018-04-02T00:00:00.599700+03:00",
    $lt: "2018-06-06T00:00:00.599700+03:00"
  }
};

function statementsCollection(db) {
  return db.collection('statements');
}

function runAggregateQuery(collection, query) {
  query.unshift({
    $match: queryMatch
  });
  return collection.aggregate(query).toArray();
}

module.exports = {
  materialInteractions: (db) => {
    const collection = statementsCollection(db);

    return runAggregateQuery(collection, [
      {
        $addFields: {
          verb: {
            $arrayElemAt: [ { $objectToArray: "$statement.verb.display" }, 0 ] // TODO Need to find a way to extract only value as that is needed in many instances
          },
          name: {
            $arrayElemAt: [ { $objectToArray: "$statement.object.definition.name" }, 0 ]
          }
        }
      },
      {
        $group: {
          _id: "$statement.object.id",
          count: {
            $sum: 1
          },
          verbs: {
            $addToSet: "$verb.v"
          },
          name: {
            $last: "$name.v"
          }
        }
      },
      {
        $sort: {
          count: -1
        }
      }
    ]);
  },
  materialInteractionsByVerb: (db) => {
    const collection = statementsCollection(db);

    return runAggregateQuery(collection, [
      {
        $addFields: {
          verb: {
            $arrayElemAt: [ { $objectToArray: "$statement.verb.display" }, 0 ] // TODO Need to find a way to extract only value as that is needed in many instances
          },
          name: {
            $arrayElemAt: [ { $objectToArray: "$statement.object.definition.name" }, 0 ]
          }
        }
      },
      {
        $group: {
          _id: {
            url: "$statement.object.id",
            verb: "$verb.v"
          },
          count: {
            $sum: 1
          },
          name: {
            $last: "$name.v"
          }
        }
      },
      {
        $sort: {
          count: -1
        }
      }
    ]);
  },
  materialScores: (db) => {
    const collection = statementsCollection(db);

    return runAggregateQuery(collection, [
      {
        $match: {
          "statement.result.score": {
            $exists: true
          }
        }
      },
      {
        $addFields: {
          verb: {
            $arrayElemAt: [ { $objectToArray: "$statement.verb.display" }, 0 ] // TODO Need to find a way to extract only value as that is needed in many instances
          },
          name: {
            $arrayElemAt: [ { $objectToArray: "$statement.object.definition.name" }, 0 ]
          }
        }
      },
      {
        $group: {
          _id: {
            url: "$statement.object.id",
            verb: "$verb.v"
          },
          count: {
            $sum: 1
          },
          rawMin: {
            $min: "$statement.result.score.raw"
          },
          rawMax: {
            $max: "$statement.result.score.raw"
          },
          rawAvg: {
            $avg: "$statement.result.score.raw"
          },
          name: {
            $last: "$name.v"
          },
          successCount: {
            $sum: {
              $cond: {
                if:  {
                  $eq: ['$statement.result.success', true]
                },
                then: 1,
                else: 0
              }
            }
          },
          min: {
            $last: "$statement.result.score.min"
          },
          max: {
            $last: "$statement.result.score.max"
          }
        }
      },
      {
        $sort: {
          count: -1
        }
      }
    ]);
  }
};