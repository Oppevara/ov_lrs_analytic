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
  },
  contentTypeVerbs: (db) => {
    const collection = statementsCollection(db);

    return runAggregateQuery(collection, [
      {
        $addFields: {
          verb: {
            $arrayElemAt: [ { $objectToArray: "$statement.verb.display" }, 0 ] // TODO Need to find a way to extract only value as that is needed in many instances
          },
          category: {
            $arrayElemAt: [ "$statement.context.contextActivities.category", 0 ]
          }
        }
      },
      {
        $addFields: {
          cid: {
            $arrayElemAt: [ { $split: [ { $arrayElemAt: [ { $split: ["$category.id", 'H5P.'] }, 1 ] } , '-'] }, 0 ]
          }
        }
      },
      {
        $group: {
          _id: "$cid",
          verbs: {
            $addToSet: "$verb.v"
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
  materialScoresAndStatsForPeriodAndUrls: (db, options) => {
    const collection = statementsCollection(db);
    console.log(options.from.toISOString());

    return runAggregateQuery(collection, [
      {
        $match: {
          "statement.timestamp": {
            $gte: options.from.toISOString(),
            $lt: options.to.toISOString()
          },
          $or: [
            {
              "statement.object.id": {
                $in: options.ids
              }
            },
            {
              "statement.context.contextActivities.parent.0.id": {
                $in: options.ids
              }
            }
          ]
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
        $addFields: {
          cid: {
            $arrayElemAt: [ { $split: [ { $arrayElemAt: [ { $split: ["$category.id", 'H5P.'] }, 1 ] } , '-'] }, 0 ]
          }
        }
      },
      {
        $group: {
          _id: {
            url: "$statement.object.id",
            verb: "$verb.v"
          },
          name: {
            $last: "$name.v"
          },
          sessions: {
            $addToSet: "$statement.actor.account.name"
          },
          durations: {
            $push: "$statement.result.duration"
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
          successCount: {
            $sum: {
              $cond: {
                if:  {
                  $or: [
                    {
                      $eq: ['$statement.result.success', true]
                    },
                    {
                      $eq: ['$statement.result.completion', true]
                    }
                  ]
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
