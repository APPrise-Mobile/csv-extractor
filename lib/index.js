'use strict';
var csv = require('csv-parse');
var fs = require('fs');
var transform = require('stream-transform');
var q = require('q');
var _ = require('lodash');

module.exports = function CSVExtractor(filepath, columnNames, groupBy) {
  var fpath = filepath;

  var parse = function parse(filePath) {
    var deferred = q.defer();
    var output = [];
    var parser = csv({
      delimiter: ','
    });

    var input = fs.createReadStream(filePath);
    var colNames;
    var transformer = transform(function (record) {
      //set the column names
      if (_.isUndefined(colNames)) {
        if (columnNames) {
          colNames = [];
          _.forEach(record, function(columnName) {
            columnName = columnName.trim();
            colNames.push(columnName);
          });
          return;
        }
      }
      var data = {};
      _.forEach(record, function (rec, index) {
        if (rec !== '' && rec !== ' ') {
          rec = rec.trim();
          if (columnNames) {
            data[colNames[index]] = rec;
          } else {
            data[index] = rec;
          }
        }
      });
      output.push(data);
    });
    parser.on('finish', function () {
      deferred.resolve(output);
    });
    parser.on('error', function (err) {
      deferred.reject(err);
    });

    input.pipe(parser).pipe(transformer);
    return deferred.promise;
  };

  var extract = function extract() {
    return parse(fpath)
      .then(function (dataArray) {
        if (groupBy) {
          return _.groupBy(dataArray, groupBy);
        } else {
          return dataArray;
        }
      });
  };

  return {
    extract: extract
  };
};
