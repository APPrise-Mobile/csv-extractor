'use strict';
var csv = require('csv-parse');
var fs = require('fs');
var transform = require('stream-transform');
var q = require('q');
var _ = require('lodash');

module.exports = function CSVExtractor(filepath, columnNames, groupBy, reportErrors) {
  var fpath = filepath;

  var parse = function parse(filePath) {
    var deferred = q.defer();
    var output = [];
    var parser = csv({
      delimiter: ','
    });

    var input = fs.createReadStream(filePath);
    var colNames;
    var errors = []
    var rowNumber = 0
    var transformer = transform(function (record) {
      //increment the row number
      rowNumber++

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
        if (_.isUndefined(rec) || _.isNull(rec)) {
          errors.push({row: rowNumber, column: index})
          return
        }
        rec = rec.trim();
        if (_.isEmpty(rec)) {
          errors.push({row: rowNumber, column: index})
          return
        }
        if (rec !== '' && rec !== ' ') {
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
      deferred.resolve({output: output, errors: errors});
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
        var output = dataArray.output
        var errors = dataArray.errors
        if (groupBy) {
          output = _.groupBy(output, groupBy);
        }

        if (reportErrors) {
          return {output: output, errors: errors}
        }
        return output
      });
  };

  return {
    extract: extract
  };
};
