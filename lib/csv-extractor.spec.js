'use strict';
var csvExtractor = require('./index');
var q = require('q');
var _ = require('lodash');
var should = require('should');

describe('CSV Extractor', function() {

  beforeEach(function() {

  });

  it('should parse a csv with newlines correctly', function(done) {
    var extractor = csvExtractor('test/test.csv');
    extractor.extract()
      .then(function(data) {
        data.length.should.equal(5);
        data[0][0].should.equal('frankros91@gmail.com');
        data[1][0].should.equal('darata@gmail.com');
        data[2][0].should.equal('jasonmayde@gmail.com');
        data[3][0].should.equal('tmartinez@gmail.com');
        data[4][0].should.equal('elevy@gmail.com');
        done();
      })
      .then(null, done);
  });

  it('should parse a csv with a second column correctly', function(done) {
    var extractor = csvExtractor('test/test-1.csv');
    extractor.extract()
      .then(function(data) {
        data[0][0].should.equal('frankros91@gmail.com');
        data[0][1].should.equal('group1');
        data[1][0].should.equal('darata@gmail.com');
        data[1][1].should.equal('group2');
        data[2][0].should.equal('jasonmayde@gmail.com');
        data[2][1].should.equal('group3');
        data[3][0].should.equal('tmartinez@gmail.com');
        data[3][1].should.equal('group2');
        data[4][0].should.equal('elevy@gmail.com');
        data[4][1].should.equal('group1');
        done();
      })
      .then(null, done);
  });

  it('should set the keys as the column names if the csv has column names and you sepecify it', function(done) {
    var extractor = csvExtractor('test/test-column-names.csv', true);
    extractor.extract()
      .then(function(data) {
        data[0].registrationCode.should.equal('frankros91@gmail.com');
        data[0].groupName.should.equal('group1');
        data[1].registrationCode.should.equal('darata@gmail.com');
        data[1].groupName.should.equal('group2');
        data[2].registrationCode.should.equal('jasonmayde@gmail.com');
        data[2].groupName.should.equal('group3');
        data[3].registrationCode.should.equal('tmartinez@gmail.com');
        data[3].groupName.should.equal('group2');
        data[4].registrationCode.should.equal('elevy@gmail.com');
        data[4].groupName.should.equal('group1');
        done();
      })
      .then(null, done);
  });

  it('should also work if the csv is only one column long with column names', function(done) {
    var extractor = csvExtractor('test/test-single-column-name.csv', true);
    extractor.extract()
      .then(function(data) {
        data[0].registrationCode.should.equal('frankros91@gmail.com');
        data[1].registrationCode.should.equal('darata@gmail.com');
        data[2].registrationCode.should.equal('jasonmayde@gmail.com');
        data[3].registrationCode.should.equal('tmartinez@gmail.com');
        data[4].registrationCode.should.equal('elevy@gmail.com');
        done();
      })
      .then(null, done);
  });

  it('should group the output by the specified key', function(done) {
    var extractor = csvExtractor('test/test-column-names.csv', true, 'groupName');
    extractor.extract()
      .then(function(data) {
        var group1 = data.group1;
        group1[0].registrationCode.should.equal('frankros91@gmail.com');
        group1[1].registrationCode.should.equal('elevy@gmail.com');

        var group2 = data.group2;
        group2[0].registrationCode.should.equal('darata@gmail.com');
        group2[1].registrationCode.should.equal('tmartinez@gmail.com');

        var group3 = data.group3;
        group3[0].registrationCode.should.equal('jasonmayde@gmail.com');
        done();
      })
      .then(null, done);
  });
});
