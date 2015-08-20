var db = require('./../src/db'),
    expect = require('expect'),
    sql = require('mssql'),
    config = require('./../src/config');
    _ = require('lodash');


describe('Tests for db.js:', function () {

    it('should connect to target database', function (done) {

        //db.connect(function(connectionInfo) {
        //    expect(connectionInfo).toNotBe(null);
        //    done();
        //});
        var connection = new sql.Connection(config.connections.targetDb);
        connection.connect().then(function () {
            expect(connection).toNotBe(null);
            done();
        });
    });

    it('should connect to destination database', function(done) {

        var connection = new sql.Connection(config.connections.destinationDb);
        connection.connect().then(function () {
            expect(connection).toNotBe(null);
            done();
        }).catch(function (err) {
            logger.error(err);
        });;
    });


    it('should do this and then that', function (done) {

        var connection = new sql.Connection(config.connections.targetDb);
        connection.connect().then(function () {
            var request = new sql.Request(connection);
            var query = db.getTrendData();

            request.query('SELECT 1 AS NUMBER;').then(function (recordset) {
                expect(recordset[0]).toNotBeNull;
                done();
            }).catch(function (err) {
                logger.error(err);
            });
        }).catch(function (err) {
            logger.error(err);
        });
    });


    it('should use streaming', function(done) {
        this.timeout(10000);
        var connection = new sql.Connection(config.connections.targetDb);
        connection.connect().then(function () {
            var request = new sql.Request(connection);
            request.stream = true;
            request.query(db.getTrendData());

            request.on('recordset', function(columns) {
                //console.log(columns)
            });

            request.on('row', function(row) {
                expect(row.ProfileName).toBeA('string')
                expect(row.LotID).toBeA('number');
                //console.log(row);
            });

            request.on('error', function(err) {
                console.log(err);
            });

            request.on('done', function(returnValue) {
                done();
            });

        }).catch(function (err) {
            console.log(err);
        });
    });


    it('should create a table if it does not exist', function(done) {

        var connection = new sql.Connection(config.connections.destinationDb);
        connection.connect().then(function () {

            var table = new sql.Table('Products');
            table.create = true;
            table.columns.add('a', sql.Int, {nullable: true});
            table.columns.add('b', sql.VarChar(50), {nullable: false});
            table.rows.add(777, 'test');

            var request = new sql.Request(connection);
            request.bulk(table, function(err, rowCount) {
                if (err) console.log(err);
                done();
            });


        }).catch(function (err) {
            console.log(err);
        });;
    });


    it('should create a table column from a recordset column definition', function(done) {

        var definition = {
            index: 3,
            name: 'sku',
            length: 4,
            type: sql.VarChar(50),
            scale: undefined,
            precision: undefined,
            nullable: true,
            caseSensitive: false,
            identity: false,
            readOnly: false
        }

        var connection = new sql.Connection(config.connections.destinationDb);
        connection.connect().then(function () {

            var table = new sql.Table('Trend_UnitTest');
            table.create = true;
            table.columns.add(definition.name, definition.type, { nullable: definition.nullable });
            table.rows.add(77773434);

            var request = new sql.Request(connection);
            request.bulk(table, function(err, rowCount) {
                if (err) console.log(err);
                done();
            });


        }).catch(function (err) {
            console.log(err);
        });;
    });


    //it('should create a table in a destination db with recordset columns', function (done) {
    //
    //    this.timeout(10000);
    //    var table = undefined;
    //
    //    var connection = new sql.Connection(config.connections.targetDb);
    //    connection.connect().then(function () {
    //        var request = new sql.Request(connection);
    //        request.stream = true;
    //        request.query(db.getTrendData());
    //
    //        request.on('recordset', function(columns) {
    //
    //            table = new sql.Table('Dynamictable_UnitTest');
    //            table.create = true;
    //
    //            var arr = _.values(columns)
    //            arr.forEach(function(def) {
    //                table.columns.add(def.name, def.type, { nullable: def.nullable } );
    //            });
    //        });
    //
    //
    //        request.on('row', function(row) {
    //            var arr = _.values(row);
    //            //rows is a 2D array
    //            table.rows.push(arr);
    //        });
    //
    //        request.on('error', function(err) {
    //            console.log(err);
    //        });
    //
    //        request.on('done', function(returnValue) {
    //        });
    //
    //    }).catch(function (err) {
    //        console.log(err);
    //    });
    //
    //
    //    var destinationConnection = new sql.Connection(config.connections.destinationDb);
    //    destinationConnection.connect().then(function () {
    //
    //        //console.log(table);
    //
    //        var createTableRequest = new sql.Request(destinationConnection);
    //        createTableRequest.bulk(table, function(err, rowCount) {
    //            //if (err) console.log(err);
    //            done();
    //        });
    //
    //    }).catch(function (err) {
    //        //console.log(err);
    //    });
    //});


    it('should create table from recordset', function(done) {

        var table = undefined;
        var connection = new sql.Connection(config.connections.targetDb);
        connection.connect().then(function () {
            var request = new sql.Request(connection);
            var query = db.getTrendData();

            request.query(query).then(function (recordset) {
                var temp = recordset.toTable();
                table = new sql.Table('dynamicTableTest');
                table.colums = temp.columns;
                table.rows = temp.rows;

            }).catch(function (err) {
                logger.error(err);
            });
        }).catch(function (err) {
            logger.error(err);
        });


        var destinationConnection = new sql.Connection(config.connections.destinationDb);
        destinationConnection.connect().then(function () {

            //console.log(table);
            var createTableRequest = new sql.Request(destinationConnection);
            createTableRequest.bulk(table, function(err, rowCount) {
                if (err) console.log(err);
                done();
            });

        }).catch(function (err) {
            //console.log(err);
        });


    });


});


function convertDataType(definition) {

    if(definition.type.declaration.localeCompare('varchar') == 0) {
        return sql.VarChar(sql.MAX);
    } else {
        return definition.type;
    }
};


// Notes:
// 1.) Call done() after your test assertions.
// 2.)




