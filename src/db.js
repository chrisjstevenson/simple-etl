var sql = require('mssql'),
    assert = require('assert'),
    logger = require('./logger'),
    config = require('./config');


exports.connect = function (callback) {

    var connection = new sql.Connection(config.db, function (err) {
        if (err) {
            logger.error(err);
        }
        else {
            logger.info('MSSQL: Connection established on ' + config.db.server + ',' + config.db.port);
            logger.info('MSSQL: Max pools set to ' + config.db.pool.max);
            callback(connection);
        }
    });
};


exports.promiseTest = function () {

    var connection = new sql.Connection(config.db);
    connection.connect().then(function () {
        var request = new sql.Request(connection);
        request.query('select 1 as number').then(function (recordset) {

            logger.info(recordset);
            logger.info('executed');

        }).catch(function (err) {
            logger.error(err);
        });
    }).catch (function (err) {
        logger.error(err);
    });
};


exports.getTrendData = function () {

    var query = 'SELECT top 100' +
        'ti.TradeInID,' +
        'l.LotID,' +
        'pc.serialnumber,' +
        'sku.sku,' +
        "pr.ProfileID," +
        'sku.Dept,' +
        'sku.Class,' +
        'sku.SubClass,' +
        'l.InvType,' +
        'l.ProdcondID,' +
        'pc.visibleCondID,' +
        'pc.InOriginalBox,' +
        'Days_In_Warehouse = DATEDIFF(DAY, ti.DateQuoted, ih.ShipDate),' +
        'p.PaymentAmount,' +
        'ti.QuoteTotal,' +
        //'ti.TestedTotal,' +
        'ii.SalePrice ' +
        //'ih.ShipDate ' +
    'FROM dbo.AM_Lots AS l ' +
    'LEFT OUTER JOIN dbo.AM_InvoiceItems AS ii ON l.LotID = ii.LotID ' +
    'LEFT OUTER JOIN dbo.AM_InvoiceHeader AS ih ON ii.InvoiceID = ih.InvoiceID ' +
    'LEFT OUTER JOIN dbo.AM_TradeInLots AS tl ON l.LotID = tl.LotID ' +
    'LEFT OUTER JOIN dbo.AM_TradeIn AS ti ON tl.TradeInID = ti.TradeInID ' +
    'LEFT OUTER JOIN dbo.AM_TradeInPayments AS p ON ti.TradeInID = p.TradeInID ' +
    'INNER JOIN dbo.BBSKUData sku ON l.ConsignorSKU = sku.SKU ' +
    'LEFT OUTER JOIN dbo.AM_Profile pr ON l.ProfileID = pr.ProfileID ' +
    'LEFT JOIN AM_ProductCondition pc ON l.LotID = pc.LotID ' +
    'WHERE ' +
    "ti.DateQuoted > '1/1/2015'" +
    'AND ii.SalePrice IS NOT NULL';

    return query;
}


exports.createLotIdQuery = function (lotId) {

    var query = 'SELECT '+
        'ti.TradeInID, '+
        'pr.ProfileName as profileName, '+
        'Days_In_Warehouse = DATEDIFF(DAY, ti.DateQuoted, ih.ShipDate), '+
        'ti.DateReceived as dateReceived, '+
        'l.LotID, '+
        'sku.sku,'+
        'sku.title, '+
        'ti.TradeInID, '+
        'ti.DateQuoted,   '+
        'p.PaymentAmount,    '+
        'ti.QuoteTotal,  '+
        'ti.TestedTotal,  '+
        'ii.SalePrice,   '+
        'ih.ShipDate '+
        'FROM dbo.AM_Lots AS l '+
        'LEFT OUTER JOIN dbo.AM_InvoiceItems AS ii ON l.LotID = ii.LotID '+
        'LEFT OUTER JOIN dbo.AM_InvoiceHeader AS ih ON ii.InvoiceID = ih.InvoiceID '+
        'LEFT OUTER JOIN dbo.AM_TradeInLots AS tl ON l.LotID = tl.LotID '+
        'LEFT OUTER JOIN dbo.AM_TradeIn AS ti ON tl.TradeInID = ti.TradeInID '+
        'LEFT OUTER JOIN dbo.AM_TradeInPayments AS p ON ti.TradeInID = p.TradeInID '+
        'LEFT OUTER JOIN dbo.BBSKUData sku ON l.ConsignorSKU = sku.SKU '+
        'LEFT OUTER JOIN dbo.AM_Profile pr ON l.ProfileID = pr.ProfileID '+
        'WHERE l.LotID = ' + lotId;

    return query;

};


exports.fetch = function (connection, lotId, callback) {

    var query = 'SELECT '+
        'ti.TradeInID, '+
        'pr.ProfileName, '+
        'Days_In_Warehouse = DATEDIFF(DAY, ti.DateQuoted, ih.ShipDate), '+
        'ti.DateReceived, '+
        'l.LotID, '+
        'sku.sku,'+
        'sku.title, '+
        'ti.TradeInID, '+
        'ti.DateQuoted,   '+
        'p.PaymentAmount,    '+
        'ti.QuoteTotal,  '+
        'ti.TestedTotal,  '+
        'ii.SalePrice,   '+
        'ih.ShipDate '+
        'FROM dbo.AM_Lots AS l '+
        'LEFT OUTER JOIN dbo.AM_InvoiceItems AS ii ON l.LotID = ii.LotID '+
        'LEFT OUTER JOIN dbo.AM_InvoiceHeader AS ih ON ii.InvoiceID = ih.InvoiceID '+
        'LEFT OUTER JOIN dbo.AM_TradeInLots AS tl ON l.LotID = tl.LotID '+
        'LEFT OUTER JOIN dbo.AM_TradeIn AS ti ON tl.TradeInID = ti.TradeInID '+
        'LEFT OUTER JOIN dbo.AM_TradeInPayments AS p ON ti.TradeInID = p.TradeInID '+
        'LEFT OUTER JOIN dbo.BBSKUData sku ON l.ConsignorSKU = sku.SKU '+
        'LEFT OUTER JOIN dbo.AM_Profile pr ON l.ProfileID = pr.ProfileID '+
        'WHERE l.LotID = ' + lotId;
    //logger.debug(query);

    var request = new sql.Request(connection);
    request.query(query, function(err, recordset) {
        if (err) {
            logger.error(err);
        }
        else {
            //logger.info(recordset);
            callback(recordset);
        }
    });
}