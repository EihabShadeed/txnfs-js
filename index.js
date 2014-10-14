/**
 * @author Ali Ismael
 *
 */


'use strict';

var path = require('path')
    , fs = require('fs')
    , txn_file = require('./lib/txn_file');


(function () {

    var txnFiles = [];
    var fsRoot = path.dirname(require.main.filename) + '/txn_fs';

    if (!fs.existsSync(fsRoot)) {
        fsRoot = path.resolve('.') + '/txn_fs';
        if (!fs.existsSync(fsRoot)) {
            throw new Error('txn_fs root folder does not exist at: ' + fsRoot);
        }
    }

    /**
     *
     * @param fileName
     * @returns {*}
     */
    function getTXNFile(fileName) {
        if (!fileName) {
            throw new Error('Transaction file name can\'t be null, nor empty string');
        }

        var abxoluteFileName = path.normalize(fsRoot + path.sep + fileName);
        var retVal;
        retVal = txnFiles[abxoluteFileName];

        if (!retVal) {
            retVal = txn_file.createTXNFile(abxoluteFileName);
            txnFiles[abxoluteFileName] = retVal;
        }
        return retVal;
    }


    /**
     *
     *
     */
    module.exports = Object.create(Object.prototype,
        {
            getTXNFile: {
                value: getTXNFile
            }
        }
    );

}());




