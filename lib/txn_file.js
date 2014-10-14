/**
 * Created by aismael on 2/6/14.
 */

'use strict';

var path = require('path')
    , fs = require('fs');

(function () {


    var DIRTY_FILE_EXTENSION = ".txn.dirty";
    var ROLLBACK_FILE_EXTENSION = ".txn.rollback";
    var TXN_LOG_FILE_EXTENSION = ".txn.log";


    /**
     * Transaction status
     */
    var Status = {
        STATUS_ACTIVE: 0,
        STATUS_COMMITTED: 3,
        STATUS_COMMITTING: 8,
        STATUS_MARKED_ROLLBACK: 1,
        STATUS_NO_TRANSACTION: 6,
        STATUS_PREPARED: 2,
        STATUS_PREPARING: 7,
        STATUS_ROLLEDBACK: 4,
        STATUS_ROLLING_BACK: 9,
        STATUS_UNKNOWN: 5
    };


    /**
     *
     * @param fileName
     */
    function createTXNFile(fileName) {
        var txnStatus = Status.STATUS_NO_TRANSACTION;
        var txnFileName = fileName;
        var dirtyFileName = fileName + DIRTY_FILE_EXTENSION;
        var rollbackFileName = fileName + ROLLBACK_FILE_EXTENSION;
        var txnLogFileName = fileName + TXN_LOG_FILE_EXTENSION;
        var rollbackOnly = false;


        /**
         *
         * @param sourceFile
         * @param destFile
         */
        function copyFile(sourceFile, destFile) {
            if (fs.existsSync(sourceFile)) {
                var buff = fs.readFileSync(sourceFile);
                fs.writeFileSync(destFile, buff);
            }
        }


        /**
         *
         */
        function logTxnEntry() {

            var logEnt = {};
            logEnt.txnStatus = txnStatus;

            var logEntStr = JSON.stringify(logEnt);

            switch (txnStatus) {
                case Status.STATUS_UNKNOWN:
                case Status.STATUS_NO_TRANSACTION:
                case Status.STATUS_ACTIVE:
                    fs.writeFileSync(txnLogFileName, logEntStr, {encoding: 'utf8'});
                    break;
                default:
                    fs.appendFileSync(txnLogFileName, logEntStr, {encoding: 'utf8'});
                    break;
            }
        }


        /**
         *
         */
        function redoTxnLog() {

            if (fs.existsSync(txnLogFileName)) {
                var txnEntryStrList = fs.readFileSync(txnFileName, {'encoding': 'utf8'}).split('\n');

                txnEntryStrList = txnEntryStrList || [];

                txnStatus = Status.STATUS_UNKNOWN;
                for (var i = 0; i < txnEntryStrList.length; i++) {
                    try {
                        var txnLogEnt = JSON.parse(txnEntryStrList[i]);
                        txnStatus = txnLogEnt.txnStatus;
                    } catch (err) {
                    }
                }

                switch (txnStatus) {
                    case Status.STATUS_COMMITTING:
                        try {
                            copyFile(dirtyFileName, txnFileName);
                        } catch (e) {
                            setRollbackOnly();
                            throw e;
                        }
                        // delete the dirty file if exists:
                        if (fs.existsSync(dirtyFileName)) {
                            fs.unlinkSync(dirtyFileName);
                        }
                        break;
                    case Status.STATUS_ROLLING_BACK:
                        if (fs.existsSync(rollbackFileName)) {
                            copyFile(rollbackFileName, txnFileName);
                        }
                        break;
                    default:
                        break;
                }

                txnStatus = Status.STATUS_NO_TRANSACTION;
                logTxnEntry();

                if (fs.existsSync(dirtyFileName)) {
                    fs.unlinkSync(dirtyFileName);
                }

                if (fs.existsSync(rollbackFileName)) {
                    fs.unlinkSync(rollbackFileName);
                }
            }
        }


        /**
         *
         * @param dirPath
         */
        function mkdirRecursive(dirPath) {
            var parentPath = path.dirname(dirPath);

            if (!fs.existsSync(parentPath)) {
                mkdirRecursive(parentPath);
            }

            if (!fs.existsSync(dirPath)) {
                fs.mkdirSync(dirPath);
            }
        }


        /**
         *
         */
        function begin() {
            if (txnStatus != Status.STATUS_NO_TRANSACTION) {
                throw new Error("TXN Status is already set");
            }

            if (fs.existsSync(txnLogFileName)) {
                // this will recover from a previous crash:
                redoTxnLog();
            }

            if (!fs.existsSync(txnFileName)) {
                mkdirRecursive(path.dirname(txnFileName));
                var buff = new Buffer(0);
                fs.writeFileSync(txnFileName, buff);
            }

            copyFile(txnFileName, dirtyFileName);

            txnStatus = Status.STATUS_ACTIVE;
            logTxnEntry();
        }


        /**
         *
         */
        function commit() {

            switch (txnStatus) {
                case Status.STATUS_COMMITTING:
                    throw new Error("Transaction is already in COMMITTING state.");
                case Status.STATUS_NO_TRANSACTION:
                    throw new Error("Not in an active Transaction state.");
                case Status.STATUS_MARKED_ROLLBACK:
                case Status.STATUS_ROLLING_BACK:
                    throw new Error("Transaction is already in ROLLEDBACK state.");
                case Status.STATUS_UNKNOWN:
                    throw new Error("Transaction is in unknown state.");
                default:
                    if (rollbackOnly) {
                        throw new Error("Transaction is in ROLLEDBACK_ONLY state.");
                    }
                    break;
            }

            txnStatus = Status.STATUS_PREPARING;
            logTxnEntry();

            copyFile(txnFileName, rollbackFileName);

            txnStatus = Status.STATUS_PREPARED;
            logTxnEntry();

            txnStatus = Status.STATUS_COMMITTING;
            logTxnEntry();

            try {
                copyFile(dirtyFileName, txnFileName);
            } catch (e) {
                setRollbackOnly();
                throw e;
            }

            txnStatus = Status.STATUS_COMMITTED;
            logTxnEntry();

            txnStatus = Status.STATUS_ACTIVE;
            logTxnEntry();
        }


        /**
         *
         */
        function rollback() {
            switch (txnStatus) {
                case Status.STATUS_NO_TRANSACTION:
                case Status.STATUS_COMMITTED:
                case Status.STATUS_ROLLEDBACK:
                case Status.STATUS_ROLLING_BACK:
                case Status.STATUS_UNKNOWN:
                    throw new Error("Transaction is not in a state which can be rolled back.");
                default:
                    break;
            }

            txnStatus = Status.STATUS_ROLLING_BACK;
            logTxnEntry();

            if (fs.existsSync(rollbackFileName)) {
                copyFile(rollbackFileName, txnFileName);
            }

            txnStatus = Status.STATUS_ROLLEDBACK;
            logTxnEntry();

            txnStatus = Status.STATUS_ACTIVE;
            logTxnEntry();
        }

        /**
         *
         */
        function setRollbackOnly() {
            switch (txnStatus) {
                case Status.STATUS_ACTIVE:
                case Status.STATUS_PREPARING:
                case Status.STATUS_PREPARED:
                case Status.STATUS_COMMITTED:
                case Status.STATUS_COMMITTING:
                    rollbackOnly = true;
                    break;
                default:
                    rollbackOnly = false;
                    break;
            }
        }

        /**
         *
         * @returns { A data } buffer containing the file contents
         */
        function readFile() {
            if (txnStatus != Status.STATUS_ACTIVE) {
                throw new Error('File transaction status is not STATUS_ACTIVE, begin a transaction or complete earlier transactions ');
            }

            return  fs.readFileSync(txnFileName);
        }

        /**
         *
         * @param dataBuffer
         */
        function writeFile(dataBuffer) {
            if (txnStatus != Status.STATUS_ACTIVE) {
                throw new Error('File transaction status is not STATUS_ACTIVE, begin a transaction or complete earlier transactions ');
            }

            fs.writeFileSync(dirtyFileName, dataBuffer);
        }


        return Object.create(Object.prototype, {
                rollbackOnly: {
                    get: function () {
                        return rollbackOnly;
                    }
                },
                setRollbackOnly: {
                    value: setRollbackOnly
                },
                txnStatus: {
                    get: function () {
                        return txnStatus;
                    }
                },
                begin: {
                    value: begin
                },
                commit: {
                    value: commit
                },
                rollback: {
                    value: rollback
                },
                readFile: {
                    value: readFile
                },
                writeFile: {
                    value: writeFile
                }
            }
        );
    }


    /**
     *
     *
     */
    module.exports = Object.create(Object.prototype,
        {
            createTXNFile: {
                value: createTXNFile
            }
        }
    );

}());

