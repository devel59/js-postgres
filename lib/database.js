const { createSavepointName } = require('./savepoint');

const hrtime = process.hrtime.bigint;
const transactionQueries = {
    begin: {
        text: 'BEGIN'
    },
    commit: {
        text: 'COMMIT'
    },
    rollback: {
        text: 'ROLLBACK'
    }
};

/**
 * @typedef PgPool
 * @property {PgPool_connect} connect
 */
/**
 * @callback PgPool_connect
 * @returns {Promise<PgPoolClient>}
 */


/**
 * @typedef PgPoolClient
 * @property {PgPoolClient_release} release
 */
/**
 * @callback PgPoolClient_release
 * @param {Error} [error]
 */


/**
 * @typedef PgClient
 * @property {PgClient_query} query
 */
/**
 * @callback PgClient_query
 * @param {QueryConfig} queryConfig
 */


/**
 * @typedef {Object} DatabasePoolInterface
 * @property {DatabasePoolInterface_use} use
 */
/**
 * @callback DatabasePoolInterface_use
 * @param {DatabasePoolInterface_useCallback} callback
 */
/**
 * @callback DatabasePoolInterface_useCallback
 * @param {PgClient} pgClient
 */


/**
 * @typedef {Object} DatabaseLoggerInterface
 * @property {DatabaseLoggerInterface_log} debug
 * @property {DatabaseLoggerInterface_log} error
 */
/**
 * @callback DatabaseLoggerInterface_log
 * @param {string} message
 * @param {Object} data
 */


/**
 * @typedef Queryable
 * @property {Queryable_query} query
 */
/**
 * @callback Queryable_query
 * @param {QueryConfig} queryConfig
 * @returns {Promise<PgResult>}
 */


/**
 * @typedef DatabaseInterface
 * @property {Queryable_query} query
 * @property {DatabaseInterface_any} any
 * @property {DatabaseInterface_none} none
 * @property {DatabaseInterface_one} one
 * @property {DatabaseInterface_oneOrNone} oneOrNone
 * @property {DatabaseInterface_many} many
 * @property {DatabaseInterface_tx} tx
 * @property {DatabaseInterface_task} task
 */
/**
 * @callback DatabaseInterface_any
 * @param {QueryConfig} queryConfig
 * @returns {Promise<Array<*>>}
 */
/**
 * @callback DatabaseInterface_none
 * @param {QueryConfig} queryConfig
 * @returns {Promise<null>}
 */
/**
 * @callback DatabaseInterface_one
 * @param {QueryConfig} queryConfig
 * @returns {Promise<*>}
 */
/**
 * @callback DatabaseInterface_oneOrNone
 * @param {QueryConfig} queryConfig
 * @returns {Promise<*>}
 */
/**
 * @callback DatabaseInterface_many
 * @param {QueryConfig} queryConfig
 * @returns {Promise<Array<*>>}
 */
/**
 * @callback DatabaseInterface_tx
 * @param {DatabaseInterface_dbCallback} callback
 * @returns {Promise<*>}
 */
/**
 * @callback DatabaseInterface_task
 * @param {DatabaseInterface_dbCallback} callback
 * @returns {Promise<*>}
 */
/**
 * @callback DatabaseInterface_dbCallback
 * @param {DatabaseInterface} db
 */


/**
 * {@link https://node-postgres.com/api/client#clientquery}
 *
 * @typedef QueryConfig
 * @property {string} text
 * @property {Array<*>} [values]
 * @property {string} [name]
 * @property {string} [rowMode]
 * @property {Object} [types]
 */


/**
 * {@link https://node-postgres.com/api/result}
 *
 * @typedef PgResult
 * @property {Array<Object>} rows
 */


/**
 * @param {bigint} value
 * @returns {number}
 */
function nanosToMillis(value) {
    return Number(value) / 1_000_000;
}

class DatabasePool {
    /**
     * @param {Object} options
     * @param {PgPool} options.pgPool
     */
    constructor({ pgPool }) {
        this.pgPool = pgPool;
    }

    /**
     * @param {DatabasePoolInterface_useCallback} callback
     * @returns {Promise<*>}
     */
    async use(callback) {
        const client = await this.pgPool.connect();

        try {
            const result = await callback(client);
            client.release();

            return result;
        } catch (err) {
            client.release(err);

            throw err;
        }
    }
}

class QueryResultError extends Error {}

class QueryResultSomeError extends QueryResultError {
    constructor() {
        super('No return data was expected.');
    }
}

class QueryResultManyError extends QueryResultError {
    constructor() {
        super('Multiple rows were not expected.');
    }
}

class QueryResultNoneError extends QueryResultError {
    constructor() {
        super('No data returned from the query.');
    }
}

/**
 * @type {DatabaseInterface_any}
 * @this Queryable
 */
async function databaseAny(queryConfig) {
    const result = await this.query(queryConfig);
    return result.rows;
}

/**
 * @type {DatabaseInterface_none}
 * @this Queryable
 */
async function databaseNone(queryConfig) {
    const result = await this.query(queryConfig);
    if (result.rows.length > 0) {
        throw new QueryResultSomeError();
    }
    return null;
}

/**
 * @type {DatabaseInterface_one}
 * @this Queryable
 */
async function databaseOne(queryConfig) {
    const result = await this.query(queryConfig);
    if (result.rows.length === 0) {
        throw new QueryResultNoneError();
    }
    if (result.rows.length > 1) {
        throw new QueryResultManyError();
    }
    return result.rows[0];
}

/**
 * @type {DatabaseInterface_oneOrNone}
 * @this Queryable
 */
async function databaseOneOrNone(queryConfig) {
    const result = await this.query(queryConfig);
    if (result.rows.length > 1) {
        throw new QueryResultManyError();
    }
    if (result.rows.length === 1) {
        return result.rows[0];
    }
    return null;
}

/**
 * @type {DatabaseInterface_many}
 * @this Queryable
 */
async function databaseMany(queryConfig) {
    const result = await this.query(queryConfig);
    if (result.rows.length === 0) {
        throw new QueryResultNoneError();
    }
    return result.rows;
}

/**
 * @param {Function} cls
 */
function makeDatabase(cls) {
    cls.prototype.any = databaseAny;
    cls.prototype.none = databaseNone;
    cls.prototype.one = databaseOne;
    cls.prototype.oneOrNone = databaseOneOrNone;
    cls.prototype.many = databaseMany;
}

/**
 * @param {DatabaseInterface} db
 * @param {Object} queries
 * @param {QueryConfig} queries.begin
 * @param {QueryConfig} queries.commit
 * @param {QueryConfig} queries.rollback
 * @param {DatabaseInterface_dbCallback} callback
 * @returns {Promise<*>}
 */
async function runTransaction(db, queries, callback) {
    await db.query(queries.begin);

    try {
        const result = await callback(db);
        await db.query(queries.commit);

        return result;
    } catch (err) {
        await db.query(queries.rollback);

        throw err;
    }
}

/**
 * @param {Error} error
 * @returns {Error}
 */
function createLogError(error) {
    const logError = new Error(error.message);
    logError.cause = error;

    return logError;
}

function defaultCreateTransactionDatabase({ client, parentDatabase }) {
    return new TransactionDatabase({
        pgClient: client,
        logger: parentDatabase.logger
    });
}

function defaultCreateTaskDatabase({ client, parentDatabase }) {
    return new TaskDatabase({
        pgClient: client,
        logger: parentDatabase.logger,
        createTransactionDatabase: parentDatabase.createTransactionDatabase
    });
}

class Database {
    /**
     * @param {Object} options
     * @param {DatabasePoolInterface} options.pool
     * @param {DatabaseLoggerInterface} options.logger
     * @param {Function} [options.createTransactionDatabase]
     * @param {Function} [options.createTaskDatabase]
     */
    constructor({
        pool,
        logger,
        createTransactionDatabase = defaultCreateTransactionDatabase,
        createTaskDatabase = defaultCreateTaskDatabase
    }) {
        this.pool = pool;
        this.logger = logger;
        this.createTransactionDatabase = createTransactionDatabase;
        this.createTaskDatabase = createTaskDatabase;
    }

    /**
     * @type {Queryable_query}
     */
    async query(queryConfig) {
        const logData = {
            query: queryConfig,
            duration: {
                query: 0,
                client: 0,
                total: 0
            }
        };

        const totalStartTime = hrtime();
        try {
            const poolResult = await this.pool.use(async (client) => {
                logData.duration.client = nanosToMillis(hrtime() - totalStartTime);

                const queryStartTime = hrtime();
                const clientResult = await client.query(queryConfig);
                logData.duration.query = nanosToMillis(hrtime() - queryStartTime);

                return clientResult;
            });

            logData.duration.total = nanosToMillis(hrtime() - totalStartTime);
            this.logger.debug('sql-query', logData);

            return poolResult;
        } catch (err) {
            logData.duration.total = nanosToMillis(hrtime() - totalStartTime);
            logData.error = createLogError(err);
            this.logger.error('sql-error', logData);

            throw err;
        }
    }

    /**
     * @type {DatabaseInterface_tx}
     */
    tx(callback) {
        return this.pool.use((client) => {
            return runTransaction(
                this.createTransactionDatabase({
                    client,
                    parentDatabase: this
                }),
                transactionQueries,
                callback
            );
        });
    }

    /**
     * @type {DatabaseInterface_task}
     */
    task(callback) {
        return this.pool.use(async (client) => {
            return callback(this.createTaskDatabase({
                client,
                parentDatabase: this
            }));
        });
    }
}
makeDatabase(Database);

/**
 * @type {Queryable_query}
 * @this TransactionDatabase|TaskDatabase
 */
async function innerDatabaseQuery(queryConfig) {
    const logData = {
        query: queryConfig,
        duration: {
            query: 0
        }
    };

    const queryStartTime = hrtime();
    try {
        const result = await this.pgClient.query(queryConfig);
        logData.duration.query = nanosToMillis(hrtime() - queryStartTime);
        this.logger.debug('sql-query', logData);

        return result;
    } catch (err) {
        logData.duration.query = nanosToMillis(hrtime() - queryStartTime);
        logData.error = createLogError(err);
        this.logger.error('sql-error', logData);

        throw err;
    }
}

/**
 * @param {Function} cls
 */
function makeInnerDatabase(cls) {
    cls.prototype.query = innerDatabaseQuery;
}

class TransactionDatabase {
    /**
     * @param {Object} options
     * @param {PgClient} options.pgClient
     * @param {DatabaseLoggerInterface} options.logger
     */
    constructor({ pgClient, logger }) {
        this.pgClient = pgClient;
        this.logger = logger;
    }

    /**
     * @type {DatabaseInterface_tx}
     */
    async tx(callback) {
        const savepointName = createSavepointName();

        return runTransaction(
            this,
            {
                begin: {
                    text: `SAVEPOINT ${savepointName}`
                },
                commit: {
                    text: `RELEASE SAVEPOINT ${savepointName}`
                },
                rollback: {
                    text: `ROLLBACK TO SAVEPOINT ${savepointName}`
                }
            },
            callback
        );
    }

    /**
     * @type {DatabaseInterface_task}
     */
    async task(callback) {
        return callback(this);
    }
}
makeInnerDatabase(TransactionDatabase);
makeDatabase(TransactionDatabase);

class TaskDatabase {
    /**
     * @param {Object} options
     * @param {PgClient} options.pgClient
     * @param {DatabaseLoggerInterface} options.logger
     * @param {Function} options.createTransactionDatabase
     */
    constructor({
        pgClient,
        logger,
        createTransactionDatabase
    }) {
        this.pgClient = pgClient;
        this.logger = logger;
        this.createTransactionDatabase = createTransactionDatabase;
    }

    /**
     * @type {DatabaseInterface_tx}
     */
    async tx(callback) {
        return runTransaction(
            this.createTransactionDatabase({
                client: this.pgClient,
                parentDatabase: this
            }),
            transactionQueries,
            callback
        );
    }

    /**
     * @type {DatabaseInterface_task}
     */
    async task(callback) {
        return callback(this);
    }
}
makeInnerDatabase(TaskDatabase);
makeDatabase(TaskDatabase);

module.exports = {
    Database,
    DatabasePool,
    TransactionDatabase,
    TaskDatabase
};