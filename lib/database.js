// @ts-check
const hrtime = process.hrtime.bigint;


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
 * Время для подсчета продолжительности
 *
 * @returns {number}
 */
function getTime() {
    return Number(hrtime() / 1000000n);
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
            // @ts-ignore
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
 * @param {DatabaseInterface_dbCallback} callback
 * @returns {Promise<*>}
 */
async function runTransaction(db, callback) {
    await db.query({
        text: 'BEGIN'
    });
    try {
        const result = await callback(db);
        await db.query({
            text: 'COMMIT'
        });
        return result;
    } catch (err) {
        await db.query({
            text: 'ROLLBACK'
        });
        throw err;
    }
}

/**
 * @param {Object} error
 * @returns {Object}
 */
function getErrorLogData(error) {
    error.originalStack = error.stack;
    Error.captureStackTrace(error);

    return {
        stack: error.stack,
        data: error
    };
}

class Database {
    /**
     * @param {Object} options
     * @param {DatabasePoolInterface} options.pool
     * @param {DatabaseLoggerInterface} options.logger
     */
    constructor({ pool, logger }) {
        this.pool = pool;
        this.logger = logger;
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

        const totalStartTime = getTime();
        try {
            const poolResult = await this.pool.use(async (client) => {
                logData.duration.client = getTime() - totalStartTime;

                const queryStartTime = getTime();
                const clientResult = await client.query(queryConfig);
                logData.duration.query = getTime() - queryStartTime;
                return clientResult;
            });
            logData.duration.total = getTime() - totalStartTime;
            this.logger.debug('sql-query', logData);
            return poolResult;
        } catch (err) {
            logData.duration.total = getTime() - totalStartTime;
            logData.error = getErrorLogData(err);
            this.logger.error('sql-error', logData);
            throw err;
        }
    }

    /**
     * @type {DatabaseInterface_tx}
     */
    tx(callback) {
        return this.pool.use((client) => {
            // @ts-ignore
            return runTransaction(new TransactionDatabase({
                pgClient: client,
                logger: this.logger
            }), callback);
        });
    }

    /**
     * @type {DatabaseInterface_task}
     */
    task(callback) {
        return this.pool.use(async (client) => {
            // @ts-ignore
            return callback(new TaskDatabase({
                pgClient: client,
                logger: this.logger
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

    const queryStartTime = getTime();
    try {
        const result = await this.pgClient.query(queryConfig);
        logData.duration.query = getTime() - queryStartTime;
        this.logger.debug('sql-query', logData);
        return result;
    } catch (err) {
        logData.duration.query = getTime() - queryStartTime;
        logData.error = getErrorLogData(err);
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
     *
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
        // @ts-ignore
        return callback(this);
    }

    /**
     * @type {DatabaseInterface_task}
     */
    async task(callback) {
        // @ts-ignore
        return callback(this);
    }
}
makeInnerDatabase(TransactionDatabase);
makeDatabase(TransactionDatabase);

class TaskDatabase {
    /**
     *
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
    tx(callback) {
        // @ts-ignore
        return runTransaction(new TransactionDatabase({
            pgClient: this.pgClient,
            logger: this.logger
        }), callback);
    }

    /**
     * @type {DatabaseInterface_task}
     */
    async task(callback) {
        // @ts-ignore
        return callback(this);
    }
}
makeInnerDatabase(TaskDatabase);
makeDatabase(TaskDatabase);
