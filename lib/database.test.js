const {
    Database,
    DatabasePool,
    TransactionDatabase,
    TaskDatabase
} = require('./database');

function createQueryResult(length) {
    const rows = [];
    for (let i = 0; i < length; i++) {
        rows.push({
            value: i
        });
    }

    return {
        rows
    };
}

function createPool(client) {
    return {
        use(callback) {
            return callback(client);
        }
    };
}

const testErrorMessage = 'test';

function createRowPool(rowsLength) {
    const client = {
        async query(queryConfig) {
            if (queryConfig.text === 'error') {
                throw new Error(testErrorMessage);
            }

            return createQueryResult(rowsLength);
        }
    };

    return createPool(client);
}

const fakeLogger = {
    debug() {},
    error() {}
};

const fakeQuery = {
    text: 'select 1',
    values: [1, 2, 3]
};

const errorQuery = {
    text: 'error'
};

const zeroRowDb = new Database({
    logger: fakeLogger,
    pool: createRowPool(0)
});
const oneRowDb = new Database({
    logger: fakeLogger,
    pool: createRowPool(1)
});
const twoRowDb = new Database({
    logger: fakeLogger,
    pool: createRowPool(2)
});

const queryable = expect.objectContaining({
    query: expect.any(Function)
});

function createHistoryDb() {
    const history = [];

    const client = {
        async query(queryConfig) {
            history.push(queryConfig.text);

            return createQueryResult(0);
        }
    };

    const db = new Database({
        logger: fakeLogger,
        pool: createPool(client)
    });
    db.history = history;

    return db;
}

const savepointBeginRe = /^SAVEPOINT /;
const savepointCommitRe = /^RELEASE SAVEPOINT /;
const savepointRollbackRe = /^ROLLBACK TO SAVEPOINT /;

describe('database', () => {
    test('result has rows field', async () => {
        const result = await zeroRowDb.query(fakeQuery);

        expect(result.rows).toBeInstanceOf(Array);
    });

    for (const method of ['any', 'manyOrNone']) {
        test(method, async () => {
            const zeroResult = await zeroRowDb.any(fakeQuery);
            expect(zeroResult.length).toBe(0);

            const oneResult = await oneRowDb.any(fakeQuery);
            expect(oneResult.length).toBe(1);

            const twoResult = await twoRowDb.any(fakeQuery);
            expect(twoResult.length).toBe(2);
        });
    }

    test('none', async () => {
        const zeroResult = await zeroRowDb.none(fakeQuery);
        expect(zeroResult).toBeNull();

        await expect(oneRowDb.none(fakeQuery)).rejects.toThrow();

        await expect(twoRowDb.none(fakeQuery)).rejects.toThrow();
    });

    test('one', async () => {
        await expect(zeroRowDb.one(fakeQuery)).rejects.toThrow();

        const oneResult = await oneRowDb.one(fakeQuery);
        expect(oneResult).toEqual(expect.anything());

        await expect(twoRowDb.one(fakeQuery)).rejects.toThrow();
    });

    test('one or none', async () => {
        const zeroResult = await zeroRowDb.oneOrNone(fakeQuery);
        expect(zeroResult).toBeNull();

        const oneResult = await oneRowDb.oneOrNone(fakeQuery);
        expect(oneResult).toEqual(expect.anything());

        await expect(twoRowDb.oneOrNone(fakeQuery)).rejects.toThrow();
    });

    test('many', async () => {
        await expect(zeroRowDb.many(fakeQuery)).rejects.toThrow();

        const oneResult = await oneRowDb.many(fakeQuery);
        expect(oneResult.length).toBe(1);

        const twoResult = await twoRowDb.many(fakeQuery);
        expect(twoResult.length).toBe(2);
    });

    test('logger', async () => {
        let debugMessage;
        let debugData;
        let errorMessage;
        let errorData;

        const db = new Database({
            logger: {
                debug(message, data) {
                    debugMessage = message;
                    debugData = data;
                },
                error(message, data) {
                    errorMessage = message;
                    errorData = data;
                }
            },
            pool: createRowPool(0)
        });

        await db.query(fakeQuery);
        expect(typeof debugMessage).toBe('string');
        expect(debugData.query).toEqual(expect.anything());
        expect(debugData.duration.query).toBeGreaterThan(0);
        expect(debugData.duration.client).toBeGreaterThan(0);
        expect(debugData.duration.total).toBeGreaterThan(0);

        await expect(db.query(errorQuery)).rejects.toThrow(testErrorMessage);
        expect(typeof errorMessage).toBe('string');
        expect(errorData.error).toBeInstanceOf(Error);
    });
});

describe('transaction', () => {
    test('queryable', async () => {
        const db = createHistoryDb();
        await db.tx(async (txDb) => {
            expect(txDb).toEqual(queryable);

            await txDb.task((innerDb) => {
                expect(innerDb).toEqual(queryable);
            });

            await txDb.tx((innerDb) => {
                expect(innerDb).toEqual(queryable);
            });
        });
    });

    test('success', async () => {
        const db = createHistoryDb();
        await db.tx(async (txDb) => {
            await txDb.query(fakeQuery);
        });

        expect(db.history[0]).toBe('BEGIN');
        expect(db.history[1]).toBe(fakeQuery.text);
        expect(db.history[2]).toBe('COMMIT');
    });

    test('fail', async () => {
        const db = createHistoryDb();

        await expect(
            db.tx(async (txDb) => {
                await txDb.query(fakeQuery);

                throw new Error(testErrorMessage);
            })
        ).rejects.toThrow(testErrorMessage);

        expect(db.history[0]).toBe('BEGIN');
        expect(db.history[1]).toBe(fakeQuery.text);
        expect(db.history[2]).toBe('ROLLBACK');
    });

    test('savepoint success', async () => {
        const db = createHistoryDb();
        await db.tx(async (txDb) => {
            await txDb.tx(async (spDb) => {
                await spDb.query(fakeQuery);
            });
        });

        expect(db.history[0]).toBe('BEGIN');
        expect(db.history[1]).toMatch(savepointBeginRe);
        expect(db.history[2]).toBe(fakeQuery.text);
        expect(db.history[3]).toMatch(savepointCommitRe);
        expect(db.history[4]).toBe('COMMIT');
    });

    test('savepoint fail', async () => {
        const db = createHistoryDb();
        await db.tx(async (txDb) => {
            await expect(
                txDb.tx(async (spDb) => {
                    await spDb.query(fakeQuery);

                    throw new Error(testErrorMessage);
                })
            ).rejects.toThrow(testErrorMessage);
        });

        expect(db.history[0]).toBe('BEGIN');
        expect(db.history[1]).toMatch(savepointBeginRe);
        expect(db.history[2]).toBe(fakeQuery.text);
        expect(db.history[3]).toMatch(savepointRollbackRe);
        expect(db.history[4]).toBe('COMMIT');
    });

    test('logger', async () => {
        let debugMessage;
        let debugData;
        let errorMessage;
        let errorData;

        const db = new Database({
            logger: {
                debug(message, data) {
                    debugMessage = message;
                    debugData = data;
                },
                error(message, data) {
                    errorMessage = message;
                    errorData = data;
                }
            },
            pool: createRowPool(0)
        });

        await db.tx(async (txDb) => {
            await txDb.query(fakeQuery);
            expect(typeof debugMessage).toBe('string');
            expect(debugData.query).toEqual(expect.anything());
            expect(debugData.duration.query).toBeGreaterThan(0);

            await expect(txDb.query(errorQuery)).rejects.toThrow(testErrorMessage);
            expect(typeof errorMessage).toBe('string');
            expect(errorData.error).toBeInstanceOf(Error);
        });
    });
});

describe('task', () => {
    test('queryable', async () => {
        const db = createHistoryDb();
        await db.task(async (taskDb) => {
            expect(taskDb).toEqual(queryable);

            await taskDb.task((innerDb) => {
                expect(innerDb).toEqual(queryable);
            });

            await taskDb.tx((innerDb) => {
                expect(innerDb).toEqual(queryable);
            });
        });
    });
});

class CustomTransactionDatabase extends TransactionDatabase {}
class CustomTaskDatabase extends TaskDatabase {}

describe('customization', () => {
    test('transaction instance', async () => {
        const db = new Database({
            logger: fakeLogger,
            pool: createRowPool(0),
            transactionDatabaseClass: CustomTransactionDatabase
        });

        await db.tx(async (txDb) => {
            expect(txDb).toBeInstanceOf(CustomTransactionDatabase);

            await txDb.tx((innerTxDb) => {
                expect(innerTxDb).toBeInstanceOf(CustomTransactionDatabase);
            });
        });
    });

    test('task instance', async () => {
        const db = new Database({
            logger: fakeLogger,
            pool: createRowPool(0),
            taskDatabaseClass: CustomTaskDatabase
        });

        await db.task(async (taskDb) => {
            expect(taskDb).toBeInstanceOf(CustomTaskDatabase);

            await taskDb.task((innerTaskDb) => {
                expect(innerTaskDb).toBeInstanceOf(CustomTaskDatabase);
            });
        });

        await db.tx(async (txDb) => {
            expect(txDb).toBeInstanceOf(TransactionDatabase);

            await txDb.tx((innerTxDb) => {
                expect(innerTxDb).toBeInstanceOf(TransactionDatabase);
            });
        });
    });

    test('transaction and task instances', async () => {
        const db = new Database({
            logger: fakeLogger,
            pool: createRowPool(0),
            transactionDatabaseClass: CustomTransactionDatabase,
            taskDatabaseClass: CustomTaskDatabase
        });

        await db.task(async (taskDb) => {
            expect(taskDb).toBeInstanceOf(CustomTaskDatabase);

            await taskDb.task((innerTaskDb) => {
                expect(innerTaskDb).toBeInstanceOf(CustomTaskDatabase);
            });

            await taskDb.tx((innerTxDb) => {
                expect(innerTxDb).toBeInstanceOf(CustomTransactionDatabase);
            });
        });
    });
});

describe('pool', () => {
    test('client is queryable', async () => {
        let pgClient;
        const pool = new DatabasePool({
            pgPool: {
                connect() {
                    return {
                        release() {},
                        query() {}
                    };
                }
            }
        });
        await pool.use((client) => {
            pgClient = client;
        });

        expect(pgClient).toEqual(queryable);
    });

    test('release', async () => {
        let successCalled = false;
        let errorCalled = false;

        const pool = new DatabasePool({
            pgPool: {
                connect() {
                    return {
                        release(err) {
                            if (err) {
                                errorCalled = true;
                                return;
                            }
                            successCalled = true;
                        }
                    };
                }
            }
        });

        await pool.use(() => {});
        await expect(pool.use(() => {
            throw new Error(testErrorMessage);
        })).rejects.toThrow(testErrorMessage);

        expect(successCalled).toBe(true);
        expect(errorCalled).toBe(true);
    });
});
