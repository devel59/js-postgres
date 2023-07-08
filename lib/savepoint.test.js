const { createSavepointName } = require('./savepoint');

describe('savepoint name', () => {
    test('length is 17', () => {
        expect(createSavepointName().length).toBe(17);
    });

    test('first character is valid', () => {
        const validCharRe = /^[a-zA-Z_]/;

        const isValid = new Array(1000).fill().map(() => {
            return createSavepointName()[0];
        }).every((char) => {
            return validCharRe.test(char);
        });

        expect(isValid).toBe(true);
    });

    test('3 names are unique', () => {
        const nameA = createSavepointName();
        const nameB = createSavepointName();
        const nameC = createSavepointName();

        expect(nameA).not.toBe(nameB);
        expect(nameB).not.toBe(nameC);
        expect(nameC).not.toBe(nameA);
    });

    test('1000 names are unique', () => {
        const names = new Array(1000).fill().map(() => {
            return createSavepointName();
        });
        const uniqueNames = new Set(names);

        expect(names.length).toBe(uniqueNames.size);
    });
});
