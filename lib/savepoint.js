const crypto = require('crypto');

const savepointNameSize = 8;
const savepointNameBufferSize = 2048;
const savepointNameBuffer = Buffer.alloc(savepointNameBufferSize);
let savepointNameOffset = 0;

/**
 * @returns {string}
 */
function createSavepointName() {
    if (savepointNameOffset === 0) {
        crypto.randomFillSync(savepointNameBuffer);
    }

    const savepointName = savepointNameBuffer.toString(
        'hex',
        savepointNameOffset,
        savepointNameOffset + savepointNameSize
    );

    savepointNameOffset += savepointNameSize;
    if (savepointNameOffset === savepointNameBufferSize) {
        savepointNameOffset = 0;
    }

    return savepointName;
}

module.exports = {
    createSavepointName
};