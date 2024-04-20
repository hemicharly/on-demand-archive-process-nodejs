import {createReadStream} from 'fs';
import {pipeline, Writable} from 'stream';
import {parse} from '@fast-csv/parse';

export class DataProcessor {
    #databaseManager;

    constructor(databaseManager) {
        this.#databaseManager = databaseManager;
    }

    async #pipefyStreams(...args) {
        return new Promise((resolve, reject) => {
            pipeline(...args, (error) => {
                error ? reject(error) : resolve();
            });
        });
    }

    #readStreamCSV(filename) {
        const readStream = createReadStream(filename);
        readStream.on('error', (error) => {
            throw new Error(`Error reading CSV file: ${error.message}`);
        });
        return readStream;
    }

    #processWrite(batchSize) {
        let count = 0;
        let batchData = [];
        const processDatabaseBatch = async (data = []) => {
            try {
                await this.#databaseManager.insertBatchTransactional(data);
                console.log(`Inserting batch into the database with ${data.length} records`);
            } catch (error) {
                throw new Error(`Error when inserting batch into database: ${error.message}`);
            }
        };

        return new Writable({
            objectMode: true,
            write: async (chunk, encoding, callback) => {
                batchData.push(chunk);
                if (batchData.length >= batchSize) {
                    try {
                        const data = batchData.slice();
                        await processDatabaseBatch(data);
                        count = count + data.length;
                        batchData = [];
                        callback();
                    } catch (error) {
                        callback(error);
                    }
                } else {
                    callback();
                }
            },
            final: async (callback) => {
                if (batchData.length > 0) {
                    try {
                        const data = batchData.slice();
                        await processDatabaseBatch(data);
                        count = count + data.length;
                        callback();
                    } catch (error) {
                        callback(error);
                    }
                } else {
                    callback();
                }
                console.log(`${count} lines were processed!`);
            }
        });
    }

    async bulkInsert(filename, batchSize = 1000, parseCsvOptions = {
        delimiter: ';',
        headers: true,
        encoding: 'utf8',
    }) {
        await this.#databaseManager.connect();
        await this.#pipefyStreams(this.#readStreamCSV(filename), parse(parseCsvOptions), this.#processWrite(batchSize));
    }
}
