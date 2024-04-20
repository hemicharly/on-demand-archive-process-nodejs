import {createReadStream, statSync} from 'fs';
import {pipeline, Writable} from 'stream';
import {parse} from '@fast-csv/parse';

export class DataProcessor {
    #databaseManager;
    #filename = 0;
    #bytesProcessed = 0;
    #fileSize = 0;

    constructor(databaseManager, filename) {
        this.#databaseManager = databaseManager;
        this.#filename = filename;
        this.#bytesProcessed = 0;
        this.#fileSize = statSync(filename).size;
    }

    #formatFileSize(sizeInBytes) {
        const KB = 1024;
        const MB = 1024 * KB;
        const GB = 1024 * MB;

        if (sizeInBytes < KB) {
            return `${sizeInBytes} B`;
        }
        if (sizeInBytes < MB) {
            return `${(sizeInBytes / KB).toFixed(2)} KB`;
        }
        if (sizeInBytes < GB) {
            return `${(sizeInBytes / MB).toFixed(2)} MB`;
        }
        return `${(sizeInBytes / GB).toFixed(2)} GB`;
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
        readStream.on('data', (chunk) => {
            this.#bytesProcessed += chunk.length;
            const progress = ((this.#bytesProcessed / this.#fileSize) * 100).toFixed(2);
            console.log(`The file has been read ${this.#formatFileSize(this.#bytesProcessed)} of ${this.#formatFileSize(this.#fileSize)} (${progress}%)`);
        });
        return readStream;
    }

    #processWrite(batchSize) {
        let count = 0;
        let batchData = [];
        const processDatabaseBatch = async (data = []) => {
            try {
                await this.#databaseManager.insertBatchTransactional(data);
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
                        count += data.length;
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
                        count += data.length;
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

    async bulkInsert(batchSize = 1000, parseCsvOptions = {
        delimiter: ';',
        headers: true,
        encoding: 'utf8',
    }) {
        await this.#databaseManager.connect();
        await this.#pipefyStreams(this.#readStreamCSV(this.#filename), parse(parseCsvOptions), this.#processWrite(batchSize));
    }
}
