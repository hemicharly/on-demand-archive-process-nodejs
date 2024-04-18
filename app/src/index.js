import {createReadStream} from 'fs';
import {pipeline, Writable} from 'stream';
import {parse} from '@fast-csv/parse';


const pipefyStreams = async (...args) => {
    return new Promise((resolve, reject) => {
        pipeline(...args, (error) => {
            error ? reject(error) : resolve();
        });
    });
}

const transformer = parse({
    delimiter: ',',
    headers: true,
    encoding: 'utf8',
});

const processWrite = () => {
    let batchData = [];
    const batchSize = 5;

    const processDatabaseBatch = async (data = []) => {
        try {
            /*TODO: Logica para persistir no banco de dados*/
            console.log("Inserindo lote no banco de dados:", data.length);
        } catch (error) {
            throw new Error(`Erro ao inserir lote no banco de dados: ${error.message}`);
        }
    };

    return new Writable({
        objectMode: true,
        write: async (chunk, encoding, callback) => {
            batchData.push(chunk);
            if (batchData.length >= batchSize) {
                try {
                    await processDatabaseBatch(batchData.slice());
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
                    await processDatabaseBatch(batchData.slice());
                    callback();
                } catch (error) {
                    callback(error);
                }
            } else {
                callback();
            }
        }
    });
}

const run = async () => {
    try {
        const readStreamCSV = createReadStream('file.csv');
        await pipefyStreams(readStreamCSV, transformer, processWrite());
    } catch (e) {
        console.error(e);
    }
}

run();