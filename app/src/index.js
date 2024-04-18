import 'dotenv/config';
import {createReadStream} from 'fs';
import {pipeline, Writable} from 'stream';
import {parse} from '@fast-csv/parse';
import {createConnection} from 'mysql';

const config = {
    host: process.env.DB_HOST,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
}

const connection = createConnection(config);

const dbConnect = async () => new Promise((resolve, reject) => {
    connection.connect((error) => {
        if (error) {
            reject(error);
        }
        console.log('Conexão bem-sucedida ao banco de dados MySQL.');
        resolve();
    });
});


const pipefyStreams = async (...args) => {
    return new Promise((resolve, reject) => {
        pipeline(...args, (error) => {
            error ? reject(error) : resolve();
        });
    });
}

const readStreamCSV = (filename) => {
    const readStream = createReadStream(filename);
    readStream.on('error', (error) => {
        throw new Error(`Erro ao ler o arquivo CSV: ${error.message}`);
    });
    return readStream;
}

const transformer = parse({
    delimiter: ';',
    headers: true,
    encoding: 'utf8',
});

const processWrite = () => {
    let batchData = [];
    const batchSize = 5;
    const processDatabaseBatch = async (data = []) => {
        try {
            const values = data.map((item) => [item.NAME, item.CITY, item.STATE]);
            const query = 'INSERT INTO PERSON (NAME, CITY, STATE) VALUES ?';
            await insertBatchTransactional(query, values);
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


const insertBatchTransactional = (query, values) => new Promise((resolve, reject) => {
    connection.beginTransaction((error) => {
        if (error) {
            reject(error);
            return;
        }
        connection.query(query, [values], (error) => {
            if (error) {
                connection.rollback(() => {
                    reject(error);
                });
            } else {
                connection.commit((error) => {
                    if (error) {
                        connection.rollback(() => {
                            reject(error);
                        });
                    } else {
                        resolve();
                    }
                });
            }
        });
    });
});


const run = async () => {
    try {
        await dbConnect();
        await pipefyStreams(readStreamCSV('file.csv'), transformer, processWrite());
    } catch (error) {
        console.error("Ocorreu um erro durante a execução:", error);
        process.exit(1);
    } finally {
        console.log('Fim da execução!');
        process.exit(0);
    }
}

run().then();