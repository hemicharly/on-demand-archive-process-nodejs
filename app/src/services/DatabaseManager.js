import {createConnection} from 'mysql';

export class DatabaseManager {
    #connection;

    constructor(configOptions = {
        host: '',
        user: '',
        password: '',
        database: ''
    }) {
        this.#connection = createConnection(configOptions);
    }

    async connect() {
        return new Promise((resolve, reject) => {
            this.#connection.connect((error) => {
                if (error) {
                    reject(error);
                }
                console.log('Successful connection to MySQL database.');
                resolve();
            });
        });
    }

    async insertBatchTransactional(data) {
        const values = data.map((item) => [item.NAME, item.CITY, item.STATE]);
        const query = 'INSERT INTO PERSON (NAME, CITY, STATE) VALUES ?';

        return new Promise((resolve, reject) => {
            this.#connection.beginTransaction((error) => {
                if (error) {
                    reject(error);
                    return;
                }
                this.#connection.query(query, [values], (error) => {
                    if (error) {
                        this.#connection.rollback(() => {
                            reject(error);
                        });
                    } else {
                        this.#connection.commit((error) => {
                            if (error) {
                                this.#connection.rollback(() => {
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
    }
}
