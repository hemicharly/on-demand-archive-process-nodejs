import 'dotenv/config';
import {DatabaseManager} from './services/DatabaseManager.js';
import {DataProcessor} from "./services/DataProcessor.js";


const run = async () => {
    try {
        console.log('Start of execution!');
        const configOptions = {
            host: process.env.DB_HOST,
            user: process.env.DB_USERNAME,
            password: process.env.DB_PASSWORD,
            database: process.env.DB_DATABASE,
        };
        const databaseManager = new DatabaseManager(configOptions);
        const dataProcessor = new DataProcessor(databaseManager);
        await dataProcessor.bulkInsert('file.csv', 3);
    } catch (error) {
        console.error("An error occurred during execution:", error);
        process.exit(1);
    } finally {
        console.log('End of execution!');
        process.exit(0);
    }
}

run().then();