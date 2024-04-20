import 'dotenv/config';
import {DatabaseManager} from './services/DatabaseManager.js';
import {DataProcessor} from "./services/DataProcessor.js";


const run = async () => {
    console.log('Start of execution.');
    const startTime = process.hrtime();
    try {

        const configOptions = {
            host: process.env.DB_HOST,
            user: process.env.DB_USERNAME,
            password: process.env.DB_PASSWORD,
            database: process.env.DB_DATABASE,
        };
        const databaseManager = new DatabaseManager(configOptions);
        const dataProcessor = new DataProcessor(databaseManager);
        await dataProcessor.bulkInsert('file.csv', 5000);
    } catch (error) {
        console.error("An error occurred during execution:", error);
        process.exit(1);
    } finally {
        const endTime = process.hrtime(startTime);
        const duration = (endTime[0] * 1000 + endTime[1] / 1000000).toFixed(2);
        console.log(`End of execution with: ${duration}ms`);
        process.exit(0);
    }
}

run().then();