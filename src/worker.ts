import { parentPort, workerData } from "worker_threads";
import { DynamoStream } from "./stream";

const listeningTables: DynamoStream[] = [];

const { endpoint, waitBeforeInit, watchInterval, region, tables } = workerData;

DynamoStream.maxWaitTime = waitBeforeInit;
DynamoStream.endpoint = endpoint;
DynamoStream.watchInterval = watchInterval;
DynamoStream.region = region;
parentPort!.on("message", async (e) => {
  const { channel } = e;

  if (channel == "init") {
    tables.forEach((table: any) => {
      const dynamoStream = new DynamoStream(table);

      dynamoStream.init();

      dynamoStream.on("records", (records, DDBStreamBatchInfo) => {
        parentPort!.postMessage({ records, DDBStreamBatchInfo, TableName: dynamoStream.TableName });
      });
      listeningTables.push(dynamoStream);
    });
  }
});
