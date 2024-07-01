import { parentPort, workerData } from "worker_threads";
import { DynamoStream } from "./stream";

const { waitBeforeInit, watchInterval, tables, clientConfig } = workerData;

DynamoStream.maxWaitTime = waitBeforeInit;
DynamoStream.watchInterval = watchInterval;
if (clientConfig) {
  DynamoStream.clientConfig = clientConfig;
}

parentPort!.on("message", async (e) => {
  const { channel } = e;

  if (channel == "init") {
    await Promise.all(
      tables.map(async (table: any) => {
        try {
          const dynamoStream = new DynamoStream(table);
          await dynamoStream.init();

          dynamoStream.on("records", (records, DDBStreamBatchInfo) => {
            parentPort!.postMessage({ records, DDBStreamBatchInfo, TableName: dynamoStream.TableName });
          });
        } catch (error) {
          console.log(error);
        }
      })
    );

    parentPort!.postMessage({ channel: "ready" });
  }
});
