import type { SlsAwsLambdaPlugin } from "serverless-aws-lambda/defineConfig";
import { Worker } from "worker_threads";
import path from "path";
import { StreamsHandler } from "./streamhandler";

let handler: StreamsHandler;
let worker: Worker;
const workerPath = path.resolve(__dirname, "./worker.js");

export interface Config {
  endpoint?: string;
  region?: string;
  waitBeforeInit?: number;
  watchInterval?: number;
}
const defaultOptions: Config = {
  endpoint: "http://localhost:8000",
  region: "eu-west-1",
  waitBeforeInit: 25,
  watchInterval: 2,
};

export const dynamoStream = (
  config: Config = defaultOptions
): SlsAwsLambdaPlugin => {
  const mergedConfig: Config = { ...defaultOptions, ...config };
  return {
    name: "ddblocal-stream",
    onInit: async function () {
      if (!this.isDeploying && !this.isPackaging) {
        handler = new StreamsHandler(this.serverless, this.lambdas);

        worker = new Worker(workerPath, {
          workerData: {
            endpoint: mergedConfig.endpoint,
            waitBeforeInit: mergedConfig.waitBeforeInit,
            watchInterval: mergedConfig.watchInterval,
            region: mergedConfig.region,
            tables: handler.listenableTables,
          },
        });
        worker.on("message", handler.setRecords);
      }
    },
    offline: {
      onReady: async function () {
        worker?.postMessage({ channel: "init" });
      },
    },
  };
};
