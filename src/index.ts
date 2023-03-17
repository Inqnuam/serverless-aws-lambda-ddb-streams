import type { SlsAwsLambdaPlugin } from "serverless-aws-lambda/defineConfig";
import { Worker } from "worker_threads";
import path from "path";
import { StreamsHandler } from "./streamhandler";
import { StreamFailure } from "./failure";

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
  region: "us-east-1",
  waitBeforeInit: 25,
  watchInterval: 2,
};

export const dynamoStream = (config: Config = defaultOptions): SlsAwsLambdaPlugin => {
  return {
    name: "ddblocal-stream",
    onInit: async function () {
      if (!this.isDeploying && !this.isPackaging) {
        const region = this.serverless.service.provider.region;
        if (region) {
          StreamFailure.REGION = region;
        }
        const mergedConfig: Config = { ...defaultOptions, region, ...config };

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
      onReady: async function (port) {
        StreamFailure.LOCAL_PORT = port;
        worker?.postMessage({ channel: "init" });
      },
    },
  };
};
