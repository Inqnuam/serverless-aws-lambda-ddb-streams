import type { SlsAwsLambdaPlugin } from "serverless-aws-lambda/defineConfig";
import { Worker } from "worker_threads";
import { StreamsHandler } from "./streamhandler";
import { StreamFailure } from "./failure";
// @ts-ignore
import workerPath from "resolvedPaths";

let handler: StreamsHandler;
let worker: Worker;

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
  const dynamoOnReady: Function[] = [];

  const notifyReadyState = async () => {
    for (const fn of dynamoOnReady) {
      try {
        await fn();
      } catch (error) {}
    }
  };
  return {
    name: "ddblocal-stream",
    pluginData: {
      onReady: (cb: Function) => {
        if (typeof cb == "function") {
          dynamoOnReady.push(cb);
        } else {
          console.warn("onReady callback must be a function");
        }
      },
    },
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
        worker.on("message", async (msg) => {
          if (msg.channel == "ready") {
            await notifyReadyState();
          } else {
            handler.setRecords(msg);
          }
        });
      }
    },
    onExit: () => {
      worker?.terminate();
    },
    offline: {
      onReady: async function (port) {
        StreamFailure.LOCAL_PORT = port;
        worker?.postMessage({ channel: "init" });
      },
    },
  };
};
