import type { SlsAwsLambdaPlugin } from "serverless-aws-lambda/defineConfig";
import { Worker } from "worker_threads";
import { StreamsHandler } from "./streamhandler";
import { StreamFailure } from "./failure";
// @ts-ignore
import workerPath from "resolvedPaths";
import type { DynamoDBClientConfig } from "@aws-sdk/client-dynamodb";

let handler: StreamsHandler;
let worker: Worker;

export interface Config {
  waitBeforeInit?: number;
  watchInterval?: number;
}
const defaultOptions: Config = {
  waitBeforeInit: 25,
  watchInterval: 2,
};

export const dynamoStream = (clientConfig?: DynamoDBClientConfig | null, config: Config = defaultOptions): SlsAwsLambdaPlugin => {
  const dynamoOnReady: Function[] = [];

  const notifyReadyState = async () => {
    self.pluginData.isReady = true;

    for (const fn of dynamoOnReady) {
      try {
        await fn();
      } catch (error) {}
    }
  };

  const self: SlsAwsLambdaPlugin = {
    name: "ddblocal-stream",
    pluginData: {
      onReady: (cb: Function) => {
        if (typeof cb == "function") {
          dynamoOnReady.push(cb);
        } else {
          console.warn("onReady callback must be a function");
        }
      },
      isReady: false,
    },
    onInit: async function () {
      if (!this.isDeploying && !this.isPackaging) {
        const region = this.serverless.service.provider.region;
        if (region) {
          StreamFailure.REGION = region;
        }
        const mergedConfig: Config = { ...defaultOptions, ...config };

        handler = new StreamsHandler(this.serverless, this.lambdas);

        worker = new Worker(workerPath, {
          workerData: {
            waitBeforeInit: mergedConfig.waitBeforeInit,
            watchInterval: mergedConfig.watchInterval,
            tables: handler.listenableTables,
            clientConfig: clientConfig,
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

  return self;
};
