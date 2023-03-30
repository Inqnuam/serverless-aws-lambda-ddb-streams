import EventEmitter from "events";
import { StreamFailure, getBatchItemFailures } from "./failure";
import { Batch } from "./batch";
import { TumblingWindow } from "./tumbling";

const sleep = (secondes: number) => new Promise((resolve) => setTimeout(resolve, secondes * 1000));

interface Config {
  TableName: string;
  StreamEnabled?: boolean;
  StreamViewType?: string;
  batchSize: number;
  batchWindow?: number;
  maximumRetryAttempts?: number;
  maximumRecordAgeInSeconds?: number;
  bisectBatchOnFunctionError?: boolean;
  tumblingWindowInSeconds?: number;
  functionResponseType?: string;
  filterPatterns?: any[];
  onFailure?: {
    kind: "sns" | "sqs";
    name: string;
  };
}

type Invoke = (event: any, info?: any) => Promise<void | any>;
const ONE_DAY = 86400 * 1000;

export class Subscriber extends EventEmitter {
  lambdaName: string;
  TableName: string;
  StreamEnabled?: boolean;
  StreamViewType?: string;
  invoke: Invoke;
  batchSize: number;
  batchWindow: number;
  maximumRetryAttempts?: number;
  maximumRecordAgeInSeconds: number = ONE_DAY;
  bisectBatchOnFunctionError?: boolean;
  tumblingWindowInSeconds?: number;
  onFailure?: {
    kind: "sns" | "sqs";
    name: string;
  };
  functionResponseType?: string;
  filterPatterns?: any[];
  failure?: StreamFailure;
  event: any;
  #batches: Batch[] = [];
  #tumblings: TumblingWindow[] = [];
  constructor(event: Config, invoke: Invoke, name: string) {
    super();
    this.event = event;
    this.lambdaName = name;
    this.TableName = event.TableName;
    this.StreamEnabled = event.StreamEnabled;
    this.StreamViewType = event.StreamViewType;
    this.invoke = invoke;
    this.batchSize = event.batchSize;
    this.functionResponseType = event.functionResponseType;
    this.filterPatterns = event.filterPatterns;
    this.bisectBatchOnFunctionError = event.bisectBatchOnFunctionError;
    this.tumblingWindowInSeconds = event.tumblingWindowInSeconds;
    this.maximumRetryAttempts = event.maximumRetryAttempts;

    if (event.maximumRecordAgeInSeconds) {
      this.maximumRecordAgeInSeconds = event.maximumRecordAgeInSeconds * 1000;
    }

    if (event.onFailure) {
      this.failure = new StreamFailure(event.onFailure, name);
    }

    if (!event.batchWindow || isNaN(event.batchWindow) || event.batchWindow == -1) {
      this.batchWindow = 0;
    } else {
      this.batchWindow = event.batchWindow * 1000;
    }

    this.on("invoke", (record: any, DDBStreamBatchInfo: any) => {
      const foundBatch = this.#batches.find((x) => !x.closed);

      const setRecord = foundBatch?.setRecord(record, DDBStreamBatchInfo);
      if (!foundBatch || !setRecord) {
        const batch = new Batch({
          batchSize: this.batchSize,
          batchWindow: this.batchWindow,
          maximumRecordAgeInSeconds: this.maximumRecordAgeInSeconds,
          tumbling: this.#getTumbling(DDBStreamBatchInfo),
          onComplete: async (batch: Batch, isFinalInvokeForWindow: boolean) => {
            console.log(`\x1b[35mDynamoDB Stream: ${this.TableName} > ${this.lambdaName}\x1b[0m`);

            const { error } = await this.callLambda(batch);

            if (error) {
              await this.#handleInvokeError(batch, DDBStreamBatchInfo);
            }
            if (isFinalInvokeForWindow) {
              const foundIndex = this.#batches.findIndex((x) => x == batch);
              if (foundIndex != -1) {
                const foundTumblingIndex = this.#tumblings.findIndex((x) => x == this.#batches[foundIndex].tumbling);
                if (foundTumblingIndex != -1) {
                  this.#tumblings.splice(foundTumblingIndex, 1);
                }
                this.#batches.splice(foundIndex, 1);
              }
            }
          },
          onRecordExpire: this.failure
            ? () => {
                this.failure!.timeout(DDBStreamBatchInfo);
              }
            : undefined,
        });

        this.#batches.push(batch);
        batch.setRecord(record, DDBStreamBatchInfo);
      }
    });
  }
  static #numericCompare = (operator: string, value: number, compareTo: number): boolean => {
    switch (operator) {
      case "=":
        return value == compareTo;
      case ">":
        return value > compareTo;
      case "<":
        return value < compareTo;
      case ">=":
        return value >= compareTo;
      case "<=":
        return value >= compareTo;
      default:
        return false;
    }
  };
  static #expressionOperators: {
    [key: string]: (record: any, key: string, operatorValue: any) => boolean;
  } = {
    exists: (record: any, key: string, operatorValue: any) => {
      if (operatorValue === true) {
        return key in record;
      } else if (operatorValue === false) {
        return !(key in record);
      } else {
        throw new Error("stream filter 'exists' value must be 'true' or 'false'");
      }
    },
    prefix: (record: any, key: string, operatorValue: any) => {
      if (typeof operatorValue !== "string") {
        throw new Error("stream filter 'prefix' value must be typeof 'string'");
      }

      const val = record[key]?.S ?? typeof record[key] == "string" ? record[key] : undefined;

      if (val) {
        return val.startsWith(operatorValue);
      }
      return false;
    },
    numeric: (record: any, key: string, operatorValue: any) => {
      if (!Array.isArray(operatorValue) || ![2, 4].includes(operatorValue.length)) {
        throw new Error("stream filter 'numeric' value must an array with 2 or 4 items");
      }

      if (!(key in record)) {
        return false;
      }

      const andResult: boolean[] = [];
      const [comparator, value] = operatorValue;
      andResult.push(Subscriber.#numericCompare(comparator, record[key], value));

      if (operatorValue.length == 4) {
        const [, , comparator, value] = operatorValue;
        andResult.push(Subscriber.#numericCompare(comparator, record[key], value));
      }

      return andResult.every((x) => x === true);
    },
    "anything-but": (record: any, key: string, operatorValue: any) => {
      if (!Array.isArray(operatorValue) || !operatorValue.every((x) => typeof x == "string")) {
        throw new Error("stream filter 'anything-but' value must an array of string");
      }
      const val = record[key]?.S ?? typeof record[key] == "string" ? record[key] : undefined;
      if (val) {
        return !operatorValue.includes(val);
      }

      return false;
    },
  };

  static #filter = (record: any, key: string, operator: any) => {
    const opType = typeof operator;
    if (opType == "string" || opType === null) {
      return record[key] == operator;
    } else if (opType == "object" && !Array.isArray(operator)) {
      const andConditions: boolean[] = [];

      for (const [opName, opValue] of Object.entries(operator)) {
        if (opName in Subscriber.#expressionOperators) {
          andConditions.push(Subscriber.#expressionOperators[opName](record, key, opValue));
        }
      }
      return andConditions.every((x) => x === true);
    }

    return false;
  };

  static #filterObject = (pattern: any, record: any) => {
    const filterResult: boolean[] = [];

    for (const [key, operator] of Object.entries(pattern)) {
      let childFilterResult: boolean[] = [];

      if (Array.isArray(operator)) {
        childFilterResult = operator.map((x) => Subscriber.#filter(record, key, x));
      } else if (record[key]) {
        childFilterResult = [Subscriber.#filterObject(operator, record[key])];
      }

      filterResult.push(childFilterResult.some((x) => x === true));
    }

    return filterResult.every((x) => x === true);
  };
  #filterRecords = (records: any[]) => {
    if (Array.isArray(this.filterPatterns)) {
      return records.map((x) => {
        const filterResult = this.filterPatterns!.map((p) => Subscriber.#filterObject(p, x));

        const foundIndex = filterResult.findIndex((x) => x === true);
        if (foundIndex != -1) {
          return x;
        } else {
          return null;
        }
      });
    } else {
      return records;
    }
  };
  setRecords = async (records: any[], DDBStreamBatchInfo: any) => {
    const recs = this.#filterRecords(records).filter((x: any) => x);

    for (const record of recs) {
      this.emit("invoke", record, DDBStreamBatchInfo);
    }
  };

  async callLambda(batch: Batch, Records?: any[]) {
    let res: any = {};

    try {
      const response = await this.invoke(batch.getStreamEvent(Records), { kind: "ddb", event: this.event });

      if (this.tumblingWindowInSeconds) {
        batch.tumbling!.setState(response.state);
      }

      if (this.functionResponseType == "ReportBatchItemFailures") {
        const responseItems = getBatchItemFailures(batch.records, response);

        if (responseItems) {
          const { success, failures } = responseItems;
          success.forEach((x) => batch.removeRecord(x));

          if (failures.length) {
            res.error = new Error("has failed items");
          }
        } else {
          batch.records.forEach((e: any) => batch.removeRecord(e.eventID));
        }
      } else {
        batch.records.forEach((e: any) => batch.removeRecord(e.eventID));
      }
    } catch (error) {
      res.error = error;
    }
    return res;
  }
  async #handleInvokeError(batch: Batch, DDBStreamBatchInfo: any) {
    if (this.maximumRetryAttempts) {
      return this.#retry(batch, DDBStreamBatchInfo);
    }
    this.#retryUntilTimeout(batch);
  }
  async #retryUntilTimeout(batch: Batch) {
    while (batch.records.length) {
      console.log(`\x1b[35mDynamoDB Stream retry until record expires!: ${this.TableName} > ${this.lambdaName}\x1b[0m`);
      await this.callLambda(batch);
      await sleep(0.6);
    }
  }
  async #retry(batch: Batch, DDBStreamBatchInfo: any) {
    let totalRetry = 0;
    retries: while (totalRetry <= this.maximumRetryAttempts! - 1) {
      totalRetry++;
      let batches = [batch.records];
      if (this.bisectBatchOnFunctionError) {
        const batchSize = Math.ceil(batch.records.length / 2);
        batches = this.#createBatch(batch.records, batchSize);
      }

      console.log(`\x1b[35mDynamoDB Stream retry n°${totalRetry}: ${this.TableName} > ${this.lambdaName}\x1b[0m`);
      for (const Records of batches) {
        await this.callLambda(batch, Records);
      }

      if (!batch.records.length) {
        break retries;
      }
      await sleep(0.6);
    }
    if (this.failure && batch.records.length) {
      batch.records.forEach((e: any) => batch.removeRecord(e.eventID));
      this.failure.unhandled(DDBStreamBatchInfo, totalRetry);
    }
  }

  #createBatch(records: any, batchSize: number) {
    const batches = [];

    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      batches.push(batch);
    }

    return batches;
  }

  #getTumbling = ({ shardId, streamArn }: any) => {
    if (this.tumblingWindowInSeconds) {
      const foundTumbling = this.#tumblings.find((x) => !x.isFinalInvokeForWindow && x.shardId == shardId);
      if (foundTumbling) {
        return foundTumbling;
      } else {
        const tumbling = new TumblingWindow({ shardId, eventSourceARN: streamArn, tumblingWindowInSeconds: this.tumblingWindowInSeconds });
        this.#tumblings.push(tumbling);
        return tumbling;
      }
    }
  };
}
