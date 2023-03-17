import EventEmitter from "events";
import { StreamFailure } from "./failure";

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
  functionResponseType?: string;
  filterPatterns?: any[];
  onFailure?: {
    kind: "sns" | "sqs";
    name: string;
  };
}

type Invoke = (event: any, info?: any) => Promise<void>;

export class Subscriber extends EventEmitter {
  lambdaName: string;
  TableName: string;
  StreamEnabled?: boolean;
  StreamViewType?: string;
  invoke: Invoke;
  batchSize: number;
  batchWindow: number;
  maximumRetryAttempts?: number;
  maximumRecordAgeInSeconds?: number;
  bisectBatchOnFunctionError?: boolean;
  onFailure?: {
    kind: "sns" | "sqs";
    name: string;
  };
  functionResponseType?: string;
  filterPatterns?: any[];
  #records: any[] = [];
  failure?: StreamFailure;
  event: any;
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

    let tm = setTimeout(() => {}, 0);
    this.on("invoke", (DDBStreamBatchInfo: any) => {
      clearTimeout(tm);

      tm = setTimeout(async () => {
        await this.prepareInvoke(DDBStreamBatchInfo);
      }, this.batchWindow);
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
      this.#records.push(record);
      this.#expireRecord(record.eventID, DDBStreamBatchInfo);
      this.emit("invoke", DDBStreamBatchInfo);
    }
  };

  async prepareInvoke(DDBStreamBatchInfo: any) {
    if (!this.#records.length) {
      return;
    }

    const batches = this.#createBatch(this.#records.slice(), this.batchSize);

    console.log(`\x1b[35mDynamoDB Stream: ${this.TableName}\x1b[0m`);
    for (const Records of batches) {
      await this.callLambda(Records, DDBStreamBatchInfo);
    }
  }
  async callLambda(Records: any[], DDBStreamBatchInfo: any) {
    try {
      await this.invoke({ Records }, { kind: "ddb", event: this.event });
      Records.forEach((e: any) => this.#clearRecord(e.eventID));
    } catch (error) {
      await this.#handleInvokeError(Records, DDBStreamBatchInfo);
    }
  }
  async #handleInvokeError(Records: any, DDBStreamBatchInfo: any) {
    if (this.maximumRetryAttempts) {
      return this.#retry(Records, DDBStreamBatchInfo);
    }

    if (this.maximumRecordAgeInSeconds) {
      this.#retryUntilTimeout(Records);
    }
  }
  async #retryUntilTimeout(Records: any[]) {
    Records.forEach(async (record) => {
      while (this.#records.find((x) => x.eventID == record.eventID)) {
        try {
          await this.invoke({ Records }, { kind: "ddb", event: this.event });
          this.#clearRecord(record.eventID);
        } catch (error) {}

        await sleep(0.6);
      }
    });
  }
  async #retry(Records: any, DDBStreamBatchInfo: any) {
    let batches = [Records];
    if (this.bisectBatchOnFunctionError) {
      const batchSize = Math.ceil(Records.length / 2);
      batches = this.#createBatch(Records, batchSize);
    }

    let totalRetry = 0;
    retries: while (totalRetry <= this.maximumRetryAttempts!) {
      totalRetry++;

      const failedBatches = [];
      for (const Records of batches) {
        try {
          await this.invoke({ Records }, { kind: "ddb", event: this.event });
          Records.forEach((e: any) => this.#clearRecord(e.eventID));
        } catch (error) {
          failedBatches.push(Records);
        }
      }

      batches = failedBatches;

      if (!batches.length) {
        break retries;
      }
    }
    if (this.failure && batches.length && !this.maximumRecordAgeInSeconds) {
      this.failure.unhandled(DDBStreamBatchInfo, totalRetry);
    }
  }
  #clearRecord(eventID: string) {
    const foundIndex = this.#records.findIndex((x) => x.eventID == eventID);

    if (foundIndex != -1) {
      this.#records.splice(foundIndex, 1);
      return true;
    }
  }
  #expireRecord(eventID: string, DDBStreamBatchInfo: any) {
    if (!this.maximumRecordAgeInSeconds) {
      return;
    }
    setTimeout(async () => {
      if (this.#clearRecord(eventID) && this.failure) {
        this.failure.timeout(DDBStreamBatchInfo);
      }
    }, this.maximumRecordAgeInSeconds);
  }
  #createBatch(records: any, batchSize: number) {
    const batches = [];

    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      batches.push(batch);
    }

    return batches;
  }
}
