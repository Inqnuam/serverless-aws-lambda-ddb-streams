interface Config {
  TableName: string;
  StreamEnabled?: boolean;
  StreamViewType?: string;
  invoke: (event: any) => Promise<void>;
  batchSize: number;
  functionResponseType?: string;
  filterPatterns?: any[];
}
export class Subscriber {
  TableName: string;
  StreamEnabled?: boolean;
  StreamViewType?: string;
  invoke: (event: any) => Promise<void>;
  batchSize: number;
  onFailure?: string;
  functionResponseType?: string;
  filterPatterns?: any[];
  #records: any[] = [];

  static #numericCompare = (
    operator: string,
    value: number,
    compareTo: number
  ): boolean => {
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
        throw new Error(
          "stream filter 'exists' value must be 'true' or 'false'"
        );
      }
    },
    prefix: (record: any, key: string, operatorValue: any) => {
      if (typeof operatorValue !== "string") {
        throw new Error("stream filter 'prefix' value must be typeof 'string'");
      }

      const val =
        record[key]?.S ?? typeof record[key] == "string"
          ? record[key]
          : undefined;

      if (val) {
        return val.startsWith(operatorValue);
      }
      return false;
    },
    numeric: (record: any, key: string, operatorValue: any) => {
      if (
        !Array.isArray(operatorValue) ||
        ![2, 4].includes(operatorValue.length)
      ) {
        throw new Error(
          "stream filter 'numeric' value must an array with 2 or 4 items"
        );
      }

      if (!(key in record)) {
        return false;
      }

      const andResult: boolean[] = [];
      const [comparator, value] = operatorValue;
      andResult.push(
        Subscriber.#numericCompare(comparator, record[key], value)
      );

      if (operatorValue.length == 4) {
        const [, , comparator, value] = operatorValue;
        andResult.push(
          Subscriber.#numericCompare(comparator, record[key], value)
        );
      }

      return andResult.every((x) => x === true);
    },
    "anything-but": (record: any, key: string, operatorValue: any) => {
      if (
        !Array.isArray(operatorValue) ||
        !operatorValue.every((x) => typeof x == "string")
      ) {
        throw new Error(
          "stream filter 'anything-but' value must an array of string"
        );
      }
      const val =
        record[key]?.S ?? typeof record[key] == "string"
          ? record[key]
          : undefined;
      if (val) {
        return !operatorValue.includes(val);
      }

      return false;
    },
  };
  constructor({
    TableName,
    StreamEnabled,
    StreamViewType,
    invoke,
    batchSize,
    functionResponseType,
    filterPatterns,
  }: Config) {
    this.TableName = TableName;
    this.StreamEnabled = StreamEnabled;
    this.StreamViewType = StreamViewType;
    this.invoke = invoke;
    this.batchSize = batchSize;
    this.functionResponseType = functionResponseType;
    this.filterPatterns = filterPatterns;
  }

  static #filter = (record: any, key: string, operator: any) => {
    const opType = typeof operator;
    if (opType == "string" || opType === null) {
      return record[key] == operator;
    } else if (opType == "object" && !Array.isArray(operator)) {
      const andConditions: boolean[] = [];

      for (const [opName, opValue] of Object.entries(operator)) {
        if (opName in Subscriber.#expressionOperators) {
          andConditions.push(
            Subscriber.#expressionOperators[opName](record, key, opValue)
          );
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
        childFilterResult = operator.map((x) =>
          Subscriber.#filter(record, key, x)
        );
      } else if (record[key]) {
        childFilterResult = [Subscriber.#filterObject(operator, record[key])];
      }

      filterResult.push(childFilterResult.some((x) => x === true));
    }

    return filterResult.every((x) => x === true);
  };
  #filterRecords = (records: any[]) => {
    if (Array.isArray(this.filterPatterns)) {
      return records.filter((x) => {
        const filterResult = this.filterPatterns!.map((p) =>
          Subscriber.#filterObject(p, x)
        );
        return filterResult.some((x) => x === true);
      });
    } else {
      return records;
    }
  };
  setRecords = async (records: any[]) => {
    for (const record of this.#filterRecords(records)) {
      this.#records.push(record);
      if (this.#records.length == this.batchSize) {
        try {
          await this.invoke({ Records: this.#records });
        } catch (error) {
          console.log(error);
        }

        this.#records = [];
      }
    }
  };
}
