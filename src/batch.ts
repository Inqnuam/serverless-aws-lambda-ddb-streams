import type { TumblingWindow } from "./tumbling";

interface IBatchConfig {
  batchSize: number;
  batchWindow: number;
  maximumRecordAgeInSeconds: number;
  onComplete: (batch: Batch, isFinalInvokeForWindow: boolean) => void;
  onRecordExpire?: (DDBStreamBatchInfo: any) => void;
  tumbling?: TumblingWindow;
}

export class Batch {
  #batchSize: number;
  #batchWindow: number;
  maximumRecordAgeInSeconds: number;
  onComplete: (batch: Batch, isFinalInvokeForWindow: boolean) => void;
  onRecordExpire?: (DDBStreamBatchInfo: any) => void;
  records: any[] = [];
  closed: boolean = false;
  tumbling?: TumblingWindow;
  #tmExpire: {
    [key: string]: NodeJS.Timeout;
  } = {};

  #tmBatch?: NodeJS.Timeout;
  getStreamEvent: (records?: any[]) => any;
  constructor({ batchSize, batchWindow, onComplete, onRecordExpire, maximumRecordAgeInSeconds, tumbling }: IBatchConfig) {
    this.maximumRecordAgeInSeconds = maximumRecordAgeInSeconds;
    this.#batchSize = batchSize;
    this.#batchWindow = batchWindow;
    this.onComplete = onComplete;
    this.onRecordExpire = onRecordExpire;
    this.tumbling = tumbling;

    if (this.tumbling) {
      this.getStreamEvent = (records?: any[]) => {
        return this.tumbling!.getTimeWindowEvent(records ?? this.records);
      };
      if (!this.tumbling.onComplete) {
        this.tumbling.onComplete = () => {
          this.onComplete(this, true);
        };
      }
    } else {
      this.getStreamEvent = (records?: any[]) => {
        return {
          Records: records ?? this.records,
        };
      };
    }
  }

  #setClosed() {
    if (this.records.length == this.#batchSize) {
      this.closed = true;
      clearTimeout(this.#tmBatch);
      this.onComplete(this, false);
    }
  }
  setRecord(record: any, DDBStreamBatchInfo: any) {
    if (this.closed) {
      return false;
    }

    this.records.push(record);
    this.#setClosed();
    this.#setExpire(record.eventID, DDBStreamBatchInfo);

    if (this.records.length == 1) {
      this.#tmBatch = setTimeout(() => {
        if (!this.closed) {
          this.closed = true;
          this.onComplete(this, false);
        }
      }, this.#batchWindow);
    }
    return true;
  }
  removeRecord(id: string) {
    const foundIndex = this.records.findIndex((x) => x.eventID == id);

    if (foundIndex != -1) {
      clearTimeout(this.#tmExpire[id]);
      this.records.splice(foundIndex, 1);
    }
  }

  hasRecord(id: string) {
    return this.records.find((x) => x.eventID == id);
  }
  #setExpire(id: string, DDBStreamBatchInfo: any) {
    this.#tmExpire[id] = setTimeout(() => {
      const foundIndex = this.records.findIndex((x) => x.eventID == id);

      if (foundIndex != -1) {
        this.records.splice(foundIndex, 1);

        if (this.onRecordExpire) {
          this.onRecordExpire(DDBStreamBatchInfo);
        }
      }
    }, this.maximumRecordAgeInSeconds);
  }
}
