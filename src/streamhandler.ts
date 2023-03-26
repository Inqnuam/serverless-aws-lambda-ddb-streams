import { Subscriber } from "./subscriber";

export class StreamsHandler {
  serverless;
  subscribers: Subscriber[] = [];
  #lambdas: any[] = [];
  listenableTables: any[] = [];
  constructor(serverless: any, lambdas: any[]) {
    this.serverless = serverless;
    this.#lambdas = lambdas;
    this.getSlsDeclarations();
    this.getListenableTables();
  }

  getSlsDeclarations = () => {
    for (const lambda of this.#lambdas) {
      const streamEvents = lambda.ddb;

      if (streamEvents.length) {
        for (const event of streamEvents) {
          const subscriber = new Subscriber(event, lambda.invoke, lambda.outName);
          this.subscribers.push(subscriber);
        }
      }
    }
  };

  getListenableTables = () => {
    for (const stream of this.subscribers) {
      const { TableName, StreamEnabled, StreamViewType } = stream;

      const foundIndex = this.listenableTables.findIndex((x) => x.TableName == TableName);

      if (!("StreamEnabled" in stream) || StreamEnabled) {
        let table: any = {
          TableName,
        };
        if (StreamViewType) {
          table.StreamViewType = StreamViewType;
        }

        if (foundIndex == -1) {
          this.listenableTables.push(table);
        } else {
          this.listenableTables[foundIndex] = { ...this.listenableTables[foundIndex], ...table };
        }
      }
    }
  };
  setRecords = ({ records, TableName, DDBStreamBatchInfo }: { records: any[]; TableName: string; DDBStreamBatchInfo: any }) => {
    this.subscribers.forEach((x) => {
      if (x.TableName == TableName) {
        x.setRecords(records, DDBStreamBatchInfo);
      }
    });
  };
}
