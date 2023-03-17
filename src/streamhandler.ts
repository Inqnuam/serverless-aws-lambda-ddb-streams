import { Subscriber } from "./subscriber";

export class StreamsHandler {
  serverless;
  ddbStreamTables: any[] = [];
  subscribers: Subscriber[] = [];
  #lambdas: any[] = [];
  listenableTables: any[] = [];
  constructor(serverless: any, lambdas: any[]) {
    this.serverless = serverless;
    this.#lambdas = lambdas;
    this.getDynamoStreamTables();
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
  getDynamoStreamTables = () => {
    let ddbStreamTables = [];
    if (this.serverless.service.resources?.Resources) {
      ddbStreamTables = Object.entries(this.serverless.service.resources.Resources)?.reduce((accum, obj: [string, any]) => {
        const [key, value] = obj;

        if (value.Type == "AWS::DynamoDB::Table") {
          const { TableName, StreamSpecification } = value.Properties;
          if (TableName) {
            accum[key] = {
              TableName,
            };

            if (StreamSpecification) {
              let StreamEnabled = false;
              if (!("StreamEnabled" in StreamSpecification) || StreamSpecification.StreamEnabled) {
                StreamEnabled = true;
              }
              accum[key]["StreamEnabled"] = StreamEnabled;

              if (StreamSpecification.StreamViewType) {
                accum[key]["StreamViewType"] = StreamSpecification.StreamViewType;
              }
            }
          }
        }

        return accum;
      }, {} as any);
    }
    this.ddbStreamTables = ddbStreamTables;
  };

  setRecords = ({ records, TableName, DDBStreamBatchInfo }: { records: any[]; TableName: string; DDBStreamBatchInfo: any }) => {
    this.subscribers.forEach((x) => {
      if (x.TableName == TableName) {
        x.setRecords(records, DDBStreamBatchInfo);
      }
    });
  };
}
