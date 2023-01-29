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
    const funcs = this.serverless.service.functions;
    let functionsNames = Object.keys(funcs);

    for (const funcName of functionsNames) {
      const lambda: any = this.serverless.service.getFunction(funcName);
      const foundLambda = this.#lambdas.find((x) => x.name == funcName);

      if (!lambda || !foundLambda) {
        continue;
      }

      if (lambda.events.length) {
        const streamEvents = lambda.events.map(this.#parseDdbStreamDefinitions).filter(Boolean);

        if (streamEvents.length) {
          for (const event of streamEvents) {
            const subscriber = new Subscriber({ ...event, invoke: foundLambda.invoke });
            this.subscribers.push(subscriber);
          }
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
    return ddbStreamTables;
  };

  #parseDynamoTableNameFromArn = (arn: any) => {
    if (typeof arn === "string") {
      const ddb = arn.split(":")?.[2];
      const TableName = arn.split("/")?.[1];

      if (ddb === "dynamodb" && TableName) {
        return TableName;
      }
    }
  };

  #getTableNameFromResources = (obj: any) => {
    const [key, value] = Object.entries(obj)?.[0];

    if (!key || !value) {
      return;
    }

    if (key == "Fn::GetAtt" || key == "Ref") {
      const [resourceName] = value as unknown as any[];

      const resource = this.ddbStreamTables[resourceName];
      if (resource) {
        return resource.TableName;
      }
    } else if (key == "Fn::ImportValue" && typeof value == "string") {
      // @ts-ignore
      return this.#parseDynamoTableNameFromArn(this.serverless.service.resources?.Outputs?.[value]?.Export?.Name);
    }
  };
  #getStreamTableInfoFromTableName = (tableName: string) => {
    const foundInfo = Object.values(this.ddbStreamTables).find((x) => x.TableName == tableName);

    if (foundInfo) {
      return foundInfo;
    }
  };
  #parseDdbStreamDefinitions = (event: any) => {
    if (!event || Object.keys(event)[0] !== "stream") {
      return;
    }

    let parsedEvent: any = {};

    const val = Object.values(event)[0] as any;
    const valType = typeof val;

    if (valType == "string") {
      const parsedTableName = this.#parseDynamoTableNameFromArn(val);
      if (parsedTableName) {
        parsedEvent.TableName = parsedTableName;
      }
    } else if (val && !Array.isArray(val) && valType == "object" && (!("enabled" in val) || val.enabled)) {
      const parsedTableName = this.#parseDynamoTableNameFromArn(val.arn);

      if (parsedTableName) {
        parsedEvent.TableName = parsedTableName;
      } else if (val.arn && typeof val.arn == "object") {
        const parsedTableName = this.#getTableNameFromResources(val.arn);

        if (parsedTableName) {
          parsedEvent.TableName = parsedTableName;
        }
      }

      if (parsedEvent.TableName) {
        parsedEvent.batchSize = val.batchSize ?? 100;

        if (val.functionResponseType) {
          parsedEvent.functionResponseType = val.functionResponseType;
        }
        if (val.filterPatterns) {
          parsedEvent.filterPatterns = val.filterPatterns;
        }

        if (val.destinations?.onFailure) {
          parsedEvent.onFailure = val.destinations.onFailure;
        }
      }
    }

    if (parsedEvent.TableName) {
      const streamInfo = this.#getStreamTableInfoFromTableName(parsedEvent.TableName);

      parsedEvent = { ...parsedEvent, ...streamInfo };

      if (!("StreamEnabled" in parsedEvent)) {
        parsedEvent.StreamEnabled = true;
      }
      return parsedEvent;
    }
  };

  setRecords = ({ records, TableName }: { records: any[]; TableName: string }) => {
    this.subscribers.forEach((x) => {
      if (x.TableName == TableName) {
        x.setRecords(records);
      }
    });
  };
}
