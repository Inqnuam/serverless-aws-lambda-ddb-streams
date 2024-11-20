import {
  DynamoDBStreamsClient,
  DescribeStreamCommand,
  GetShardIteratorCommand,
  GetRecordsCommand,
  ExpiredIteratorException,
  TrimmedDataAccessException,
  ListStreamsCommand,
  type StreamViewType,
  type Shard,
  type GetShardIteratorCommandInput,
} from "@aws-sdk/client-dynamodb-streams";
import { DynamoDBClient, UpdateTableCommand, waitUntilTableExists, type DynamoDBClientConfig } from "@aws-sdk/client-dynamodb";
import EventEmitter from "events";

export class DynamoStream extends EventEmitter {
  cli: DynamoDBClient;
  streamCli: DynamoDBStreamsClient;
  TableName: string;
  StreamViewType: StreamViewType = "NEW_AND_OLD_IMAGES";
  #isFirstConnection: boolean = true;
  static maxWaitTime: number = 25;
  static watchInterval: number = 1;
  static clientConfig: DynamoDBClientConfig = { endpoint: "http://127.0.0.1:8000", region: "ddblocal", credentials: { accessKeyId: "test", secretAccessKey: "test" } };
  constructor(config = { TableName: "", StreamViewType: undefined }) {
    super();

    this.cli = new DynamoDBClient(DynamoStream.clientConfig);
    this.streamCli = new DynamoDBStreamsClient(DynamoStream.clientConfig);
    this.TableName = config.TableName;

    if (config.StreamViewType) {
      this.StreamViewType = config.StreamViewType;
    }
  }

  private shards: Set<string> = new Set();
  async init() {
    await this.#enableStream();
    await this.start();

    setInterval(async () => {
      try {
        const StreamDescription = await this.getAvailableStream();
        if (!StreamDescription?.Shards || !StreamDescription.StreamArn) {
          return;
        }

        for (const Shard of StreamDescription?.Shards) {
          if (!this.shards.has(Shard.ShardId!)) {
            try {
              await this.watch(Shard, StreamDescription.StreamArn);
              this.shards.add(Shard.ShardId!);
            } catch {}
          }
        }
      } catch (error) {}
    }, 1000);
  }

  private async start() {
    const StreamDescription = await this.getAvailableStream();

    if (!StreamDescription) {
      return setTimeout(async () => {
        try {
          await this.start();
        } catch {}
      }, 1000);
    }

    const { Shards, StreamArn } = StreamDescription;
    if (this.#isFirstConnection) {
      console.log(`✅ Successfully connected to Table "${this.TableName}"`);
      this.#isFirstConnection = false;
    }

    if (StreamArn && Shards?.length) {
      const Shard = Shards[Shards.length - 1];

      await this.watch(Shard, StreamArn);
      this.shards.add(Shard.ShardId!);
    }
  }

  async getAvailableStream() {
    const cmd = new ListStreamsCommand({ TableName: this.TableName });
    const { Streams } = await this.streamCli.send(cmd);
    if (!Streams) {
      return;
    }

    const streams = await Promise.all(
      Streams.filter((x) => x.StreamArn).map((x) => {
        return this.describeStream(x.StreamArn!);
      })
    );

    if (!streams) {
      return;
    }

    const enabledStreams = streams.filter((x) => x?.StreamStatus == "ENABLED");

    if (!enabledStreams?.length) {
      return;
    }

    return enabledStreams[0];
  }

  async describeStream(StreamArn: string) {
    const cmd = new DescribeStreamCommand({
      StreamArn,
    });

    const { StreamDescription } = await this.streamCli.send(cmd);
    return StreamDescription;
  }

  async getShardIterator(Shard: Shard, StreamArn: string, SequenceNumber?: string) {
    let params: GetShardIteratorCommandInput = {
      StreamArn,
      ShardId: Shard.ShardId,
      ShardIteratorType: "TRIM_HORIZON",
    };

    const shardInfo = new GetShardIteratorCommand(params);

    const res = await this.streamCli.send(shardInfo);
    return res.ShardIterator;
  }

  async #enableStream() {
    try {
      await waitUntilTableExists({ client: this.cli, maxWaitTime: DynamoStream.maxWaitTime }, { TableName: this.TableName });
      const enableStream = new UpdateTableCommand({
        TableName: this.TableName,
        StreamSpecification: {
          StreamEnabled: true,
          StreamViewType: this.StreamViewType,
        },
      });
      try {
        await this.cli.send(enableStream);
      } catch (error) {}
    } catch (error) {
      console.log(`Could not connect to table ${this.TableName}`);
    }
  }

  async getRecords(ShardIterator: string | undefined) {
    const recordCmd = new GetRecordsCommand({
      ShardIterator,
    });

    return this.streamCli.send(recordCmd);
  }
  async watch(Shard: Shard, StreamArn: string) {
    const startSequenceNumber = Shard.SequenceNumberRange?.StartingSequenceNumber;
    const endSequenceNumber = Shard.SequenceNumberRange?.EndingSequenceNumber;

    let iterator = await this.getShardIterator(Shard, StreamArn);

    const watcher = setInterval(async () => {
      try {
        const { NextShardIterator, Records } = await this.getRecords(iterator);

        if (Records?.length) {
          const dummyDate = new Date().toISOString();
          const DDBStreamBatchInfo = {
            shardId: Shard.ShardId,
            startSequenceNumber,
            endSequenceNumber,
            approximateArrivalOfFirstRecord: dummyDate,
            approximateArrivalOfLastRecord: dummyDate,
            batchSize: Records.length,
            streamArn: StreamArn,
          };
          this.emit("records", Records, DDBStreamBatchInfo);
        }

        if (NextShardIterator) {
          iterator = NextShardIterator;
        } else {
          iterator = undefined;
          clearInterval(watcher);
          await this.start();
        }
      } catch (error: any) {
        if (error instanceof ExpiredIteratorException) {
          clearInterval(watcher);
          await this.start();
        } else if (error instanceof TrimmedDataAccessException) {
          iterator = await this.getShardIterator(Shard, StreamArn, Shard.SequenceNumberRange?.StartingSequenceNumber);
        } else {
          console.log(error);
        }
      }
    }, DynamoStream.watchInterval * 1000);
  }
}
